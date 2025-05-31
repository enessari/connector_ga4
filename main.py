import json
import os
import pandas as pd
import requests
import asyncio
import aiofiles
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import time
from functools import wraps
import logging
from dataclasses import dataclass
import csv
import io
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account

# === CONFIGURATION & LOGGING ===

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceConfig:
    max_workers: int = 3  # Reduced for stability
    batch_size: int = 5000  # Larger batches for efficiency  
    rate_limit_delay: float = 0.2  # Increased delay
    memory_threshold: int = 100_000  # Higher threshold
    enable_caching: bool = True
    chunk_size: int = 25_000  # Larger chunks
    max_retries: int = 3
    backoff_factor: float = 2.0

# === ENHANCED ERROR HANDLING ===

class GA4ConnectorError(Exception):
    """Base exception for GA4 Connector"""
    pass

class DataProcessingError(GA4ConnectorError):
    """Error during data processing"""
    pass

class APIError(GA4ConnectorError):
    """Error during API calls"""
    pass

def sanitize_csv_data(data: List[Dict]) -> List[Dict]:
    """Sanitize data for CSV writing to prevent escape character issues"""
    sanitized_data = []
    
    for row in data:
        sanitized_row = {}
        for key, value in row.items():
            if isinstance(value, str):
                # Replace problematic characters
                sanitized_value = (value
                    .replace('"', '""')  # Escape quotes
                    .replace('\n', ' ')   # Replace newlines with spaces
                    .replace('\r', ' ')   # Replace carriage returns
                    .replace('\t', ' ')   # Replace tabs with spaces
                    .strip()              # Remove leading/trailing whitespace
                )
                # Limit field length to prevent extremely long strings
                if len(sanitized_value) > 1000:
                    sanitized_value = sanitized_value[:997] + "..."
                sanitized_row[key] = sanitized_value
            else:
                sanitized_row[key] = value
        sanitized_data.append(sanitized_row)
    
    return sanitized_data

# === RATE LIMITING & CACHING ===

class RateLimiter:
    def __init__(self, delay: float = 0.2):
        self.delay = delay
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()

class SimpleCache:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.cache = {}
    
    def get(self, key: str) -> Any:
        return self.cache.get(key) if self.enabled else None
    
    def set(self, key: str, value: Any):
        if self.enabled:
            self.cache[key] = value

# Global instances
rate_limiter = RateLimiter()
cache = SimpleCache()

def rate_limited(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        rate_limiter.wait()
        return func(*args, **kwargs)
    return wrapper

def retry_on_failure(max_retries: int = 3, backoff_factor: float = 2.0):
    """Decorator for retrying failed operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator

# === OPTIMIZED AUTH & CLIENT ===

def write_temp_credentials(service_account_dict):
    creds_path = "/tmp/service_account.json"
    with open(creds_path, "w") as f:
        json.dump(service_account_dict, f)
    return creds_path

def build_credentials(creds_path):
    return service_account.Credentials.from_service_account_file(creds_path)

def create_data_client(credentials):
    return BetaAnalyticsDataClient(credentials=credentials)

def create_admin_client(credentials):
    return AnalyticsAdminServiceClient(credentials=credentials)

# === ENHANCED CSV WRITER ===

class SafeStreamingCSVWriter:
    """Memory-efficient CSV writer with enhanced error handling and data sanitization"""
    
    def __init__(self, file_path: str, chunk_size: int = 25000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.buffer = []
        self.headers_written = False
        self.total_rows = 0
        self.error_count = 0
        self.success_count = 0
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Initialize error log
        self.error_log_path = file_path.replace('.csv', '_errors.log')
    
    def add_rows(self, rows: List[Dict]):
        """Add rows with data validation and sanitization"""
        if not rows:
            return
            
        try:
            # Sanitize data before adding to buffer
            sanitized_rows = sanitize_csv_data(rows)
            self.buffer.extend(sanitized_rows)
            self.total_rows += len(sanitized_rows)
            self.success_count += len(sanitized_rows)
            
            logger.info(f"Added {len(sanitized_rows)} sanitized rows to buffer. Total: {self.total_rows}")
            
            if len(self.buffer) >= self.chunk_size:
                self._flush()
                
        except Exception as e:
            self.error_count += len(rows)
            error_msg = f"Error adding rows: {e}"
            logger.error(error_msg)
            self._log_error(error_msg, rows[:5])  # Log first 5 rows for debugging
    
    def _flush(self):
        """Flush buffer to CSV with enhanced error handling"""
        if not self.buffer:
            return
            
        try:
            df = pd.DataFrame(self.buffer)
            
            # Clean DataFrame
            df = self._clean_dataframe(df)
            
            # Write with safe CSV parameters
            mode = 'w' if not self.headers_written else 'a'
            header = not self.headers_written
            
            # Use safe CSV writing parameters
            df.to_csv(
                self.file_path, 
                mode=mode, 
                header=header, 
                index=False,
                encoding='utf-8',
                escapechar='\\',  # Set escape character
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator='\n'
            )
            
            self.headers_written = True
            flushed_count = len(self.buffer)
            self.buffer.clear()
            
            logger.info(f"Successfully flushed {flushed_count} rows to {self.file_path}")
            
        except Exception as e:
            error_msg = f"Error flushing data to CSV: {e}"
            logger.error(error_msg)
            self._log_error(error_msg, self.buffer[:10])
            
            # Try alternative writing method
            self._fallback_write()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for safe CSV writing"""
        try:
            # Convert all columns to string and clean
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).fillna('')
                    # Additional cleaning for problematic characters
                    df[col] = df[col].str.replace(r'[\x00-\x1f\x7f-\x9f]', ' ', regex=True)
            
            # Fill NaN values
            df = df.fillna('')
            
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {e}")
            return df
    
    def _fallback_write(self):
        """Fallback CSV writing method using Python's csv module"""
        try:
            if not self.buffer:
                return
                
            # Get headers from first row
            if self.buffer:
                headers = list(self.buffer[0].keys())
                
                with open(self.file_path, 'a' if self.headers_written else 'w', 
                         newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(
                        csvfile, 
                        fieldnames=headers,
                        escapechar='\\',
                        quoting=csv.QUOTE_MINIMAL
                    )
                    
                    if not self.headers_written:
                        writer.writeheader()
                        self.headers_written = True
                    
                    for row in self.buffer:
                        try:
                            writer.writerow(row)
                        except Exception as row_error:
                            logger.warning(f"Skipping problematic row: {row_error}")
                            continue
                
                logger.info(f"Fallback write completed for {len(self.buffer)} rows")
                self.buffer.clear()
                
        except Exception as e:
            logger.error(f"Fallback write also failed: {e}")
            # Save data as JSON as last resort
            self._emergency_save()
    
    def _emergency_save(self):
        """Emergency save as JSON when CSV writing fails completely"""
        try:
            json_path = self.file_path.replace('.csv', '_emergency.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(self.buffer, f, ensure_ascii=False, indent=2)
            logger.info(f"Emergency save completed: {json_path}")
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Emergency save failed: {e}")
    
    def _log_error(self, error_msg: str, sample_data: List[Dict]):
        """Log errors with sample data for debugging"""
        try:
            with open(self.error_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat()}: {error_msg}\n")
                f.write(f"Sample data: {json.dumps(sample_data, ensure_ascii=False)}\n\n")
        except Exception:
            pass  # Don't fail if we can't log errors
    
    def finalize(self) -> Dict[str, int]:
        """Finalize writing and return statistics"""
        try:
            self._flush()
            
            stats = {
                'total_rows': self.total_rows,
                'success_count': self.success_count,
                'error_count': self.error_count,
                'success_rate': (self.success_count / max(1, self.total_rows)) * 100
            }
            
            logger.info(f"CSV writing completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error finalizing CSV writer: {e}")
            return {
                'total_rows': self.total_rows,
                'success_count': self.success_count,
                'error_count': self.error_count + len(self.buffer),
                'success_rate': 0
            }

# === OPTIMIZED ENRICHMENT ===

@rate_limited
@retry_on_failure(max_retries=3)
def get_account_display_names(credentials):
    cache_key = "account_names"
    cached = cache.get(cache_key)
    if cached:
        return cached
    
    client = create_admin_client(credentials)
    account_map = {}
    try:
        accounts = client.list_accounts()
        for acc in accounts:
            acc_id = acc.name.split("/")[-1]
            account_map[acc_id] = acc.display_name
        cache.set(cache_key, account_map)
        logger.info(f"Fetched {len(account_map)} account names")
    except Exception as e:
        logger.error(f"Could not fetch account names: {e}")
        raise APIError(f"Failed to fetch account names: {e}")
    return account_map

@retry_on_failure(max_retries=3)
def discover_all_properties_optimized(service_account_dict):
    """Optimized property discovery with caching and error handling"""
    cache_key = "all_properties"
    cached = cache.get(cache_key)
    if cached:
        logger.info(f"Using cached properties: {len(cached)} properties")
        return cached
    
    creds_path = write_temp_credentials(service_account_dict)
    credentials = build_credentials(creds_path)
    admin_client = create_admin_client(credentials)
    account_map = get_account_display_names(credentials)
    properties = []
    
    try:
        accounts = admin_client.list_accounts()
        for acc in accounts:
            acc_id = acc.name.split("/")[-1]
            acc_name = account_map.get(acc_id, f"account_{acc_id}")
            try:
                prop_list = admin_client.list_properties(parent=acc.name)
                for prop in prop_list:
                    properties.append({
                        "account_id": acc_id,
                        "account_name": acc_name,
                        "property_id": prop.name.split("/")[-1],
                        "property_name": prop.display_name
                    })
            except Exception as e:
                logger.warning(f"Could not fetch properties for account {acc_id}: {e}")
                continue
                
        cache.set(cache_key, properties)
        logger.info(f"Discovered {len(properties)} properties")
    except Exception as e:
        logger.error(f"Property discovery failed: {e}")
        raise APIError(f"Property discovery failed: {e}")
    return properties

def enrich_properties_concurrent(property_list, credentials, max_workers: int = 3):
    """Concurrent property enrichment with error handling"""
    account_map = get_account_display_names(credentials)
    
    def enrich_single_property(prop):
        try:
            client = create_admin_client(credentials)
            property_id = prop["property_id"]
            cache_key = f"property_{property_id}"
            cached = cache.get(cache_key)
            if cached:
                return cached
                
            property_path = f"properties/{property_id}"
            prop_metadata = client.get_property(name=property_path)
            account_id = prop_metadata.parent.split("/")[-1]
            account_name = account_map.get(account_id, f"account_{account_id}")
            
            result = {
                "property_id": property_id,
                "property_name": prop_metadata.display_name,
                "account_id": account_id,
                "account_name": account_name,
                **prop
            }
            cache.set(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Failed to enrich property {prop}: {e}")
            return {**prop, "account_id": "unknown", "account_name": "unknown", "enrichment_error": str(e)}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(enrich_single_property, prop) for prop in property_list]
        enriched = []
        for future in as_completed(futures):
            try:
                result = future.result()
                enriched.append(result)
            except Exception as e:
                logger.error(f"Property enrichment future failed: {e}")
    
    logger.info(f"Enriched {len(enriched)}/{len(property_list)} properties")
    return enriched

# === ENHANCED QUERY ENGINE ===

@rate_limited
@retry_on_failure(max_retries=3)
def run_query_for_property_enhanced(data_client, query_name, dimensions, metrics, 
                                  date_range, dimension_filter, prop):
    """Enhanced query execution with comprehensive error handling"""
    property_id = prop.get("property_id")
    account_id = prop.get("account_id", "unknown")
    property_name = prop.get("property_name", "")
    account_name = prop.get("account_name", "")
    
    logger.info(f"Starting query '{query_name}' for property {property_id}")
    
    try:
        # Build request with pagination support
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[date_range],
            dimension_filter=dimension_filter,
            limit=100000,  # Max per request
            offset=0
        )
        
        all_results = []
        page_count = 0
        
        while True:
            try:
                report = data_client.run_report(request)
                page_count += 1
                
                batch_results = []
                for row in report.rows:
                    try:
                        record = {
                            "account_id": str(account_id),
                            "account_name": str(account_name),
                            "property_id": str(property_id),
                            "property_name": str(property_name)
                        }
                        
                        # Add dimension values
                        for i, dim in enumerate(dimensions):
                            try:
                                value = row.dimension_values[i].value if i < len(row.dimension_values) else ""
                                record[dim.name] = str(value) if value is not None else ""
                            except Exception as e:
                                logger.warning(f"Error processing dimension {dim.name}: {e}")
                                record[dim.name] = ""
                        
                        # Add metric values
                        for i, metric in enumerate(metrics):
                            try:
                                value = row.metric_values[i].value if i < len(row.metric_values) else "0"
                                record[metric.name] = str(value) if value is not None else "0"
                            except Exception as e:
                                logger.warning(f"Error processing metric {metric.name}: {e}")
                                record[metric.name] = "0"
                        
                        batch_results.append(record)
                        
                    except Exception as e:
                        logger.warning(f"Error processing row in query '{query_name}': {e}")
                        continue
                
                all_results.extend(batch_results)
                logger.info(f"Property {property_id}: Processed page {page_count}, {len(batch_results)} rows")
                
                # Check if there are more pages
                if len(report.rows) < request.limit:
                    break
                
                request.offset += request.limit
                
                # Safety check to prevent infinite loops
                if page_count > 100:  # Max 10M rows
                    logger.warning(f"Reached maximum page limit for property {property_id}")
                    break
                    
            except Exception as e:
                logger.error(f"Error in pagination for property {property_id}, page {page_count}: {e}")
                break
        
        logger.info(f"Completed query '{query_name}' for property {property_id}: {len(all_results)} rows")
        return all_results
        
    except Exception as e:
        error_msg = f"Query '{query_name}' failed for property {property_id}: {e}"
        logger.error(error_msg)
        
        # Log detailed error
        log_query_error(query_name, property_id, str(e), {
            'dimensions': [d.name for d in dimensions],
            'metrics': [m.name for m in metrics],
            'property_name': property_name,
            'account_name': account_name
        })
        
        return []

def log_query_error(query_name: str, property_id: str, error: str, context: Dict):
    """Log query errors to a structured file"""
    try:
        error_path = "/data/out/tables/query_errors.csv"
        os.makedirs(os.path.dirname(error_path), exist_ok=True)
        
        error_record = {
            'timestamp': datetime.now().isoformat(),
            'query_name': query_name,
            'property_id': property_id,
            'error': error,
            'context': json.dumps(context)
        }
        
        # Write header if file doesn't exist
        file_exists = os.path.exists(error_path)
        with open(error_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=error_record.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(error_record)
            
    except Exception as e:
        logger.error(f"Failed to log query error: {e}")

def execute_queries_for_large_data(params, creds_path, property_list, start_date, end_date):
    """Execute queries optimized for large datasets with enhanced error handling"""
    credentials = build_credentials(creds_path)
    destination_prefix = params.get("destination", "ga4.output")
    query_definitions = params.get("query_definitions", [])
    output_format = params.get("output_format", "default")
    
    # Performance configuration
    perf_config = PerformanceConfig(
        max_workers=params.get("max_workers", 3),
        batch_size=params.get("batch_size", 5000),
        rate_limit_delay=params.get("rate_limit_delay", 0.2),
        memory_threshold=params.get("memory_threshold", 100_000),
        chunk_size=params.get("chunk_size", 25_000)
    )
    
    logger.info(f"Starting execution with config: workers={perf_config.max_workers}, "
                f"batch_size={perf_config.batch_size}, chunk_size={perf_config.chunk_size}")
    
    total_queries = len(query_definitions)
    completed_queries = 0
    
    for query_idx, query in enumerate(query_definitions, 1):
        query_name = query.get("name", f"query_{query_idx}")
        logger.info(f"Processing query {query_idx}/{total_queries}: {query_name}")
        
        try:
            dimensions = [Dimension(name=d) for d in query.get("dimensions", [])]
            metrics = [Metric(name=m) for m in query.get("metrics", [])]
            dimension_filter = build_dimension_filter(query.get("dimension_filter"))
            date_range = DateRange(start_date=start_date, end_date=end_date)
            
            # Setup enhanced streaming writer
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if output_format == "airbyte_json":
                out_path = f"/data/out/tables/{destination_prefix}-{query_name}-{timestamp}.csv"
            else:
                out_path = f"/data/out/tables/{destination_prefix}.{query_name}.{timestamp}.csv"
            
            writer = SafeStreamingCSVWriter(out_path, perf_config.chunk_size)
            
            # Execute queries with controlled concurrency
            def execute_for_property(prop):
                try:
                    data_client = create_data_client(credentials)
                    return run_query_for_property_enhanced(
                        data_client, query_name, dimensions, metrics, 
                        date_range, dimension_filter, prop
                    )
                except Exception as e:
                    logger.error(f"Error creating client or executing query for property {prop.get('property_id')}: {e}")
                    return []
            
            # Process properties in smaller batches to manage memory
            batch_size = min(perf_config.max_workers, len(property_list))
            successful_properties = 0
            failed_properties = 0
            
            for i in range(0, len(property_list), batch_size):
                batch_properties = property_list[i:i + batch_size]
                logger.info(f"Processing property batch {i//batch_size + 1}: properties {i+1}-{min(i+batch_size, len(property_list))}")
                
                with ThreadPoolExecutor(max_workers=perf_config.max_workers) as executor:
                    future_to_prop = {
                        executor.submit(execute_for_property, prop): prop 
                        for prop in batch_properties
                    }
                    
                    for future in as_completed(future_to_prop):
                        prop = future_to_prop[future]
                        try:
                            results = future.result(timeout=300)  # 5 minute timeout
                            if results:
                                writer.add_rows(results)
                                successful_properties += 1
                                logger.info(f"Property {prop.get('property_id')}: Processed {len(results):,} rows")
                            else:
                                logger.warning(f"No data returned for property {prop.get('property_id')}")
                        except Exception as e:
                            failed_properties += 1
                            logger.error(f"Error processing property {prop.get('property_id')}: {e}")
                
                # Brief pause between batches
                if i + batch_size < len(property_list):
                    time.sleep(1)
            
            # Finalize output
            stats = writer.finalize()
            
            if stats['total_rows'] > 0:
                logger.info(f"Query '{query_name}' completed successfully:")
                logger.info(f"  - Total rows: {stats['total_rows']:,}")
                logger.info(f"  - Success rate: {stats['success_rate']:.1f}%")
                logger.info(f"  - Successful properties: {successful_properties}")
                logger.info(f"  - Failed properties: {failed_properties}")
                logger.info(f"  - Output file: {out_path}")
                
                # Create enhanced manifest
                create_enhanced_manifest(out_path, query_name, destination_prefix, stats, 
                                       property_list, successful_properties, failed_properties)
                completed_queries += 1
            else:
                logger.warning(f"No data written for query '{query_name}'")
                
        except Exception as e:
            logger.error(f"Failed to process query '{query_name}': {e}")
            continue
    
    logger.info(f"Execution completed: {completed_queries}/{total_queries} queries successful")

def build_dimension_filter(filter_config):
    """Build dimension filter from config with error handling"""
    if not filter_config:
        return None
    
    try:
        and_conditions = filter_config.get("and_group", [])
        filters = []
        
        for cond in and_conditions:
            try:
                filter_expr = FilterExpression(
                    filter=Filter(
                        field_name=cond["field_name"],
                        string_filter=Filter.StringFilter(value=cond["string_filter"]["value"])
                    )
                )
                filters.append(filter_expr)
            except Exception as e:
                logger.warning(f"Error building filter condition: {e}")
                continue
        
        if filters:
            return FilterExpression(and_group=FilterExpression.ListExpression(expressions=filters))
        
    except Exception as e:
        logger.error(f"Error building dimension filter: {e}")
    
    return None

def create_enhanced_manifest(file_path: str, query_name: str, destination_prefix: str, 
                           stats: Dict, property_list: List[Dict], 
                           successful_properties: int, failed_properties: int):
    """Create enhanced manifest with detailed statistics"""
    try:
        property_ids = list({prop.get("property_id", "unknown") for prop in property_list})
        account_ids = list({prop.get("account_id", "unknown") for prop in property_list})
        property_names = list({prop.get("property_name", "") for prop in property_list})
        account_names = list({prop.get("account_name", "") for prop in property_list})
        
        manifest = {
            "output_table": f"{destination_prefix}.{query_name}",
            "filename": os.path.basename(file_path),
            "format": "csv",
            "created_at": datetime.utcnow().isoformat() + "Z",
            "query_name": query_name,
            "data_statistics": stats,
            "property_statistics": {
                "total_properties": len(property_list),
                "successful_properties": successful_properties,
                "failed_properties": failed_properties,
                "success_rate": (successful_properties / max(1, len(property_list))) * 100
            },
            "property_ids": property_ids,
            "account_ids": account_ids,
            "property_names": property_names,
            "account_names": account_names,
            "data_quality": {
                "rows_processed": stats['total_rows'],
                "rows_successful": stats['success_count'],
                "rows_failed": stats['error_count'],
                "data_success_rate": stats['success_rate']
            }
        }
        
        manifest_path = file_path.replace(".csv", ".manifest.json")
        with open(manifest_path, "w", encoding='utf-8') as mf:
            json.dump(manifest, mf, indent=2, ensure_ascii=False)
        logger.info(f"Enhanced manifest written to {manifest_path}")
        
    except Exception as e:
        logger.error(f"Failed to create manifest: {e}")

def inject_date_dimension(query_definitions: List[Dict]) -> List[Dict]:
    """Add date dimension if not present"""
    for query in query_definitions:
        dimensions = query.get("dimensions", [])
        if "date" not in dimensions:
            dimensions.insert(0, "date")
            query["dimensions"] = dimensions
    return query_definitions

# === MAIN EXECUTION ===
def main():
    execution_start = time.time()
    
    # Load configuration
    try:
        with open('/data/config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error("Cannot read /data/config.json")
        raise e

    params = config.get("parameters", {})
    if "parameters" in params:
        params = params["parameters"]

    # Validate service account
    service_account_json = params.get("service_account_json")
    if not service_account_json or not isinstance(service_account_json, dict):
        raise ValueError("Missing or invalid service account credentials")

    # Setup dates
    if not params.get("start_date") or not params.get("end_date"):
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=7)
        logger.info(f"Using default date range: {start_date} to {end_date}")
    else:
        start_date = params.get("start_date")
        end_date = params.get("end_date")

    # Setup credentials
    creds_path = setup_credentials(service_account_json)
    _, admin_client, _ = create_clients(creds_path)

    # Handle properties
    property_list = params.get("property_list", [])
    if not property_list:
        logger.info("Discovering all properties...")
        property_list = discover_properties(service_account_json)
    else:
        logger.info("Enriching provided properties...")
        property_list = enrich_properties(property_list, admin_client)

    if not property_list:
        logger.error("No properties found or accessible")
        return

    logger.info(f"Processing {len(property_list)} properties for large dataset queries")

    # Prepare queries
    query_definitions = params.get("query_definitions", [])
    if not query_definitions:
        logger.error("No query definitions provided")
        return
    
    query_definitions = inject_date_dimension(query_definitions)

    # Execute optimized for large data
    execute_queries_for_large_data(params, creds_path, property_list, str(start_date), str(end_date))
    
    total_time = time.time() - execution_start
    logger.info(f"Total execution completed in {total_time:.1f} seconds")

if __name__ == "__main__":
    main()
