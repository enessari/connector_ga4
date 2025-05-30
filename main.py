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
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account

# === CONFIGURATION & LOGGING ===

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceConfig:
    max_workers: int = 5  # Concurrent API calls
    batch_size: int = 1000  # Rows per batch
    rate_limit_delay: float = 0.1  # Seconds between API calls
    memory_threshold: int = 50_000  # Max rows in memory before flushing
    enable_caching: bool = True
    chunk_size: int = 10_000  # Pandas chunk size

# === RATE LIMITING & CACHING ===

class RateLimiter:
    def __init__(self, delay: float = 0.1):
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

# === OPTIMIZED ENRICHMENT ===

@rate_limited
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
    except Exception as e:
        logger.error(f"Could not fetch account names: {e}")
    return account_map

def discover_all_properties_optimized(service_account_dict):
    """Optimized property discovery with caching"""
    cache_key = "all_properties"
    cached = cache.get(cache_key)
    if cached:
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
            prop_list = admin_client.list_properties(parent=acc.name)
            for prop in prop_list:
                properties.append({
                    "account_id": acc_id,
                    "account_name": acc_name,
                    "property_id": prop.name.split("/")[-1],
                    "property_name": prop.display_name
                })
        cache.set(cache_key, properties)
    except Exception as e:
        logger.error(f"Property discovery failed: {e}")
    return properties

def enrich_properties_concurrent(property_list, credentials, max_workers: int = 5):
    """Concurrent property enrichment"""
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
            return {**prop, "account_id": "unknown", "account_name": "unknown"}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(enrich_single_property, prop) for prop in property_list]
        enriched = []
        for future in as_completed(futures):
            enriched.append(future.result())
    
    return enriched

# === OPTIMIZED QUERY ENGINE ===

class StreamingCSVWriter:
    """Memory-efficient CSV writer that flushes data in chunks"""
    
    def __init__(self, file_path: str, chunk_size: int = 10000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.buffer = []
        self.headers_written = False
        self.total_rows = 0
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    def add_rows(self, rows: List[Dict]):
        self.buffer.extend(rows)
        self.total_rows += len(rows)
        
        if len(self.buffer) >= self.chunk_size:
            self._flush()
    
    def _flush(self):
        if not self.buffer:
            return
            
        df = pd.DataFrame(self.buffer)
        
        # Write headers only once
        mode = 'w' if not self.headers_written else 'a'
        header = not self.headers_written
        
        df.to_csv(self.file_path, mode=mode, header=header, index=False)
        self.headers_written = True
        self.buffer.clear()
        
        logger.info(f"Flushed {len(df)} rows to {self.file_path}")
    
    def close(self):
        self._flush()
        return self.total_rows

@rate_limited
def run_query_for_property_optimized(data_client, query_name, dimensions, metrics, 
                                   date_range, dimension_filter, prop):
    """Optimized query execution with error handling"""
    property_id = prop.get("property_id")
    account_id = prop.get("account_id", "unknown")
    property_name = prop.get("property_name", "")
    account_name = prop.get("account_name", "")
    
    try:
        # Build request with pagination support
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[date_range],
            dimension_filter=dimension_filter,
            limit=10000,  # Max per request
            offset=0
        )
        
        all_results = []
        while True:
            report = data_client.run_report(request)
            
            batch_results = []
            for row in report.rows:
                record = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "property_id": property_id,
                    "property_name": property_name
                }
                record.update({dimensions[i].name: row.dimension_values[i].value 
                             for i in range(len(dimensions))})
                record.update({metrics[i].name: row.metric_values[i].value 
                             for i in range(len(metrics))})
                batch_results.append(record)
            
            all_results.extend(batch_results)
            
            # Check if there are more pages
            if len(report.rows) < request.limit:
                break
            
            request.offset += request.limit
        
        logger.info(f"Query '{query_name}' returned {len(all_results)} rows for property {property_id}")
        return all_results
        
    except Exception as e:
        logger.error(f"Query '{query_name}' failed for property {property_id}: {e}")
        # Log error to file
        error_path = "/data/out/tables/query_errors.csv"
        os.makedirs(os.path.dirname(error_path), exist_ok=True)
        with open(error_path, "a") as err_file:
            err_file.write(f"{datetime.now()},\"{query_name}\",{property_id},\"{e}\"\n")
        return []

def execute_queries_concurrent(params, creds_path, property_list, start_date, end_date):
    """Execute queries concurrently across properties"""
    credentials = build_credentials(creds_path)
    destination_prefix = params.get("destination", "ga4.output")
    query_definitions = params.get("query_definitions", [])
    output_format = params.get("output_format", "default")
    
    # Performance configuration
    perf_config = PerformanceConfig(
        max_workers=params.get("max_workers", 5),
        batch_size=params.get("batch_size", 1000),
        rate_limit_delay=params.get("rate_limit_delay", 0.1),
        memory_threshold=params.get("memory_threshold", 50_000)
    )
    
    logger.info(f"Starting concurrent execution with {perf_config.max_workers} workers")
    
    for query in query_definitions:
        query_name = query.get("name", "query")
        logger.info(f"Processing query: {query_name}")
        
        dimensions = [Dimension(name=d) for d in query.get("dimensions", [])]
        metrics = [Metric(name=m) for m in query.get("metrics", [])]
        dimension_filter = build_dimension_filter(query.get("dimension_filter"))
        date_range = DateRange(start_date=start_date, end_date=end_date)
        
        # Setup streaming writer
        timestamp = datetime.now().strftime("%Y%m%d")
        if output_format == "airbyte_json":
            out_path = f"/data/out/tables/{destination_prefix}-{query_name}-{timestamp}.csv"
        else:
            out_path = f"/data/out/tables/{destination_prefix}.{query_name}.{timestamp}.csv"
        
        writer = StreamingCSVWriter(out_path, perf_config.batch_size)
        
        # Execute queries concurrently
        def execute_for_property(prop):
            data_client = create_data_client(credentials)  # Create client per thread
            return run_query_for_property_optimized(
                data_client, query_name, dimensions, metrics, 
                date_range, dimension_filter, prop
            )
        
        with ThreadPoolExecutor(max_workers=perf_config.max_workers) as executor:
            future_to_prop = {
                executor.submit(execute_for_property, prop): prop 
                for prop in property_list
            }
            
            for future in as_completed(future_to_prop):
                prop = future_to_prop[future]
                try:
                    results = future.result()
                    if results:
                        writer.add_rows(results)
                        logger.info(f"Added {len(results)} rows for property {prop.get('property_id')}")
                except Exception as e:
                    logger.error(f"Error processing property {prop.get('property_id')}: {e}")
        
        # Finalize output
        total_rows = writer.close()
        
        if total_rows > 0:
            logger.info(f"Query '{query_name}' completed: {total_rows} total rows written to {out_path}")
            
            # Create optimized manifest
            create_manifest(out_path, query_name, destination_prefix, total_rows, property_list)
        else:
            logger.warning(f"No data for query '{query_name}'")

def build_dimension_filter(filter_config):
    """Build dimension filter from config"""
    if not filter_config:
        return None
    and_conditions = filter_config.get("and_group", [])
    filters = [
        FilterExpression(
            filter=Filter(
                field_name=cond["field_name"],
                string_filter=Filter.StringFilter(value=cond["string_filter"]["value"])
            )
        )
        for cond in and_conditions
    ]
    return FilterExpression(and_group=FilterExpression.ListExpression(expressions=filters))

def create_manifest(file_path: str, query_name: str, destination_prefix: str, 
                   row_count: int, property_list: List[Dict]):
    """Create optimized manifest file"""
    property_ids = list({prop.get("property_id", "unknown") for prop in property_list})
    account_ids = list({prop.get("account_id", "unknown") for prop in property_list})
    property_names = list({prop.get("property_name", "") for prop in property_list})
    account_names = list({prop.get("account_name", "") for prop in property_list})
    
    manifest = {
        "output_table": f"{destination_prefix}.{query_name}",
        "filename": os.path.basename(file_path),
        "format": "csv",
        "row_count": row_count,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "query_name": query_name,
        "property_ids": property_ids,
        "account_ids": account_ids,
        "property_names": property_names,
        "account_names": account_names,
        "performance_stats": {
            "total_properties": len(property_list),
            "processing_time": None  # Could be added with timing
        }
    }
    
    manifest_path = file_path.replace(".csv", ".manifest.json")
    with open(manifest_path, "w") as mf:
        json.dump(manifest, mf, indent=2, ensure_ascii=False)
    logger.info(f"Manifest written to {manifest_path}")

def inject_date_dimension(query_definitions):
    """Inject date dimension if not present"""
    for query in query_definitions:
        dimensions = query.get("dimensions", [])
        if "date" not in dimensions:
            dimensions.insert(0, "date")
            query["dimensions"] = dimensions
    return query_definitions

# === MAIN EXECUTION ===

def main():
    start_time = time.time()
    
    try:
        with open('/data/config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error("Cannot read or parse /data/config.json")
        raise e

    params = config.get("parameters", {})
    if "parameters" in params:
        logger.info("Detected nested 'parameters' block in config — flattening...")
        params = params["parameters"]

    service_account_json = params.get("service_account_json")
    if not service_account_json or not isinstance(service_account_json, dict):
        raise ValueError("Missing service account credentials")

    property_list = params.get("property_list", [])
    
    # Date handling
    if not params.get("start_date") or not params.get("end_date"):
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=7)
        logger.info(f"Using default date range: {start_date} to {end_date}")
    else:
        start_date = params.get("start_date")
        end_date = params.get("end_date")

    creds_path = write_temp_credentials(service_account_json)
    credentials = build_credentials(creds_path)

    # Property discovery/enrichment
    if not property_list:
        logger.info("Discovering all accessible GA4 properties...")
        property_list = discover_all_properties_optimized(service_account_json)
    else:
        logger.info("Enriching property list with concurrent API calls...")
        max_workers = params.get("max_workers", 5)
        property_list = enrich_properties_concurrent(property_list, credentials, max_workers)

    logger.info(f"Processing {len(property_list)} properties")

    # Query preparation
    query_definitions = params.get("query_definitions", [])
    params["query_definitions"] = inject_date_dimension(query_definitions)

    # Execute optimized queries
    execute_queries_concurrent(params, creds_path, property_list, str(start_date), str(end_date))
    
    total_time = time.time() - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
