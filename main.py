import json
import os
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account

# === CONFIGURATION ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LargeDataConfig:
    """Optimized for large datasets with few properties"""
    CHUNK_SIZE = 50000  # Large chunks for big data
    MAX_ROWS_PER_REQUEST = 100000  # GA4 limit
    RATE_LIMIT_DELAY = 0.05  # Minimal delay
    MEMORY_FLUSH_THRESHOLD = 100000  # Flush to disk threshold

# === SIMPLE RATE LIMITER ===
class SimpleRateLimiter:
    def __init__(self, delay: float = 0.05):
        self.delay = delay
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()

rate_limiter = SimpleRateLimiter()

# === OPTIMIZED STREAMING WRITER ===
class OptimizedCSVWriter:
    """High-performance CSV writer for large datasets"""
    
    def __init__(self, file_path: str, chunk_size: int = 50000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.buffer = []
        self.headers_written = False
        self.total_rows = 0
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Pre-allocate buffer for performance
        self.buffer.clear()
    
    def add_batch(self, rows: List[Dict]):
        """Add a batch of rows efficiently"""
        self.buffer.extend(rows)
        self.total_rows += len(rows)
        
        # Flush if buffer is getting large
        if len(self.buffer) >= self.chunk_size:
            self._flush()
    
    def _flush(self):
        if not self.buffer:
            return
            
        # Convert to DataFrame efficiently
        df = pd.DataFrame(self.buffer)
        
        # Write with optimal settings
        mode = 'w' if not self.headers_written else 'a'
        header = not self.headers_written
        
        # Use fast CSV writer settings
        df.to_csv(
            self.file_path, 
            mode=mode, 
            header=header, 
            index=False,
            chunksize=10000  # Pandas internal chunking
        )
        
        self.headers_written = True
        rows_flushed = len(self.buffer)
        self.buffer.clear()
        
        logger.info(f"Flushed {rows_flushed:,} rows to disk (Total: {self.total_rows:,})")
    
    def finalize(self) -> int:
        """Flush remaining data and return total row count"""
        self._flush()
        logger.info(f"File completed: {self.total_rows:,} total rows")
        return self.total_rows

# === AUTH & CLIENTS ===
def setup_credentials(service_account_dict: Dict) -> str:
    """Setup credentials and return path"""
    creds_path = "/tmp/ga4_service_account.json"
    with open(creds_path, "w") as f:
        json.dump(service_account_dict, f)
    return creds_path

def create_clients(creds_path: str):
    """Create both clients once"""
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    data_client = BetaAnalyticsDataClient(credentials=credentials)
    admin_client = AnalyticsAdminServiceClient(credentials=credentials)
    return data_client, admin_client, credentials

# === PROPERTY MANAGEMENT ===
def get_account_names(admin_client) -> Dict[str, str]:
    """Get account ID to name mapping"""
    account_map = {}
    try:
        accounts = admin_client.list_accounts()
        for acc in accounts:
            acc_id = acc.name.split("/")[-1]
            account_map[acc_id] = acc.display_name
        logger.info(f"Loaded {len(account_map)} account names")
    except Exception as e:
        logger.warning(f"Could not fetch account names: {e}")
    return account_map

def discover_properties(service_account_dict: Dict) -> List[Dict]:
    """Discover all properties - sequential for reliability"""
    creds_path = setup_credentials(service_account_dict)
    _, admin_client, _ = create_clients(creds_path)
    
    account_map = get_account_names(admin_client)
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
        
        logger.info(f"Discovered {len(properties)} properties")
    except Exception as e:
        logger.error(f"Property discovery failed: {e}")
    
    return properties

def enrich_properties(property_list: List[Dict], admin_client) -> List[Dict]:
    """Enrich properties with account info - sequential for small lists"""
    account_map = get_account_names(admin_client)
    enriched = []
    
    for prop in property_list:
        try:
            property_id = prop["property_id"]
            property_path = f"properties/{property_id}"
            prop_metadata = admin_client.get_property(name=property_path)
            
            account_id = prop_metadata.parent.split("/")[-1]
            account_name = account_map.get(account_id, f"account_{account_id}")
            
            enriched.append({
                "property_id": property_id,
                "property_name": prop_metadata.display_name,
                "account_id": account_id,
                "account_name": account_name,
                **prop
            })
            
        except Exception as e:
            logger.warning(f"Failed to enrich property {prop}: {e}")
            enriched.append({
                **prop,
                "account_id": "unknown",
                "account_name": "unknown"
            })
    
    return enriched

# === OPTIMIZED QUERY ENGINE ===
def build_dimension_filter(filter_config: Optional[Dict]) -> Optional[FilterExpression]:
    """Build dimension filter from config"""
    if not filter_config:
        return None
        
    and_conditions = filter_config.get("and_group", [])
    if not and_conditions:
        return None
    
    filters = []
    for cond in and_conditions:
        if "field_name" in cond and "string_filter" in cond:
            filters.append(
                FilterExpression(
                    filter=Filter(
                        field_name=cond["field_name"],
                        string_filter=Filter.StringFilter(
                            value=cond["string_filter"]["value"]
                        )
                    )
                )
            )
    
    if filters:
        return FilterExpression(
            and_group=FilterExpression.ListExpression(expressions=filters)
        )
    return None

def execute_large_query(data_client, query_name: str, dimensions: List[Dimension], 
                       metrics: List[Metric], date_range: DateRange, 
                       dimension_filter: Optional[FilterExpression], 
                       prop: Dict, writer: OptimizedCSVWriter):
    """Execute query with pagination for large datasets"""
    
    property_id = prop.get("property_id")
    account_id = prop.get("account_id", "unknown")
    property_name = prop.get("property_name", "")
    account_name = prop.get("account_name", "")
    
    logger.info(f"Starting query '{query_name}' for property {property_id}")
    
    try:
        offset = 0
        total_rows = 0
        
        while True:
            # Rate limiting
            rate_limiter.wait()
            
            # Build request with large limit
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[date_range],
                dimension_filter=dimension_filter,
                limit=LargeDataConfig.MAX_ROWS_PER_REQUEST,
                offset=offset
            )
            
            # Execute request
            report = data_client.run_report(request)
            
            if not report.rows:
                break
            
            # Process batch efficiently
            batch_data = []
            for row in report.rows:
                record = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "property_id": property_id,
                    "property_name": property_name
                }
                
                # Add dimensions
                for i, dim in enumerate(dimensions):
                    record[dim.name] = row.dimension_values[i].value
                
                # Add metrics
                for i, metric in enumerate(metrics):
                    record[metric.name] = row.metric_values[i].value
                
                batch_data.append(record)
            
            # Write batch to streaming writer
            writer.add_batch(batch_data)
            
            batch_size = len(batch_data)
            total_rows += batch_size
            offset += batch_size
            
            logger.info(f"Property {property_id}: Processed {total_rows:,} rows")
            
            # Check if we got less than the limit (last page)
            if batch_size < LargeDataConfig.MAX_ROWS_PER_REQUEST:
                break
        
        logger.info(f"Completed query '{query_name}' for property {property_id}: {total_rows:,} rows")
        return total_rows
        
    except Exception as e:
        logger.error(f"Query '{query_name}' failed for property {property_id}: {e}")
        
        # Log error
        error_path = "/data/out/tables/query_errors.csv"
        os.makedirs(os.path.dirname(error_path), exist_ok=True)
        with open(error_path, "a") as err_file:
            err_file.write(f"{datetime.now()},{query_name},{property_id},\"{str(e)}\"\n")
        
        return 0

def execute_queries_for_large_data(params: Dict, creds_path: str, property_list: List[Dict], 
                                  start_date: str, end_date: str):
    """Execute queries optimized for large datasets"""
    
    data_client, _, _ = create_clients(creds_path)
    
    destination_prefix = params.get("destination", "ga4.output")
    query_definitions = params.get("query_definitions", [])
    output_format = params.get("output_format", "default")
    
    logger.info(f"Executing {len(query_definitions)} queries for {len(property_list)} properties")
    
    for query in query_definitions:
        query_name = query.get("name", "unnamed_query")
        start_time = time.time()
        
        logger.info(f"Starting query: {query_name}")
        
        # Prepare query components
        dimensions = [Dimension(name=d) for d in query.get("dimensions", [])]
        metrics = [Metric(name=m) for m in query.get("metrics", [])]
        dimension_filter = build_dimension_filter(query.get("dimension_filter"))
        date_range = DateRange(start_date=start_date, end_date=end_date)
        
        # Setup output file
        timestamp = datetime.now().strftime("%Y%m%d")
        if output_format == "airbyte_json":
            out_path = f"/data/out/tables/{destination_prefix}-{query_name}-{timestamp}.csv"
        else:
            out_path = f"/data/out/tables/{destination_prefix}.{query_name}.{timestamp}.csv"
        
        # Create optimized writer for large data
        writer = OptimizedCSVWriter(out_path, LargeDataConfig.CHUNK_SIZE)
        
        # Execute query for each property sequentially (more reliable for large data)
        total_query_rows = 0
        for prop in property_list:
            prop_rows = execute_large_query(
                data_client, query_name, dimensions, metrics,
                date_range, dimension_filter, prop, writer
            )
            total_query_rows += prop_rows
        
        # Finalize output
        final_rows = writer.finalize()
        execution_time = time.time() - start_time
        
        if final_rows > 0:
            logger.info(f"Query '{query_name}' completed: {final_rows:,} rows in {execution_time:.1f}s")
            create_manifest(out_path, query_name, destination_prefix, final_rows, property_list, execution_time)
        else:
            logger.warning(f"No data for query '{query_name}'")

def create_manifest(file_path: str, query_name: str, destination_prefix: str, 
                   row_count: int, property_list: List[Dict], execution_time: float):
    """Create performance manifest"""
    
    manifest = {
        "output_table": f"{destination_prefix}.{query_name}",
        "filename": os.path.basename(file_path),
        "format": "csv",
        "row_count": row_count,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "query_name": query_name,
        "property_ids": [prop.get("property_id") for prop in property_list],
        "performance": {
            "execution_time_seconds": round(execution_time, 2),
            "rows_per_second": round(row_count / execution_time) if execution_time > 0 else 0,
            "total_properties": len(property_list)
        }
    }
    
    manifest_path = file_path.replace(".csv", ".manifest.json")
    with open(manifest_path, "w") as mf:
        json.dump(manifest, mf, indent=2, ensure_ascii=False)

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
