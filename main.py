import json
import os
import pandas as pd
from datetime import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account

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

def discover_all_properties(service_account_dict):
    creds_path = write_temp_credentials(service_account_dict)
    credentials = build_credentials(creds_path)
    admin_client = create_admin_client(credentials)
    properties = []
    try:
        accounts = admin_client.list_accounts()
        for acc in accounts:
            acc_id = acc.name.split("/")[-1]
            acc_name = acc.display_name
            prop_list = admin_client.list_properties(parent=acc.name)
            for prop in prop_list:
                properties.append({
                    "account_id": acc_id,
                    "account_name": acc_name,
                    "property_id": prop.name.split("/")[-1],
                    "property_name": prop.display_name
                })
    except Exception as e:
        print(f"[!] Property discovery failed: {e}")
    return properties

def build_dimension_filter(filter_config):
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

def run_query_for_property(data_client, query_name, dimensions, metrics, date_range, dimension_filter, prop):
    property_id = prop.get("property_id")
    account_id = prop.get("account_id", "unknown")
    property_name = prop.get("property_name", "")
    account_name = prop.get("account_name", "")
    results = []

    try:
        report = data_client.run_report(RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[date_range],
            dimension_filter=dimension_filter
        ))

        for row in report.rows:
            record = {
                "account_id": account_id,
                "account_name": account_name,
                "property_id": property_id,
                "property_name": property_name
            }
            record.update({dimensions[i].name: row.dimension_values[i].value for i in range(len(dimensions))})
            record.update({metrics[i].name: row.metric_values[i].value for i in range(len(metrics))})
            results.append(record)

    except Exception as e:
        print(f"[!] Query '{query_name}' failed for property {property_id}: {e}")
        with open("/data/out/tables/query_errors.csv", "a") as err_file:
            err_file.write(f"{datetime.now()},\"{query_name}\",{property_id},\"{e}\"\n")
    return results

def save_query_results(results, destination_prefix, query_name):
    if results:
        df = pd.DataFrame(results)
        os.makedirs("/data/out/tables", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d")
        out_path = f"/data/out/tables/{destination_prefix}.{query_name}.{timestamp}.csv"
        df.to_csv(out_path, index=False)
        print(f"[✓] Query '{query_name}' written to {out_path}")
    else:
        print(f"[!] No data for query '{query_name}'")

def execute_ga4_queries(params, creds_path, property_list, start_date, end_date):
    credentials = build_credentials(creds_path)
    data_client = create_data_client(credentials)
    destination_prefix = params.get("destination", "ga4.output")
    query_definitions = params.get("query_definitions", [])

    print("[INFO] Running GA4 Queries...")

    for query in query_definitions:
        query_name = query.get("name", "query")
        print(f"[→] Starting query: {query_name}")
        dimensions = [Dimension(name=d) for d in query.get("dimensions", [])]
        metrics = [Metric(name=m) for m in query.get("metrics", [])]
        dimension_filter = build_dimension_filter(query.get("dimension_filter"))
        date_range = DateRange(start_date=start_date, end_date=end_date)

        all_results = []
        for prop in property_list:
            print(f"   ↳ Running for property: {prop.get('property_id')} ({prop.get('property_name', '')})")
            all_results.extend(
                run_query_for_property(
                    data_client, query_name, dimensions, metrics, date_range, dimension_filter, prop
                )
            )

        save_query_results(all_results, destination_prefix, query_name)

def main():

    with open("/data/config.json", "r") as f:
    raw = f.read()
    print("[DEBUG] Raw config.json:")
    print(raw[:1000])  # ilk 1000 karakter yeterli
    config = json.loads(raw)

    try:
        with open('/data/config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        print("[ERROR] Cannot read /data/config.json")
        raise e

    params = config.get("parameters", {})
    service_account_json = params.get("service_account_json")

    print("[DEBUG] Loaded service_account_json:", json.dumps(service_account_json, indent=2)[:500], "...\n")

    if not service_account_json or not isinstance(service_account_json, dict) or "private_key" not in service_account_json:
        raise ValueError("Missing service account credentials")

    property_list = params.get("property_list", [])
    start_date = params.get("start_date", "2024-01-01")
    end_date = params.get("end_date", "2024-01-31")

    if not property_list:
        print("[i] No property list provided. Discovering all accessible GA4 properties...")
        property_list = discover_all_properties(service_account_json)

    creds_path = write_temp_credentials(service_account_json)
    execute_ga4_queries(params, creds_path, property_list, start_date, end_date)

if __name__ == "__main__":
    main()
