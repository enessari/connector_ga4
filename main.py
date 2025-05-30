import json
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account

# === AUTH & CLIENT ===

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

# === ENRICHMENT ===

def get_account_display_names(credentials):
    client = create_admin_client(credentials)
    account_map = {}
    try:
        accounts = client.list_accounts()
        for acc in accounts:
            acc_id = acc.name.split("/")[-1]
            account_map[acc_id] = acc.display_name
    except Exception as e:
        print(f"[!] Could not fetch account names: {e}")
    return account_map

def discover_all_properties(service_account_dict):
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
    except Exception as e:
        print(f"[!] Property discovery failed: {e}")
    return properties

def enrich_properties_with_admin_api(property_list, credentials):
    client = create_admin_client(credentials)
    account_map = get_account_display_names(credentials)
    enriched = []
    for prop in property_list:
        try:
            property_id = prop["property_id"]
            property_path = f"properties/{property_id}"
            prop_metadata = client.get_property(name=property_path)
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
            print(f"[!] Failed to enrich property {prop}: {e}")
            enriched.append({ **prop, "account_id": "unknown", "account_name": "unknown" })
    return enriched

# === VALIDATION ===

def validate_query_fields(query_definitions, schema_url, report_path, fail_on_invalid=True):
    print("[INFO] Validating dimensions and metrics...")
    try:
        response = requests.get(schema_url)
        schema = response.json()
        valid_dimensions = {d["apiName"] for d in schema.get("dimensions", [])}
        valid_metrics = {m["apiName"] for m in schema.get("metrics", [])}
    except Exception as e:
        print(f"[WARNING] Could not fetch GA4 schema: {e}")
        return

    invalid_queries = []
    for query in query_definitions:
        name = query.get("name", "unnamed")
        dims = query.get("dimensions", [])
        mets = query.get("metrics", [])

        invalid_dims = [d for d in dims if d not in valid_dimensions]
        invalid_mets = [m for m in mets if m not in valid_metrics]

        if invalid_dims or invalid_mets:
            print(f"[❌] Invalid fields in query '{name}':")
            if invalid_dims:
                print(f"    → Invalid dimensions: {invalid_dims}")
            if invalid_mets:
                print(f"    → Invalid metrics: {invalid_mets}")
            invalid_queries.append({
                "query": name,
                "invalid_dimensions": invalid_dims,
                "invalid_metrics": invalid_mets
            })

    if invalid_queries:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w") as f:
            f.write("query,invalid_dimensions,invalid_metrics\n")
            for iq in invalid_queries:
                f.write(f"{iq['query']},\"{','.join(iq['invalid_dimensions'])}\",\"{','.join(iq['invalid_metrics'])}\"\n")
        if fail_on_invalid:
            raise ValueError(f"[ABORTED] {len(invalid_queries)} query(ies) have invalid fields. See {report_path}")

# === QUERY ENGINE ===

def inject_date_dimension(query_definitions):
    for query in query_definitions:
        dimensions = query.get("dimensions", [])
        if "date" not in dimensions:
            dimensions.insert(0, "date")
            query["dimensions"] = dimensions
    return query_definitions

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

def save_query_results(results, destination_prefix, query_name, output_format="default"):
    if results:
        df = pd.DataFrame(results)
        os.makedirs("/data/out/tables", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d")

        # output file name based on format
        if output_format == "airbyte_json":
            df["_airbyte_data"] = df.apply(lambda row: json.dumps(row.to_dict(), ensure_ascii=False), axis=1)
            df = df[["_airbyte_data"]]
            out_path = f"/data/out/tables/{destination_prefix}-{query_name}-{timestamp}.csv"
        else:
            out_path = f"/data/out/tables/{destination_prefix}.{query_name}.{timestamp}.csv"

        df.to_csv(out_path, index=False)
        print(f"[✓] Query '{query_name}' written to {out_path}")

        # extract properties and accounts used
        property_ids = list({row.get("property_id", "unknown") for row in results})
        account_ids = list({row.get("account_id", "unknown") for row in results})
        property_names = list({row.get("property_name", "") for row in results})
        account_names = list({row.get("account_name", "") for row in results})
        dimensions = df.columns.tolist()

        # write manifest
        manifest = {
            "output_table": f"{destination_prefix}.{query_name}",
            "filename": os.path.basename(out_path),
            "format": "csv",
            "row_count": len(df),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "query_name": query_name,
            "dimensions": dimensions,
            "property_ids": property_ids,
            "account_ids": account_ids,
            "property_names": property_names,
            "account_names": account_names
        }

        manifest_path = out_path.replace(".csv", ".manifest.json")
        with open(manifest_path, "w") as mf:
            json.dump(manifest, mf, indent=2, ensure_ascii=False)
        print(f"[i] Manifest written to {manifest_path}")

    else:
        print(f"[!] No data for query '{query_name}'")


def execute_ga4_queries(params, creds_path, property_list, start_date, end_date):
    credentials = build_credentials(creds_path)
    data_client = create_data_client(credentials)
    destination_prefix = params.get("destination", "ga4.output")
    query_definitions = params.get("query_definitions", [])
    output_format = params.get("output_format", "default")

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
        save_query_results(all_results, destination_prefix, query_name, output_format)

def main():
    try:
        with open('/data/config.json', 'r') as f:
            raw = f.read()
            config = json.loads(raw)
    except Exception as e:
        print("[ERROR] Cannot read or parse /data/config.json")
        raise e

    params = config.get("parameters", {})
    if "parameters" in params:
        print("[INFO] Detected nested 'parameters' block in config — flattening...")
        params = params["parameters"]

    service_account_json = params.get("service_account_json")
    if not service_account_json or not isinstance(service_account_json, dict) or "private_key" not in service_account_json:
        raise ValueError("Missing service account credentials")

    property_list = params.get("property_list", [])
    if not params.get("start_date") or not params.get("end_date"):
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=7)
        print(f"[INFO] No date provided. Using default range: {start_date} to {end_date}")
    else:
        start_date = params.get("start_date")
        end_date = params.get("end_date")

    creds_path = write_temp_credentials(service_account_json)
    credentials = build_credentials(creds_path)

    if not property_list:
        print("[INFO] No property list provided. Discovering all accessible GA4 properties via Admin API...")
        property_list = discover_all_properties(service_account_json)
    else:
        print("[INFO] Enriching property list with Admin API metadata...")
        property_list = enrich_properties_with_admin_api(property_list, credentials)

    query_definitions = params.get("query_definitions", [])
    validation = params.get("validation", {})
    if validation.get("enabled"):
        validate_query_fields(
            query_definitions=query_definitions,
            schema_url=validation.get("schema_url"),
            report_path=validation.get("report_invalid_to", "/data/out/tables/invalid_queries.csv"),
            fail_on_invalid=validation.get("fail_on_invalid", True)
        )

    params["query_definitions"] = inject_date_dimension(query_definitions)
    execute_ga4_queries(params, creds_path, property_list, str(start_date), str(end_date))

if __name__ == "__main__":
    main()
