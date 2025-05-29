import os
import json
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, Filter, FilterExpression
)
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account


def write_temp_credentials(service_account_dict):
    creds_path = "/tmp/service_account.json"
    with open(creds_path, "w") as f:
        json.dump(service_account_dict, f)
    return creds_path


def discover_all_properties(service_account_dict):
    creds_path = write_temp_credentials(service_account_dict)
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    admin_client = AnalyticsAdminServiceClient(credentials=credentials)

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
        print(f"[!] Error discovering properties: {e}")
    return properties


def run_report(property_id, creds_path, start_date, end_date, dimensions, metrics, segment_name=None):
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    data_client = BetaAnalyticsDataClient(credentials=credentials)

    dim_objs = [Dimension(name=d) for d in dimensions]
    met_objs = [Metric(name=m) for m in metrics]

    filter_expr = None
    if segment_name:
        filter_expr = FilterExpression(
            filter=Filter(
                field_name="segment",
                string_filter=Filter.StringFilter(value=segment_name)
            )
        )

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=dim_objs,
        metrics=met_objs,
        dimension_filter=filter_expr
    )

    return data_client.run_report(request)


def write_output(data: list, destination: str):
    df = pd.DataFrame(data)
    os.makedirs("/data/out/tables", exist_ok=True)
    out_path = f"/data/out/tables/{destination}.csv"
    df.to_csv(out_path, index=False)
    print(f"[✓] Output written to {out_path}")


def main():
    with open('/data/config.json', 'r') as f:
        config = json.load(f)

    params = config.get("parameters", {})
    service_account_json = params.get("service_account_json")
    property_list = params.get("property_list", [])
    destination = params.get("destination", "analytics.profiles")
    start_date = params.get("start_date", "2023-01-01")
    end_date = params.get("end_date", "2023-01-31")
    dimensions = params.get("dimensions", ["country"])
    metrics = params.get("metrics", ["activeUsers"])
    segment_name = params.get("segment_name")

    if not service_account_json:
        raise ValueError("Missing service account JSON.")

    creds_path = write_temp_credentials(service_account_json)

    if not property_list:
        print("🔁 No property_list provided, discovering all GA4 properties...")
        property_list = discover_all_properties(service_account_json)

    all_results = []

    for prop in property_list:
        property_id = prop.get("property_id")
        property_name = prop.get("property_name", "unknown")
        account_id = prop.get("account_id", "unknown")
        account_name = prop.get("account_name", "")

        print(f"▶ Running report for: {property_name} ({property_id})")

        try:
            report = run_report(
                property_id=property_id,
                creds_path=creds_path,
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                metrics=metrics,
                segment_name=segment_name
            )

            for row in report.rows:
                row_data = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "property_id": property_id,
                    "property_name": property_name
                }

                row_data.update({
                    dimensions[i]: row.dimension_values[i].value
                    for i in range(len(dimensions))
                })
                row_data.update({
                    metrics[i]: row.metric_values[i].value
                    for i in range(len(metrics))
                })

                all_results.append(row_data)

        except Exception as e:
            print(f"[!] Failed for property {property_id}: {e}")

    if all_results:
        write_output(all_results, destination)
    else:
        print("[!] No data retrieved.")


if __name__ == "__main__":
    main()
