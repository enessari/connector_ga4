import os
import json
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.analytics.admin import AnalyticsAdminServiceClient
from google.oauth2 import service_account


def write_temp_credentials(service_account_dict):
    creds_path = "/tmp/service_account.json"
    with open(creds_path, "w") as f:
        json.dump(service_account_dict, f)
    return creds_path


def get_property_metadata(property_id, creds_path):
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    admin_client = AnalyticsAdminServiceClient(credentials=credentials)
    try:
        property_path = f"properties/{property_id}"
        prop = admin_client.get_property(name=property_path)
        return {
            "property_id": property_id,
            "property_display_name": prop.display_name,
            "account_id": prop.parent.split("/")[-1]
        }
    except Exception as e:
        print(f"[!] Error fetching metadata for {property_id}: {e}")
        return {
            "property_id": property_id,
            "property_display_name": "unknown",
            "account_id": "unknown"
        }


def run_report(property_id, creds_path):
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    data_client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date="2023-01-01", end_date="2023-01-31")],
        dimensions=[Dimension(name="country")],
        metrics=[Metric(name="activeUsers")],
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

    if not service_account_json or not property_list:
        raise ValueError("Missing service account or property list")

    creds_path = write_temp_credentials(service_account_json)
    all_results = []

    for prop in property_list:
        property_id = prop.get("property_id")
        if not property_id:
            continue

        metadata = get_property_metadata(property_id, creds_path)
        print(f"▶ {metadata['property_display_name']} ({metadata['account_id']})")

        try:
            report = run_report(property_id, creds_path)
            for row in report.rows:
                all_results.append({
                    "account_id": metadata["account_id"],
                    "property_id": metadata["property_id"],
                    "property_name": metadata["property_display_name"],
                    "country": row.dimension_values[0].value,
                    "active_users": row.metric_values[0].value
                })
        except Exception as e:
            print(f"[!] Report failed for property {property_id}: {e}")

    write_output(all_results, destination)


if __name__ == "__main__":
    main()
