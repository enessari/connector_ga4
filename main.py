from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import json
import os

def fetch_report_from_raw_json(service_account_dict, property_id):
    # Geçici dosyaya yaz
    temp_path = "/tmp/temp_service_account.json"
    with open(temp_path, "w") as f:
        json.dump(service_account_dict, f)

    credentials = service_account.Credentials.from_service_account_file(temp_path)
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date="2023-01-01", end_date="2023-01-31")],
        dimensions=[Dimension(name="country")],
        metrics=[Metric(name="activeUsers")],
    )

    response = client.run_report(request)
    return response

def main():
    with open('/data/config.json', 'r') as f:
        config = json.load(f)

    params = config.get("parameters", {})
    property_id = params.get("property_id")
    service_account_raw = params.get("service_account_json")  # <- Burada Base64 değil, direkt JSON dict bekliyoruz

    if not service_account_raw or not property_id:
        raise ValueError("Missing required parameters.")

    print(f"Running GA4 connector for property ID: {property_id}")
    response = fetch_report_from_raw_json(service_account_raw, property_id)

    for row in response.rows:
        print(
            [d.value for d in row.dimension_values],
            [m.value for m in row.metric_values]
        )

if __name__ == "__main__":
    main()
