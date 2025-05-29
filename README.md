# 📊 GA4 Connector for Meiro CDP

This Python-based connector allows you to extract Google Analytics 4 (GA4) data through Google's official APIs and deliver it into Meiro CDP pipelines. It supports multiple properties, dynamic field selection, date range control, and optional segmentation.

---

## 🔧 Features

* ✅ Support for multiple GA4 properties across accounts
* 🔁 Automatic discovery of properties if `property_list` is not provided
* 🗕️ Customizable date range via `start_date` and `end_date`
* 🧱 Dynamic selection of dimensions and metrics
* 🎯 Optional segment-level filtering
* 📅 Output is saved as a `.csv` file in `/data/out/tables/` for Meiro ingestion

---

## 🤩 How It Works in Meiro

This connector is designed to work with Meiro's **Python from Git Repository** processor.

It reads parameters from `/data/config.json` as shown below.

---

## 🦪 Example `config.json` Parameters

```json
{
  "parameters": {
    "service_account_json": {
      "type": "service_account",
      "project_id": "ga4-project",
      "private_key_id": "...",
      "private_key": "-----BEGIN PRIVATE KEY-----\\nABC...\\n-----END PRIVATE KEY-----\\n",
      "client_email": "ga4@ga4-project.iam.gserviceaccount.com",
      "client_id": "1234567890",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/..."
    },
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "dimensions": ["country", "platform"],
    "metrics": ["activeUsers", "newUsers"],
    "segment_name": "engaged_users",
    "property_list": [
      { "property_id": "207548386" },
      { "property_id": "332683219" }
    ],
    "destination": "analytics.profiles"
  }
}
```

> ℹ️ If `property_list` is **not provided**, the connector will auto-discover all accessible GA4 properties using the Admin API. This is useful when running a global connector across all projects/accounts the service account has access to. Automatically discovered properties will include `account_id`, `account_name`, `property_id`, and `property_name` in the output.

---

## 📁 File Structure

```
connector_ga4/
├── main.py                # Main execution file for Meiro connector
├── requirements.txt       # Python dependencies
└── README.md              # Full documentation (this file)
```

---

## 📆 Output Format

The connector outputs a single `.csv` file at:

```
/data/out/tables/{destination}.csv
```

Example output:

| account\_id | account\_name | property\_id | property\_name | country | platform | activeUsers | newUsers |
| ----------- | ------------- | ------------ | -------------- | ------- | -------- | ----------- | -------- |
| 4752478     | ETS           | 207548386    | ETS Web        | Turkey  | Web      | 1500        | 300      |

---

## 🛠 Dependencies

See `requirements.txt`:

```txt
google-analytics-data==0.14.1
google-analytics-admin==1.14.1
pandas
```

Install using:

```bash
pip install -r requirements.txt
```

---

## ⚠️ Notes

* Requires access to both the [GA4 Admin API](https://developers.google.com/analytics/devguides/config/admin/v1) and [GA4 Data API](https://developers.google.com/analytics/devguides/reporting/data/v1).
* Segment filtering is applied as a simple `dimension_filter` on the `segment` field.
* If dimension/metric names are invalid, the request will fail silently per property.
* Date format must be `YYYY-MM-DD`
* Supports Python 3.8+, tested with `pandas >= 1.5`

---

## 🧐 Example Use Cases

* Fetch `activeUsers` and `newUsers` by `country` and `platform`
* Segment reports for users who are "engaged"
* Discover all GA4 properties across all accessible accounts if not listed manually
* Run custom reports for specific time frames

---

## 👤 Maintainer

Created and maintained by [Abdullah Enes Sarı](https://github.com/enessari)
For feedback, issues, or contributions — feel free to connect or fork the project.

---

Built with ❤️ for intelligent analytics and actionable data pipelines.
