# 📊 GA4 Connector for CDPs (Meiro supported)

This Python-based connector allows you to extract Google Analytics 4 (GA4) data through Google's official APIs and deliver it into Meiro CDP pipelines. It supports multiple properties, dynamic field selection, date range control, optional segmentation, and outputs query-specific results per run.

---

## 🔧 Features

* ✅ Support for multiple GA4 properties across accounts
* 🔁 Automatic discovery of properties if `property_list` is not provided
* 🗕️ Customizable date range via `start_date` and `end_date` (defaults to last 7 days)
* 🧱 Dynamic selection of dimensions and metrics (with optional filter support)
* 🎯 Auto-injected `date` dimension if not defined in query
* 📄 Admin API-based enrichment of `account_name` and `property_name`
* ❌ Skips invalid queries or logs them to `/data/out/tables/invalid_queries.csv`
* 📅 Output saved as individual `.csv` files for each query under `/data/out/tables/`
* 🧪 Unit-testable modular architecture with optional schema validation support

---

## 🤩 How It Works in Meiro

This connector is designed to work with Meiro's **Python from Git Repository** processor.

It reads parameters from `/data/config.json`, including:
- `service_account_json`
- `property_list`
- `query_definitions`
- `validation` (optional)

---

## 🦪 Example `config.json`

```json
{
  "parameters": {
    "service_account_json": { "type": "service_account", ... },
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "destination": "analytics.profiles",
    "property_list": [
      { "property_id": "123456789" },
      { "property_id": "123456789" }
    ],
    "output_format": "default"  // or "airbyte_json"
    "query_definitions": [
      {
        "name": "users_by_platform",
        "dimensions": ["platform"],
        "metrics": ["activeUsers", "newUsers"]
      },
      {
        "name": "screenviews_by_screen",
        "dimensions": ["screenName"],
        "metrics": ["screenPageViews"]
      }
    ],
    "validation": {
      "enabled": true,
      "schema_url": "https://www.googleapis.com/analytics/v1alpha/metadata/ga4?fields=dimensions,metrics",
      "fail_on_invalid": true,
      "report_invalid_to": "/data/out/tables/invalid_queries.csv"
    }
  }
}
```

> ℹ️ `account_id` and `account_name` fields are auto-enriched if omitted.

---

## 📁 File Structure

```
connector_ga4/
├── main.py                # Main execution file
├── unittest.py            # Placeholder for future unit tests
├── requirements.txt       # Dependencies
├── README.md              # Documentation
└── README_QUERY_PRESETS.md# Query preset reference
```

---

## 📆 Output Format

Each query is written to:
```
/data/out/tables/{destination}.{query_name}.{YYYYMMDD}.csv
```
Example:
```
/data/out/tables/analytics.profiles.users_by_platform.20240530.csv
```

Sample output:
| date       | account_id | account_name | property_id | property_name | platform | activeUsers | newUsers |
|------------|------------|--------------|-------------|----------------|----------|-------------|----------|
| 2024-01-01 | 1234567    | ABC          | 123456789   | ZYX            | Web      | 1500        | 300      |

---

## 🛠 Dependencies

```txt
google-analytics-data>=0.14.1
google-analytics-admin>=0.24.0
pandas>=1.5.0
requests
```
Install:
```bash
pip install -r requirements.txt
```

---

## ⚠️ Notes

* Requires access to both the [GA4 Admin API](https://developers.google.com/analytics/devguides/config/admin/v1) and [GA4 Data API](https://developers.google.com/analytics/devguides/reporting/data/v1)
* Filters support only `string_filter` inside `AND` logic group
* Schema validation uses Google's official GA4 API metadata
* Each query is executed per property; failures are logged and skipped

---

## 🧪 Testing & Coverage

Run all tests:
```bash
coverage run -m unittest discover
coverage report -m
coverage html && open htmlcov/index.html
```

Tested modules:
- Credential I/O
- Query execution
- Property enrichment
- CSV output writing
- Validation logic
- Filter parsing

---

## 🧐 Use Cases

* Fetch user metrics by platform/device
* Build event and conversion summaries by region
* Run cohort-style multi-day metrics
* Export structured data for Metabase or dashboard tooling

---

## 🧭 Eksiklikler ve İyileştirme Önerileri

* **Test kapsamı eksik:** `unittest.py` şimdilik yalnızca yer tutucu içeriyor. Gerçekçi senaryoları kapsayan testlerin eklenmesi gerekiyor.
* **Güvenlik iletişim akışının tanımlanması gerekiyor:** `SECURITY.md` altında ileride kullanılacak bildirim sürecini belirlemek için bir eylem planı oluşturun.
* **Otomatik CI/CD ve kalite kontrolleri yok:** Bağımlılık güvenliği ve stil denetimleri için GitHub Actions veya benzeri bir boru hattı eklemek sürdürülebilirliği artırır.
* **Konfigürasyon örnekleri geliştirilebilir:** Büyük hacimli veri senaryoları için oran sınırlayıcı ve dosya çıktısı seçeneklerini belgeleyen ek örnekler, kullanıcıların karmaşık kurulumlarda hata yapmasını engeller.

## 👥 Sürdürülebilirlik ve İletişim

Bu proje artık **ONMARTECH LLC** tarafından sürdürülmektedir. Öneri, hata veya geliştirme talepleriniz için lütfen bir konu açın.

---

Built with ❤️ for intelligent analytics and scalable data pipelines.
