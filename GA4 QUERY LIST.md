# 📘 GA4 Query Presets for Meiro Connector (Markdown Format)

This document is a markdown-formatted reference for using the GA4 API with Meiro's Python connector. It includes ready-to-use lists of dimensions, metrics, sample queries, and filter structures.

---

## 📈 Full GA4 Metric List

| Metric                   | Description                                     |
| ------------------------ | ----------------------------------------------- |
| `activeUsers`            | Number of active users                          |
| `newUsers`               | Number of first-time users                      |
| `sessions`               | Count of sessions started                       |
| `screenPageViews`        | Pageviews and screenviews combined              |
| `userEngagementDuration` | Time users spent engaged, in seconds            |
| `bounceRate`             | Percentage of single-interaction sessions       |
| `conversions`            | Count of conversion events                      |
| `engagedSessions`        | Sessions with meaningful engagement             |
| `eventCount`             | Total number of events triggered                |
| `sessionDuration`        | Total session duration in seconds               |
| `totalRevenue`           | Combined revenue from purchases & in-app events |
| `averageSessionDuration` | Avg. time spent per session                     |
| `purchaseRevenue`        | Revenue from purchases only                     |
| `adImpressions`          | Total ad impressions served                     |
| `adClicks`               | Total ad clicks received                        |
| `engagementRate`         | Engaged sessions ÷ total sessions               |
| `views`                  | Total views (content, screen, or page)          |
| `transactions`           | Count of purchase transactions                  |

---

## 🧭 Full GA4 Dimension List

| Dimension        | Description                           |
| ---------------- | ------------------------------------- |
| `date`           | Event date in YYYYMMDD format         |
| `hour`           | Hour of event (0–23)                  |
| `country`        | User's country                        |
| `region`         | User's region or state                |
| `city`           | User's city                           |
| `platform`       | App/Web platform (e.g., Web, Android) |
| `deviceCategory` | Type of device (desktop, mobile)      |
| `browser`        | Browser name used                     |
| `pagePath`       | Page URL path                         |
| `pageTitle`      | Title of visited page                 |
| `screenName`     | App screen name                       |
| `eventName`      | Name of the triggered event           |
| `sessionSource`  | Traffic source (e.g., google)         |
| `sessionMedium`  | Medium (e.g., cpc, referral)          |
| `trafficSource`  | Full source string                    |
| `campaignName`   | UTM campaign name                     |
| `userGender`     | Gender segment                        |
| `userAgeBracket` | Age segment                           |
| `contentGroup`   | Grouped content identifier            |

---

## 🧠 Suggested Query Presets

### 🔹 Engagement by Platform

```json
{
  "dimensions": ["platform"],
  "metrics": ["activeUsers", "userEngagementDuration"]
}
```

### 🔹 Screen Views by Screen Name

```json
{
  "dimensions": ["screenName"],
  "metrics": ["screenPageViews"]
}
```

### 🔹 Bounce Rate by Device

```json
{
  "dimensions": ["deviceCategory"],
  "metrics": ["bounceRate"]
}
```

### 🔹 Revenue by Page

```json
{
  "dimensions": ["pagePath"],
  "metrics": ["purchaseRevenue"]
}
```

---

## 🔒 Example Dimension Filter

```json
"dimension_filter": {
  "and_group": [
    {
      "field_name": "country",
      "string_filter": { "value": "Turkey" }
    },
    {
      "field_name": "platform",
      "string_filter": { "value": "Web" }
    }
  ]
}
```

---

## ✅ Usage Instructions

* Copy any of the above `dimensions`, `metrics`, or `dimension_filter` JSON blocks into your `config.json` file under the `parameters` key.
* Make sure the GA4 property you're querying has access to those fields.
* Combine and modify to suit campaign, performance, or segment-level analysis.

---

## 📚 Reference

* [GA4 Data API Schema](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
* [GA4 Query Explorer](https://ga-dev-tools.google/query-explorer)

Ready to drive insight from every interaction. 🚀
