from datetime import datetime, timedelta

def enrich_properties_with_admin_api(property_list, credentials):
    client = create_admin_client(credentials)
    enriched = []
    for prop in property_list:
        try:
            property_id = prop["property_id"]
            property_path = f"properties/{property_id}"
            prop_metadata = client.get_property(name=property_path)
            account_id = prop_metadata.parent.split("/")[-1]
            account_name = f"account_{account_id}"
            enriched.append({
                "property_id": property_id,
                "property_name": prop_metadata.display_name,
                "account_id": account_id,
                "account_name": account_name
            })
        except Exception as e:
            print(f"[!] Failed to enrich property {prop}: {e}")
            enriched.append({ **prop, "account_id": "unknown", "account_name": "unknown" })
    return enriched

def inject_date_dimension(query_definitions):
    for query in query_definitions:
        dimensions = query.get("dimensions", [])
        if "date" not in dimensions:
            dimensions.insert(0, "date")
            query["dimensions"] = dimensions
    return query_definitions

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

    # calculate default date range (last 7 days if not provided)
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

    # ensure date dimension is included in every query
    query_definitions = params.get("query_definitions", [])
    params["query_definitions"] = inject_date_dimension(query_definitions)

    execute_ga4_queries(params, creds_path, property_list, str(start_date), str(end_date))

if __name__ == "__main__":
    main()
