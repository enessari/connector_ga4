import json
import os

def main():
    # Parametreleri oku
    with open('/data/config.json', 'r') as f:
        config = json.load(f)
    params = config.get("parameters", {})

    # API parametre örneği
    property_id = params.get("property_id")
    print(f"Running GA4 connector for property ID: {property_id}")

    # Daha sonra: requests veya google analytics API çağrıları

if __name__ == "__main__":
    main()
