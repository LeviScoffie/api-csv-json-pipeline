import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os

# API Details
API_URL = "https://api.messier.app/api/v1/platform/fee"
API_KEY = os.getenv("API_KEY")
SERVICES = ["horizon", "adastra", "openhatch", "p2p", "virgo"]
HEADERS = {"accept": "application/json", "X-API-KEY": API_KEY}

# Fetch data from API
def fetch_data(service):
    response = requests.get(f"{API_URL}?service={service}", headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("data", {}).get("items", [])
    else:
        print(f"Error fetching data for {service}: {response.status_code}")
        return []

# Process all services
data_list = []
for service in SERVICES:
    data = fetch_data(service)
    for item in data:
        data_list.append({
            "network": item.get("network"),
            "app": service,
            "contract_address": item.get("contract", "null"),
            "token_symbol": item.get("symbol"),
            "value": item.get("value")
        })

# Convert to DataFrame
df = pd.DataFrame(data_list)

# Save to CSV
csv_filename = "fees_data.csv"
df.to_csv(csv_filename, index=False)

# Save to JSON
json_filename = "fees_data.json"
df.to_json(json_filename, orient="records", indent=4)

# Upload to jsonBlob
json_blob_url = "https://jsonblob.com/api/jsonBlob"
with open(json_filename, "r") as json_file:
    json_data = json.load(json_file)
response = requests.post(json_blob_url, json=json_data, headers={"Content-Type": "application/json"})

# Check JSONBlob response
if response.status_code in [200, 201]:
    json_blob_link = response.headers.get("Location", "No URL returned")
    print(f"JSON Blob created successfully: {json_blob_link}")
else:
    print(f"Failed to create JSON Blob: {response.status_code}")

# Print number of records
print(f"CSV file records: {len(df)}")
print(f"JSON file records: {len(json_data)}")
