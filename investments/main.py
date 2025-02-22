import requests
import csv
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

BASE_URL = "https://api.messier.app/api/v1/platform/investment"

# We'll define a simple function to fetch data

def fetch_investment_data():
    headers = {
        "accept": "application/json",
        "X-API-KEY": API_KEY
    }
    try:
        response = requests.get(BASE_URL, headers=headers)
        if response.status_code != 200:
            print(f"HTTP Error: {response.status_code}")
            return None
        return response.json()
    except Exception as e:
        print(f"Exception while fetching data: {e}")
        return None


def main():
    # 1. Fetch the data
    json_response = fetch_investment_data()
    if not json_response:
        print("No data fetched.")
        return

    # 2. Extract the single value we care about: "mttRawValue"
    # It's located under "data" -> "virgo" -> "mttRawValue"
    data_obj = json_response.get("data", {})
    virgo_obj = data_obj.get("virgo", {})

    # The script specifically wants the key "mttRawValue".
    # We'll store it as "value_staked", and token = "mtt"
    value_staked = virgo_obj.get("mttRawValue", 0)

    # 3. Prepare the row with the columns token, value_staked
    row_data = {
        "token": "mtt",
        "value_staked": value_staked
    }

    # 4. Save to CSV
    csv_filename = "investment_data.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["token", "value_staked"])
        writer.writeheader()
        writer.writerow(row_data)

    print(f"CSV file saved: {csv_filename}")

    # 5. Save to JSON
    json_filename = "investment_data.json"
    with open(json_filename, "w", encoding="utf-8") as jf:
        # We'll store the data as an array with one row, just to keep consistent with typical usage
        json.dump([row_data], jf, indent=4)

    print(f"JSON file saved: {json_filename}")

    # 6. Upload JSON to jsonBlob
    try:
        with open(json_filename, "r", encoding="utf-8") as jf:
            json_data = json.load(jf)
        json_blob_url = "https://jsonblob.com/api/jsonBlob"
        headers = {"Content-Type": "application/json"}
        blob_response = requests.post(json_blob_url, json=json_data, headers=headers)

        if blob_response.status_code in [200, 201]:
            json_blob_link = blob_response.headers.get("Location", "No URL returned")
            print(f"JSON Blob created successfully: {json_blob_link}")
        else:
            print(f"Failed to create JSON Blob. Status: {blob_response.status_code}")
    except Exception as e:
        print(f"Exception uploading JSON to jsonBlob: {e}")


if __name__ == "__main__":
    main()