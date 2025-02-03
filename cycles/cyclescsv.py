import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch the API key from the environment
API_KEY = os.getenv("API_KEY")

# API Endpoint
API_URL = "https://api.messier.app/api/v1/virgo/cycle"

# JSONBlob API URL
JSONBLOB_API_URL = "https://jsonblob.com/api/jsonBlob"

def fetch_api_data():
    """Fetches data from the given API."""
    if not API_KEY:
        print("Error: API_KEY not found in the environment. Please check your .env file.")
        return None
    try:
        headers = {
            "accept": "application/json",
            "X-API-KEY": API_KEY
        }
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def transform_data(api_response):
    """Transforms the API response into the desired JSON structure."""
    if "data" in api_response:
        data = api_response["data"]
        return {
            "proposals_made": data.get("currentProposalCycle", 0),
            "cycle_number": data.get("currentFullCycle", 0),
            "proposals_left_in_cycle": data.get("fullCycleProposalRemainToEnd", 0)
        }
    return {}

def upload_to_jsonblob(data):
    """Uploads the transformed JSON to JSONBlob."""
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(JSONBLOB_API_URL, headers=headers, json=data)
        response.raise_for_status()  # Raise HTTPError for bad responses
        print(f"JSONBlob created successfully! URL: {response.headers.get('Location')}")
    except requests.exceptions.RequestException as e:
        print(f"Error uploading data to JSONBlob: {e}")

def main():
    # Step 1: Fetch API data
    api_response = fetch_api_data()
    if not api_response:
        return

    # Step 2: Transform the data
    transformed_data = transform_data(api_response)
    print("Transformed Data:", transformed_data)

    # Step 3: Upload to JSONBlob
    upload_to_jsonblob(transformed_data)

if __name__ == "__main__":
    main()
