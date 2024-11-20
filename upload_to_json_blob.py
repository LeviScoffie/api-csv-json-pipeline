import requests
import json
# Read JSON data
with open("proposals_data.json", 'r') as file:
    json_data = json.load(file)

# Upload to JSONBlob (LazyAPI)
response = requests.post("https://jsonblob.com/api/jsonBlob", json=json_data)

if response.status_code == 201:
    new_api_url = response.headers['Location']
    print(f"JSON successfully uploaded. Access it using the new API URL: {new_api_url}")
else:
    print(f"Failed to upload JSON. Status Code: {response.status_code}")
