import csv
import json

csv_file = "virgo_users_data.csv"
json_file = "virgo_users_data.json"

# Read CSV and convert to JSON
csv_data = []
with open(csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        csv_data.append(row)

# Write to JSON file
with open(json_file, mode='w') as file:
    json.dump(csv_data, file, indent=4)

print(f"Data converted to JSON and saved to {json_file}")
