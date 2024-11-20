import csv
import json
import os

# Define the CSV and JSON filenames
csv_filename = "proposals_data.csv"
json_filename = "proposals_data.json"

# Check if the CSV file exists
if not os.path.exists(csv_filename):
    print(f"CSV file '{csv_filename}' does not exist.")
    exit()

# Initialize an empty list to store the data
data = []

# Open the CSV file and read it
with open(csv_filename, mode="r", encoding="utf-8") as file:
    csv_reader = csv.DictReader(file)
    
    # Convert each row of the CSV into a dictionary and add it to the list
    for row in csv_reader:
        data.append(row)

# Write the data to a JSON file
with open(json_filename, mode="w", encoding="utf-8") as json_file:
    json.dump(data, json_file, indent=4)

print(f"CSV file '{csv_filename}' has been successfully converted to JSON file '{json_filename}'.")
