import csv
import os
import time
from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('API_KEY')

if not API_KEY:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

BASE_URL = 'https://api.messier.app/api/v1/virgo/proposal-list?page=1&limit=100'

# Set up headers with the API key
headers = {
    'accept': 'application/json',
    'X-API-KEY': API_KEY
}

# Fetch the data from the API
response = requests.get(BASE_URL, headers=headers)

if response.status_code != 200:
    print(f'Error fetching data from API. Status code: {response.status_code}')
    print(f'Response content: {response.text}')
    exit()  # Exit the script if there's an error

# Parse the JSON response
try:
    json_response = response.json()
except ValueError:
    print('Failed to parse JSON response')
    print(f'Response content: {response.text}')
    exit()  # Exit the script if JSON parsing fails

# Extract the proposals data
proposals = json_response.get('data', {}).get('proposals', [])

# Print the number of proposals fetched
print(f'Number of records fetched on page 1: {len(proposals)}')

# Exit if no proposals were fetched
if not proposals:
    print('No more proposals found.')
    exit()

# Define the CSV filename
csv_filename = "proposals_data.csv"

# Open a CSV file to write
try:
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        
        # Write CSV header
        writer.writerow([
            "status", "state", "title", "type", "votetype", "creator_address", 
            "creator_username", "currency_symbol", "currency_address", "signers", "neededSign"
        ])
        
        # Iterate over proposals and write rows to the CSV
        for proposal in proposals:
            status = proposal.get("status", "")
            state = proposal.get("state", "")
            title = proposal.get("title", "")
            proposal_type = proposal.get("type", "")
            vote_type = proposal.get("voteType", "")
            
            # Creator information
            creator = proposal.get("creator", {})
            creator_address = creator.get("address", "")
            creator_username = creator.get("username", "")
            
            # Currency Information
            currency = proposal.get("currency", {})
            currency_symbol = currency.get("symbol", "")
            currency_address = currency.get("contractAddress", "")  # Corrected the field name
            
            # Signers information as an array of objects
            signers = proposal.get("singers", [])
            
            # Convert signers list of dicts to a string for CSV
            signers_str = str(signers)
            
            needed_sign = proposal.get("neededSign", 0)
            
            # Write the proposal data to the CSV
            writer.writerow([
                status, state, title, proposal_type, vote_type, creator_address, 
                creator_username, currency_symbol, currency_address, signers_str, needed_sign
            ])

    # Get the size of the CSV file
    file_size = os.path.getsize(csv_filename)

    # Convert the file size to a more readable format (e.g., KB, MB)
    def sizeof_fmt(num, suffix='B'):
        for unit in ['','K','M','G','T','P','E','Z']:
            if abs(num) < 1024.0:
                return f"{num:3.1f} {unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f} Y{suffix}"

    readable_size = sizeof_fmt(file_size)
    print(f'Size of the CSV file: {readable_size}')
    print(f'Number of records saved: {len(proposals)}')

except IOError:
    print('I/O error while writing to the CSV file')
