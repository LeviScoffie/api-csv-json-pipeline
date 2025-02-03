import csv
import os
import time
from dotenv import load_dotenv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('API_KEY')

if not API_KEY:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

BASE_URL = 'https://api.messier.app/api/v1/virgo/proposal-list'
LIMIT = 100  # Number of proposals per page

# Set up headers with the API key
headers = {
    'accept': 'application/json',
    'X-API-KEY': API_KEY
}

# Set to store unique proposal identifiers
unique_proposals = set()

# Define the CSV filename
csv_filename = "proposals_data.csv"

def fetch_proposals(page):
    url = f"{BASE_URL}?page={page}&limit={LIMIT}"
    response = requests.get(url, headers=headers)

    if response.status_code == 429:  # Too many requests
        retry_after = int(response.headers.get("Retry-After", 30))
        print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        return fetch_proposals(page)

    if response.status_code != 200:
        print(f'Error fetching data from API. Status code: {response.status_code}')
        print(f'Response content: {response.text}')
        return None

    try:
        return response.json()
    except ValueError:
        print('Failed to parse JSON response')
        print(f'Response content: {response.text}')
        return None

def write_proposals_to_csv(writer, proposals):
    for proposal in proposals:
        status = proposal.get("status", "")
        state = proposal.get("state", "")
        title = proposal.get("title", "")
        approves = proposal.get("approves", [])
        num_approves = len(approves)

        # Create a unique identifier for the proposal
        unique_id = (status, state, title, num_approves)

        # Skip duplicate proposals
        if unique_id in unique_proposals:
            continue

        # Add to the set of unique proposals
        unique_proposals.add(unique_id)

        # Extract remaining fields
        proposal_type = proposal.get("type", "")
        vote_type = proposal.get("voteType", "")
        cycle = proposal.get("cycle", "")

        # Creator information
        creator = proposal.get("creator", {})
        creator_address = creator.get("address", "")
        creator_username = creator.get("username", "")

        # Currency Information
        currency = proposal.get("currency", {})
        currency_symbol = currency.get("symbol", "")
        currency_address = currency.get("contractAddress", "")

        # Signers information as an array of objects
        signers = proposal.get("singers", [])
        signers_str = str(signers)  # Convert signers list of dicts to a string

        needed_sign = proposal.get("neededSign", 0)

        # Approves information as string
        approves_str = str(approves)

        # Write the proposal data to the CSV
        writer.writerow([
            status, state, title, proposal_type, vote_type, creator_address,
            creator_username, currency_symbol, currency_address, signers_str,
            needed_sign, cycle, approves_str
        ])

def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Y{suffix}"

# Open a CSV file to write
try:
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write CSV header
        writer.writerow([
            "status", "state", "title", "type", "voteType", "creator_address",
            "creator_username", "currency_symbol", "currency_address", "signers",
            "neededSign", "cycle", "approves"
        ])

        # Fetch data from paginated API using threading
        max_workers = 5  # Number of threads to use
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_proposals, page): page for page in range(1, 11)}  # Adjust range as needed

            results = []
            for future in as_completed(futures):
                page = futures[future]
                try:
                    json_response = future.result()
                    if json_response:
                        results.append((page, json_response))
                except Exception as e:
                    print(f'Error fetching page {page}: {e}')

            # Sort results by page number
            results.sort(key=lambda x: x[0])

            for page, json_response in results:
                proposals = json_response.get('data', {}).get('proposals', [])
                if not proposals:
                    print(f'No more proposals found on page {page}.')
                    continue

                write_proposals_to_csv(writer, proposals)
                print(f'Page {page}: {len(proposals)} records fetched')

    print(f'All unique data saved to {csv_filename}')
    print('No more proposals found.')

    # Get the size of the CSV file
    file_size = os.path.getsize(csv_filename)
    readable_size = sizeof_fmt(file_size)
    print(f'Size of the CSV file: {readable_size}')

except IOError:
    print('I/O error while writing to the CSV file')