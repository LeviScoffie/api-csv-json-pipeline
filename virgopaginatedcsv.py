import requests
import csv
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment variable
API_KEY = os.getenv('API_KEY')

# Check if the API_KEY was loaded
if not API_KEY:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

BASE_URL = 'https://api.messier.app/api/v1/virgo/permissions'

# Initialize variables
page = 1
limit = 100  # Adjust if the API allows higher limits
all_data = []
seen_addresses = set()

# Define maximum number of retries
MAX_RETRIES = 5

while True:
    retries = 0
    success = False

    while not success and retries <= MAX_RETRIES:
        # Construct the API URL with pagination parameters
        url = f'{BASE_URL}?page={page}&limit={limit}'
        print(f'Fetching page {page}...')

        # Set up headers with API key
        headers = {
            'accept': 'application/json',
            'X-API-KEY': API_KEY
        }

        # Make the API request
        response = requests.get(url, headers=headers)

        # Check for rate limiting
        if response.status_code == 429:
            # Handle rate limit response
            try:
                response_json = response.json()
                retry_after_message = response_json.get('message', '')
                retry_seconds = 60  # Default retry after time

                # Extract the number of seconds from the message
                if 'Retry after:' in retry_after_message:
                    retry_seconds = int(retry_after_message.split('Retry after:')[1].split('seconds')[0].strip())

                print(f'Rate limit exceeded. Retrying after {retry_seconds} seconds...')
                time.sleep(retry_seconds)
                retries += 1
                continue
            except Exception as e:
                print(f'Error parsing rate limit response: {e}')
                retries += 1
                time.sleep(5)
                continue

        # Check for unsuccessful response
        elif response.status_code != 200:
            print(f'Error fetching data from API. Status code: {response.status_code}')
            print(f'Response content: {response.text}')
            retries += 1
            time.sleep(5)
            continue

        # Parse the JSON response
        try:
            json_response = response.json()
        except ValueError:
            print('Failed to parse JSON response')
            print(f'Response content: {response.text}')
            retries += 1
            time.sleep(5)
            continue

        # Extract the permissions data
        users = json_response.get('data', {}).get('users', [])

        # Print the number of records fetched
        print(f'Number of records fetched on page {page}: {len(users)}')

        # Break the loop if no more data
        if not users:
            print('No more users found.')
            success = True
            break

        # Extract required fields and append to all_data list
        for user_entry in users:
            user_info = user_entry.get('user', {})
            address = user_info.get('address')

            # Check if the address has already been seen
            if address in seen_addresses:
                continue  # Skip duplicate

            seen_addresses.add(address)
            all_data.append({
                'username': user_info.get('username'),
                'isactive': user_info.get('active'),
                'address': address,
                'type': user_entry.get('type'),
                'istypeActive': user_entry.get('active'),
                'isDarklist': user_entry.get('darkList'),
                'isActiveDarklist': user_entry.get('activeDarkList'),
                'stakeAmount': user_entry.get('stakeAmount')
            })

        # Set success to True after successful data retrieval
        success = True

        # Add a small delay between successful requests
        time.sleep(1)

    if not success:
        print(f'Failed to fetch data for page {page} after {retries} retries.')
        break  # Exit the main loop if the page could not be fetched after retries

    # Break the main loop if no more users
    if not users:
        break

    # Increment page number for next iteration
    page += 1

# Save the data into a CSV file if there's any data
if all_data:
    csv_file = 'virgo_users_data.csv'
    csv_columns = ['username', 'isactive', 'address', 'type', 'istypeActive', 'isDarklist', 'isActiveDarklist', 'stakeAmount']

    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in all_data:
                writer.writerow(data)
        print(f'Data successfully saved to {csv_file}')

        # Get the size of the CSV file
        file_size = os.path.getsize(csv_file)

        # Convert the file size to a more readable format (e.g., KB, MB)
        def sizeof_fmt(num, suffix='B'):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return f"{num:3.1f} {unit}{suffix}"
                num /= 1024.0
            return f"{num:.1f} Y{suffix}"

        readable_size = sizeof_fmt(file_size)
        print(f'Size of the CSV file: {readable_size}')
        print(f'Number of records saved: {len(all_data)}')
    except IOError:
        print('I/O error while writing to CSV file')
else:
    print('No data to save.')

