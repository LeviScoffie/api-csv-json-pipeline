import aiohttp
import asyncio
import csv
import os
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
# Define delay between requests (in seconds)
REQUEST_DELAY = 5

async def fetch_page(session, page):
    retries = 0
    while retries <= MAX_RETRIES:
        url = f'{BASE_URL}?page={page}&limit={limit}'
        print(f'Fetching page {page}...')
        headers = {
            'accept': 'application/json',
            'X-API-KEY': API_KEY
        }
        async with session.get(url, headers=headers) as response:
            if response.status == 429 or response.status == 400:
                retry_after_message = (await response.json()).get('message', '')
                retry_seconds = 60
                if 'Retry after:' in retry_after_message:
                    retry_seconds = int(retry_after_message.split('Retry after:')[1].split('seconds')[0].strip())
                print(f'Rate limit exceeded. Retrying after {retry_seconds} seconds...')
                await asyncio.sleep(retry_seconds)
                retries += 1
                continue
            elif response.status != 200:
                print(f'Error fetching data from API. Status code: {response.status}')
                print(f'Response content: {await response.text()}')
                retries += 1
                await asyncio.sleep(5)
                continue
            try:
                json_response = await response.json()
                return json_response.get('data', {}).get('users', [])
            except ValueError:
                print('Failed to parse JSON response')
                print(f'Response content: {await response.text()}')
                retries += 1
                await asyncio.sleep(5)
                continue
    print(f'Failed to fetch data for page {page} after {retries} retries.')
    return []

async def main():
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            users = await fetch_page(session, page)
            if not users:
                break
            print(f'Number of records fetched on page {page}: {len(users)}')
            for user_entry in users:
                user_info = user_entry.get('user', {})
                address = user_info.get('address')
                if address in seen_addresses:
                    continue
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
            page += 1
            await asyncio.sleep(REQUEST_DELAY)  # Add delay between requests

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
            file_size = os.path.getsize(csv_file)
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

if __name__ == "__main__":
    asyncio.run(main())