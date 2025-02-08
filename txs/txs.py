import aiohttp
import asyncio
import pandas as pd
import os
import json
import math
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('API_KEY')
if not API_KEY:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

BASE_URL = 'https://api.messier.app/api/v1/platform/transaction'
SERVICES = ["horizon", "adastra", "openhatch", "p2p", "virgo"]
PAGE_LIMIT = 100
REQUEST_DELAY = 5
MAX_RETRIES = 5

all_data = []
seen_records = set()  # used for deduping token rows (hash, user, symbol)

async def fetch_page(session, service, page):
    """
    Fetch a page of data from the API for a given service.
    Retries up to MAX_RETRIES upon 429/400 or non-200 statuses.
    Returns (items, total).
    """
    retries = 0
    while retries <= MAX_RETRIES:
        url = f"{BASE_URL}?service={service}&page={page}&limit={PAGE_LIMIT}"
        headers = {
            'accept': 'application/json',
            'X-API-KEY': API_KEY
        }
        async with session.get(url, headers=headers) as response:
            if response.status in [429, 400]:
                await asyncio.sleep(REQUEST_DELAY)
                retries += 1
                continue
            elif response.status != 200:
                retries += 1
                await asyncio.sleep(5)
                continue
            try:
                json_response = await response.json()
                data = json_response.get('data', {})
                items = data.get('items', [])
                raw_total = data.get('total', 0)

                # Convert total to int, handle ValueError if it isn't numeric
                try:
                    total = int(raw_total)
                except ValueError:
                    total = 0

                return items, total
            except ValueError:
                retries += 1
                await asyncio.sleep(5)
                continue

    return [], 0

async def main():
    async with aiohttp.ClientSession() as session:
        for service in SERVICES:
            page = 1

            # Fetch the first page to get 'total' and 'items'
            items, total = await fetch_page(session, service, page)
            if not items:
                print(f"No data for service {service} on page 1. Skipping.")
                continue

            # Convert total to an integer if it’s not already
            max_pages = math.ceil(total / PAGE_LIMIT) if total else 1

            while True:
                new_count = 0

                   # Flatten each item by iterating over 'trxData'
                for item in items:
                    trx_list = item.get("trxData", [])
                    if not trx_list:
                        continue

                    for token_info in trx_list:
                        dedupe_key = (
                            item.get("hash"),
                            item.get("address"),
                            token_info.get("contract"),
                            token_info.get("symbol"),
                        )

                        if dedupe_key in seen_records:
                            continue
                        seen_records.add(dedupe_key)
                        row = {
                            'transaction_type': item.get('type'),
                            'timestamp': item.get('timestamp'),
                            'blockchain': item.get('network'),
                            'service': service,
                            'hash': item.get('hash'),
                            'user': item.get('address'),
                            'token_symbol': token_info.get('symbol'),
                            'token_address': token_info.get('contract'),
                            'value': token_info.get('value')
                        }
                        all_data.append(row)
                        new_count += 1

                print(f"For service {service}, page {page} processed. Found {new_count} new records.")

                # If we've reached the last page or found no new records, stop.
                if page >= max_pages or new_count == 0:
                    break

                page += 1
                items, _ = await fetch_page(session, service, page)
                await asyncio.sleep(REQUEST_DELAY)

    # After all services and pages are done, save results
    if all_data:
        df = pd.DataFrame(all_data)
        csv_filename = "transactions_data.csv"
        json_filename = "transactions_data.json"

        df.to_csv(csv_filename, index=False)
        df.to_json(json_filename, orient="records", indent=4)

        # Upload JSON to jsonBlob
        json_blob_url = "https://jsonblob.com/api/jsonBlob"
        async with aiohttp.ClientSession() as session2:
            with open(json_filename, "r") as json_file:
                json_data = json.load(json_file)
            response = await session2.post(json_blob_url, json=json_data, headers={"Content-Type": "application/json"})

            if response.status in [200, 201]:
                json_blob_link = response.headers.get("Location", "No URL returned")
                print(f"JSON Blob created successfully: {json_blob_link}")
            else:
                print(f"Failed to create JSON Blob: {response.status}")

        print(f"CSV file saved: {csv_filename} (Records: {len(df)})")
        print(f"JSON file saved: {json_filename} (Records: {len(df)})")
    else:
        print("No data fetched.")

if __name__ == '__main__':
    asyncio.run(main())
