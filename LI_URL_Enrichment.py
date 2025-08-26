import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import deque
from tqdm import tqdm
import csv

API_KEY = 'xxxxxxxx'
ENRICH_API_URL = 'https://app.cognism.com/api/search/contact/enrich'
REDEEM_API_URL = "https://app.cognism.com/api/search/contact/redeem"

csv_file = "/Users/seanparker/Documents/Atheneum/linkedin-urls.csv"
output_file = "/Users/seanparker/Documents/Atheneum/linkedin-urls_enriched.csv"
df = pd.read_csv(csv_file)

HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json',
}

MAX_REQUESTS_PER_MIN = 250
request_times = deque()
request_lock = Lock()

def rate_limit():
    with request_lock:
        now = time.time()
        request_times.append(now)
        while request_times and now - request_times[0] > 60:
            request_times.popleft()
        if len(request_times) >= MAX_REQUESTS_PER_MIN:
            sleep_time = 60 - (now - request_times[0])
            if sleep_time > 0:
                print(f"⏳ Rate limit reached. Sleeping {sleep_time:.2f}s...")
                time.sleep(sleep_time)

def enrich_linkedinurl(linkedinurl):
    rate_limit()
    try:
        response = requests.post(ENRICH_API_URL, json={'linkedinUrl': linkedinurl}, headers=HEADERS)
        if response.status_code == 200:
            json_resp = response.json()
            if json_resp.get('results'):
                enrich_result = json_resp['results'][0]
                redeemId = enrich_result.get('redeemId')
                return enrich_result, redeemId
        else:
            print(f"⚠️ Enrich API error {response.status_code} for {linkedinurl}")
        return None, None
    except Exception as e:
        print(f"❌ Exception during enrich for {linkedinurl}: {e}")
        return None, None

def redeem_batch(redeem_ids):
    rate_limit()
    try:
        response = requests.post(REDEEM_API_URL, json={'redeemIds': redeem_ids}, headers=HEADERS)
        if response.status_code == 200:
            return response.json().get('results', [])
        else:
            print(f"⚠️ Redeem API error {response.status_code} for batch")
            return []
    except Exception as e:
        print(f"❌ Exception during redeem batch: {e}")
        return []

def process_row(row):
    enriched, redeemId = enrich_linkedinurl(row['linkedinurl'])
    if enriched is None:
        return None
    return {
        "row": row.to_dict(),
        "enriched": enriched,
        "redeemId": redeemId
    }

# --- Flatten with list expansion ---
def flatten_dict(d, parent_key='', sep='_'):
    """Recursively flattens a nested dict and expands lists into numbered keys."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        elif isinstance(v, list):
            for i, elem in enumerate(v):
                if isinstance(elem, dict):
                    items.extend(flatten_dict(elem, f"{new_key}{sep}{i}", sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", elem))
        else:
            items.append((new_key, v))
    return dict(items)

def main():
    enrich_results = []
    final_data = []

    print("🔄 Starting enrichment...")
    MAX_THREADS = 20
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
        for future in tqdm(as_completed(futures), total=len(df), desc="Enrichment"):
            result = future.result()
            if result:
                enrich_results.append(result)

    redeem_map = {item['redeemId']: item for item in enrich_results if item['redeemId']}
    redeem_ids = list(redeem_map.keys())
    BATCH_SIZE = 20

    print("🔄 Starting batch redemption...")
    for i in tqdm(range(0, len(redeem_ids), BATCH_SIZE), desc="Redemption"):
        batch = redeem_ids[i:i+BATCH_SIZE]
        redeemed_batch = redeem_batch(batch)
        for redeemed in redeemed_batch:
            rid = redeemed.get('redeemId')
            if rid in redeem_map:
                redeem_map[rid]['redeemed'] = redeemed

    print("🔄 Merging and flattening results...")
    for item in enrich_results:
        combined = {**item['row'], **item['enriched']}
        if 'redeemed' in item:
            combined.update(item['redeemed'])
        flattened = flatten_dict(combined)
        final_data.append(flattened)

    print(f"💾 Saving results to {output_file} ...")
    pd.DataFrame(final_data).to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)

    total = len(df)
    success = sum(1 for d in final_data if 'redeemId' in d and d['redeemId'])
    print(f"\n✅ Done! Processed: {total}, Successful enrichments: {success}, Success rate: {success/total*100:.2f}%")

if __name__ == "__main__":
    main()
