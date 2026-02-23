import requests
import json
import prettytable
import time
import os
from dotenv import load_dotenv

# 1. Load Environment Variables (Ensure you have a .env file with BLS_API_KEY)
load_dotenv()
API_KEY = os.getenv('BLS_API_KEY')

def fetch_bls_data(series_ids, start_year, end_year):
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    headers = {'Content-type': 'application/json'}
    
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": API_KEY
    }

    # Retry Logic with Exponential Backoff
    max_retries = 5

    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            
            # Triggers the 'except' block if status is 4xx or 5xx
            response.raise_for_status()
            
            # If successful, return the data immediately
            return response.json()

        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
            print(f"Current wait time: {wait_time}s")
            print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
            
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                print("Max retries reached. Skipping this batch.")
                return None

def process_results(json_data):
    if not json_data or json_data.get('status') != 'REQUEST_SUCCEEDED':
        print(f"API Error: {json_data.get('message')}")
        return

    for series in json_data['Results']['series']:
        table = prettytable.PrettyTable(["series id", "year", "period", "value", "footnotes"])
        seriesId = series['seriesID']
        
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            # Cleaning up footnotes
            footnotes = ",".join([f['text'] for f in item['footnotes'] if f.get('text')])
            
            if 'M01' <= period <= 'M12':
                table.add_row([seriesId, year, period, value, footnotes])

        # Respecting Rate Limits: Brief pause between file writes/processing
        time.sleep(0.5) 
        
        with open(f"{seriesId}.txt", 'w') as output:
            output.write(table.get_string())
            print(f"Saved: {seriesId}.txt")

# Main Execution
if __name__ == "__main__":
    # Pagination Note: BLS limits to 20 years per request. 
    # If your range is larger, you'd loop through year chunks here.
    data = fetch_bls_data(['CUUR0000SA0', 'SUUR0000SA0'], 2011, 2014)

    process_results(data)
