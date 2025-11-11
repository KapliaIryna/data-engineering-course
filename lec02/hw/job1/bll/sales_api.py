import os
import requests
from typing import List, Dict, Any
from dal.local_disk import save_to_disk


API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'

# 1. get data from the API

def get_sales(date: str) -> List[Dict[str, Any]]:
    
    auth_token = os.environ.get("API_AUTH_TOKEN")
    if not auth_token:
        raise ValueError("AUTH_TOKEN environment variable must be set")
    
    print(f"Fetching sales data for {date}...")
    all_sales = []
    page = 1
    
    while True:
        print(f"  Fetching page {page}...")
        
        url = f"{API_URL}sales"
        params = {'date': date, 'page': page}
        headers = {'Authorization': auth_token}
        
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 404:
            print(f"  No more data. Total pages: {page - 1}")
            break
        
        if response.status_code != 200:
            raise Exception(
                f"API request failed with status {response.status_code}: {response.text}"
            )
        
        data = response.json()
        
        if not data:
            print(f"  No more data. Total pages: {page - 1}")
            break
        
        all_sales.extend(data)
        page += 1
    
    print(f"Total records fetched: {len(all_sales)}")
    return all_sales

# 2. save data to disk

def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    
    print(f"\n--- Starting job for date: {date} ---")
    
    # get data
    sales_data = get_sales(date)
    
    # make path
    full_path = f"{raw_dir}/sales/{date}"
    
    # save
    save_to_disk(sales_data, full_path)
    
    print(f"--- Job completed successfully ---\n")