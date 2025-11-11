
# Module for reading JSON files from raw directory

from typing import List, Dict, Any
import json
from pathlib import Path


def read_json_from_raw(date: str, raw_dir: str) -> List[Dict[str, Any]]:
    
    source_dir = Path(raw_dir) / "sales" / date
    
    if not source_dir.exists():
        raise FileNotFoundError(f"Directory {source_dir} does not exist")
    
    all_data = []
    
    json_files = list(source_dir.glob("sales_*.json"))
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {source_dir}")
    
    print(f"Found {len(json_files)} JSON file(s) in {source_dir}")
    
    
    for json_file in json_files:
        print(f"  Reading {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            if isinstance(data, list):
                all_data.extend(data)
            
            else:
                all_data.append(data)
    
    print(f"Total records read: {len(all_data)}")
    return all_data