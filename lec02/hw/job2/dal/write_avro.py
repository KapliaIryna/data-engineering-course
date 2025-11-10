# module for writing data to Avro

from typing import List, Dict, Any
from pathlib import Path
import shutil
from fastavro import writer, parse_schema

# schema for sales data:
AVRO_SCHEMA = {
    "type": "record",
    "name": "Sale",
    "namespace": "sales",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "int"}
    ]
}

def save_to_avro(data: List[Dict[str, Any]], date: str, stg_dir: str) -> None:
    
    target_dir = Path(stg_dir) / "sales" / date
    
    # clean dir:
    if target_dir.exists():
        print(f"Removing existing directory: {target_dir}")
        shutil.rmtree(target_dir)
    
    # create dir:
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created directory: {target_dir}")
    
    filename = f"sales_{date}.avro"
    filepath = target_dir / filename
    
    parsed_schema = parse_schema(AVRO_SCHEMA)
    
    # save to avro:
    with open(filepath, 'wb') as out: 
        writer(out, parsed_schema, data)
    
    print(f"Saved {len(data)} records to {filepath}")