from dal.read_json import read_json_from_raw
from dal.write_avro import save_to_avro


def convert_sales_to_avro(date: str, raw_dir: str, stg_dir: str) -> int:
   
    print(f"\n--- Starting conversion for date: {date} ---")
    
    # read JSON from raw/
    print(f"Reading JSON data from {raw_dir}...")
    sales_data = read_json_from_raw(date=date, raw_dir=raw_dir)
    
    # save to Avro to /stg
    print(f"Converting to Avro and saving to {stg_dir}...")
    save_to_avro(data=sales_data, date=date, stg_dir=stg_dir)
    
    print(f"--- Conversion completed successfully ---\n")
    
    return len(sales_data)