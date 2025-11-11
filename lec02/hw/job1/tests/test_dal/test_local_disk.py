"""
Tests dal.local_disk.py module
"""
import json
import pytest
from pathlib import Path
from dal.local_disk import save_to_disk


def test_save_to_disk_creates_file(tmp_path):
   
    #Test that save_to_disk creates a JSON file
  
    test_data = [
        {"client": "John", "product": "Laptop", "price": 1000}
    ]
    date = "2022-08-09"
    test_dir = tmp_path / "raw" / "sales" / date
    
    save_to_disk(test_data, str(test_dir))
    
    # assert
    expected_file = test_dir / f"sales_{date}.json"
    assert expected_file.exists()
    
    # Check content
    with open(expected_file, 'r') as f:
        saved_data = json.load(f)
    assert saved_data == test_data

def test_save_to_disk_overwrites_existing(tmp_path):
   
    #Test that save_to_disk is idempotent (overwrites existing data)
   
    date = "2022-08-09"
    test_dir = tmp_path / "raw" / "sales" / date
    
    data1 = [{"client": "John", "price": 100}]
    data2 = [{"client": "Jane", "price": 200}]
    
    save_to_disk(data1, str(test_dir))
    save_to_disk(data2, str(test_dir))
    
    # assert - only second data should remain
    expected_file = test_dir / f"sales_{date}.json"
    with open(expected_file, 'r') as f:
        saved_data = json.load(f)
    assert saved_data == data2
    assert len(saved_data) == 1
