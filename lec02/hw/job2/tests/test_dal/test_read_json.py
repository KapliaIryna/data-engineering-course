# Tests for dal.read_json module

import pytest
import json
from pathlib import Path
from dal.read_json import read_json_from_raw


def test_read_json_from_raw_reads_file(tmp_path):
    
    # test that read_json_from_raw reads JSON file correctly
   
    # create test JSON file
    date = "2022-08-09"
    raw_dir = tmp_path / "raw"
    test_dir = raw_dir / "sales" / date
    test_dir.mkdir(parents=True)
    
    test_data = [
        {"client": "John", "purchase_date": date, "product": "Phone", "price": 100},
        {"client": "Jane", "purchase_date": date, "product": "Laptop", "price": 200}
    ]
    
    test_file = test_dir / f"sales_{date}.json"
    with open(test_file, 'w') as f:
        json.dump(test_data, f)
    
    result = read_json_from_raw(date, str(raw_dir))
    
    assert len(result) == 2
    assert result[0]['client'] == 'John'
    assert result[1]['client'] == 'Jane'


def test_read_json_raises_error_if_directory_missing(tmp_path):
    
    # test that read_json_from_raw raises FileNotFoundError if directory doesn't exist
    
    date = "2022-08-09"
    raw_dir = tmp_path / "raw"
    # do not create the directory!
    
    with pytest.raises(FileNotFoundError):
        read_json_from_raw(date, str(raw_dir))


def test_read_json_raises_error_if_no_files(tmp_path):
   
    # test that read_json_from_raw raises FileNotFoundError if no JSON files found
    
    # create directory but no files
    date = "2022-08-09"
    raw_dir = tmp_path / "raw"
    test_dir = raw_dir / "sales" / date
    test_dir.mkdir(parents=True)
    
    with pytest.raises(FileNotFoundError):
        read_json_from_raw(date, str(raw_dir))