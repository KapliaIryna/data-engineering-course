#Tests for dal.write_avro module

import pytest
from pathlib import Path
from fastavro import reader
from dal.write_avro import save_to_avro


def test_save_to_avro_creates_file(tmp_path):
    
    #Test that save_to_avro creates an Avro file

    test_data = [
        {
            "client": "John Doe",
            "purchase_date": "2022-08-09",
            "product": "Laptop",
            "price": 1000
        }
    ]
    date = "2022-08-09"
    stg_dir = tmp_path / "stg"
    
    save_to_avro(test_data, date, str(stg_dir))
    
    # assert - file exists
    expected_file = stg_dir / "sales" / date / f"sales_{date}.avro"
    assert expected_file.exists()
    
    # assert - verify avro content
    with open(expected_file, 'rb') as f:
        avro_reader = reader(f)
        records = list(avro_reader)
    
    assert len(records) == 1
    assert records[0]['client'] == 'John Doe'
    assert records[0]['price'] == 1000


def test_save_to_avro_is_idempotent(tmp_path):
    
    # Test that save_to_avro overwrites existing data
    
    date = "2022-08-09"
    stg_dir = tmp_path / "stg"
    
    data1 = [{"client": "John", "purchase_date": date, "product": "Phone", "price": 100}]
    data2 = [{"client": "Jane", "purchase_date": date, "product": "Laptop", "price": 200}]
    
    # save twice
    save_to_avro(data1, date, str(stg_dir))
    save_to_avro(data2, date, str(stg_dir))
    
    # assert - only second data remains
    expected_file = stg_dir / "sales" / date / f"sales_{date}.avro"
    with open(expected_file, 'rb') as f:
        avro_reader = reader(f)
        records = list(avro_reader)
    
    assert len(records) == 1
    assert records[0]['client'] == 'Jane'
    assert records[0]['price'] == 200