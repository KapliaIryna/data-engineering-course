# Tests for Flask main.py

import pytest
from unittest.mock import patch

import main


@pytest.fixture
def client():
    main.app.config['TESTING'] = True
    with main.app.test_client() as client:
        yield client


def test_missing_date_returns_400(client):

    # test that missing 'date' returns 400

    response = client.post(
        '/',
        json={
            'raw_dir': '/tmp/raw',
            'stg_dir': '/tmp/stg'
            # no 'date'
        }
    )
    assert response.status_code == 400
    assert b'date' in response.data


def test_missing_raw_dir_returns_400(client):
    
    # test that missing 'raw_dir' returns 400
    
    response = client.post(
        '/',
        json={
            'date': '2022-08-09',
            'stg_dir': '/tmp/stg'
            # no 'raw_dir'
        }
    )
    assert response.status_code == 400
    assert b'raw_dir' in response.data


def test_missing_stg_dir_returns_400(client):
    
    # test that missing 'stg_dir' returns 400
    
    response = client.post(
        '/',
        json={
            'date': '2022-08-09',
            'raw_dir': '/tmp/raw'
            # no 'stg_dir'
        }
    )
    assert response.status_code == 400
    assert b'stg_dir' in response.data


@patch('main.convert_sales_to_avro')
def test_successful_request_returns_201(mock_convert, client):
   
    # test that valid request returns 201
   
    # returns record count
    mock_convert.return_value = 100
    
    response = client.post(
        '/',
        json={
            'date': '2022-08-09',
            'raw_dir': '/tmp/raw',
            'stg_dir': '/tmp/stg'
        }
    )
    
    assert response.status_code == 201
    
    # check that function had correct params
    mock_convert.assert_called_once_with(
        date='2022-08-09',
        raw_dir='/tmp/raw',
        stg_dir='/tmp/stg'
    )