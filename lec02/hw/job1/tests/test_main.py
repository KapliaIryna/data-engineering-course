"""
Tests for Flask main.py
"""
import pytest
from unittest.mock import patch
import main


@pytest.fixture
def client():
    main.app.config['TESTING'] = True
    with main.app.test_client() as client:
        yield client


def test_missing_date_returns_400(client):
    
    #Test that missing 'date' parameter returns 400
    response = client.post(
        '/',
        json={
            'raw_dir': '/tmp/test'
            # no 'date'
        }
    )
    assert response.status_code == 400
    assert b'date' in response.data


def test_missing_raw_dir_returns_400(client):

    #Test that missing 'raw_dir' parameter returns 400
    response = client.post(
        '/',
        json={
            'date': '2022-08-09'
            # no 'raw_dir'
        }
    )
    assert response.status_code == 400
    assert b'raw_dir' in response.data


@patch('main.save_sales_to_local_disk')
def test_successful_request_returns_201(mock_save, client):
    
    #Test that valid request returns 201
    response = client.post(
        '/',
        json={
            'date': '2022-08-09',
            'raw_dir': '/tmp/test'
        }
    )
    assert response.status_code == 201
    
    # Check that function was called with correct params
    mock_save.assert_called_once_with(
        date='2022-08-09',
        raw_dir='/tmp/test'
    )