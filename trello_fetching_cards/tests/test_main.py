import pytest
import logging
from unittest import mock
from trello.main import main
from trello.errors import FetchError

def test_main_success(monkeypatch, temp_config_file):
    config_file_path = temp_config_file()
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])
    monkeypatch.setattr('builtins.print', lambda x: None)  # Suppress the output
    with mock.patch('trello.main.fetch_cards', return_value=[{'id': 'card_id', 'name': 'Card Name'}]):
        main()  # Expect no exceptions     

def test_main_config_error(monkeypatch):
    monkeypatch.setattr('sys.argv', ['script_name', 'non_existent_file.json'])
    with pytest.raises(SystemExit) as e:
        main()
    assert e.type == SystemExit
    assert e.value.code == 1

def test_main_fetch_error(monkeypatch, temp_config_file):
    config_file_path = temp_config_file()
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    with mock.patch('trello.main.fetch_cards', side_effect=FetchError("Test FetchError")):
        with pytest.raises(SystemExit) as e:
            main()
    assert e.type == SystemExit
    assert e.value.code == 2

def test_main_unexpected_error(monkeypatch, temp_config_file):
    config_file_path = temp_config_file()
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    with mock.patch('trello.main.fetch_cards', side_effect=Exception("Test Exception")):
        with pytest.raises(SystemExit) as e:
            main()
    assert e.type == SystemExit
    assert e.value.code == 3

def test_main_missing_argument(monkeypatch):
    monkeypatch.setattr('sys.argv', ['script_name'])  # No config file provided
    with pytest.raises(SystemExit) as e:
        main()
    assert e.type == SystemExit
    assert e.value.code == 2 

def test_main_empty_config_file(monkeypatch, temp_config_file):
    config_file_path = temp_config_file({'credential': {}})  # Empty JSON structure with missing fields

    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    with pytest.raises(SystemExit) as e:
        main()

    assert e.type == SystemExit
    assert e.value.code == 1  

def test_main_malformed_json(monkeypatch):
    monkeypatch.setattr('sys.argv', ['script_name', 'malformed_config.json'])
    with monkeypatch.context() as m:
        m.setattr('builtins.open', mock.mock_open(read_data='{"invalid_json"'))
        with pytest.raises(SystemExit) as e:
            main()
        assert e.type == SystemExit
        assert e.value.code == 1

def test_main_no_cards(monkeypatch, temp_config_file, caplog):
    config_file_path = temp_config_file()
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    # Mock fetch_cards to return an empty iterator (simulate no cards)
    with mock.patch('trello.main.fetch_cards', return_value=iter([])):
        with caplog.at_level(logging.WARNING):
            main()

    # Check that the warning about no cards was logged
    assert "Board has no cards." in caplog.text

    # Ensure that it does not raise SystemExit
    assert not any(record.levelname == 'ERROR' for record in caplog.records)