import pytest
import responses
import requests
import tempfile
import json
from unittest import mock
from trello import Config, ConfigError, FetchError, fetch_cards, extract_nested_field, process_and_print_cards, main, parse_arguments

@pytest.fixture
def valid_config_data():
    return {
        'credential': {'key': 'fake_key', 'access_token': 'fake_token'},
        'boardId': 'fake_board_id',
        'fields': ['id', 'name', 'badges.comments']
    }

@pytest.fixture
def temp_config_file():
    def _create_temp_file(data):
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        json.dump(data, temp_file)
        temp_file.close()
        return temp_file.name
    return _create_temp_file

# Config class tests
def test_config_init():
    config = Config('key', 'token', 'board_id', ['id', 'name'])
    assert config.key == 'key'
    assert config.token == 'token'
    assert config.board_id == 'board_id'
    assert config.fields == ['id', 'name']

def test_config_from_file_valid(temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    config = Config.from_file(config_file_path)
    assert config.key == valid_config_data['credential']['key']
    assert config.token == valid_config_data['credential']['access_token']
    assert config.board_id == valid_config_data['boardId']
    assert config.fields == valid_config_data['fields']

def test_config_from_file_file_not_found():
    with pytest.raises(ConfigError, match="Config file not found"):
        Config.from_file('non_existent_file.json')

@pytest.mark.parametrize(
    "data, error_message",
    [
        ({'credential': {'access_token': 'fake_token'}, 'boardId': 'fake_board_id'}, "Missing key in config file: 'key'"),
        ({'credential': {'key': 'fake_key'}, 'boardId': 'fake_board_id'}, "Missing key in config file: 'access_token'"),
        ({'credential': {'key': 'fake_key', 'access_token': 'fake_token'}}, "Missing key in config file: 'boardId'")
    ]
)
def test_config_from_file_missing_keys(temp_config_file, data, error_message):
    config_file_path = temp_config_file(data)
    with pytest.raises(ConfigError, match=error_message):
        Config.from_file(config_file_path)

def test_config_from_file_json_decode_error(temp_config_file):
    invalid_json = '{"invalid json"'  
    config_file_path = temp_config_file(invalid_json)
    
    with mock.patch('json.load', side_effect=json.JSONDecodeError("Expecting value", "doc", 0)):
        with pytest.raises(ConfigError, match="Config file is not valid JSON"):
            Config.from_file(config_file_path)

# fetch_cards tests
@responses.activate
def test_fetch_cards_success(temp_config_file, valid_config_data):
    url = f"https://api.trello.com/1/boards/{valid_config_data['boardId']}/cards"
    responses.add(responses.GET, url, json=[{'id': 'fake_card_id', 'name': 'Test Card'}], status=200)

    config_file_path = temp_config_file(valid_config_data)
    config = Config.from_file(config_file_path)
    cards = fetch_cards(config)
    assert len(cards) == 1
    assert cards[0]['id'] == 'fake_card_id'

@responses.activate
def test_fetch_cards_http_error(temp_config_file, valid_config_data):
    url = f"https://api.trello.com/1/boards/{valid_config_data['boardId']}/cards"
    responses.add(responses.GET, url, status=404)

    config_file_path = temp_config_file(valid_config_data)
    config = Config.from_file(config_file_path)
    with pytest.raises(FetchError, match="HTTP error occurred: 404"):
        fetch_cards(config)

@responses.activate
def test_fetch_cards_request_exception(temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    config = Config.from_file(config_file_path)

    with mock.patch('requests.get', side_effect=requests.exceptions.RequestException('Test Exception')):
        with pytest.raises(FetchError, match="Request error occurred: Test Exception"):
            fetch_cards(config)

# extract_nested_field tests
@pytest.mark.parametrize(
    "record, field, expected_result",
    [
        # Successful case
        ({'a': {'b': {'c': 10}}}, 'a.b.c', 10),

        # Non-existent field
        ({'a': {'b': {}}}, 'a.b.c', None),

        # Type error
        ({'a': 10}, 'a.b.c', None),

        # Empty dictionary
        ({}, 'a.b.c', None),

        # Empty nested field
        ({'a': {'b': {}}}, 'a.b', {}),

        # Partial path
        ({'a': {'b': {'c': 10, 'd': 20}}}, 'a.b', {'c': 10, 'd': 20}),

        # Handling non-dictionary at nested level
        ({'a': {'b': 'string_value'}}, 'a.b.c', None),
    ]
)
def test_extract_nested_field(record, field, expected_result):
    assert extract_nested_field(record, field) == expected_result

# process_and_print_cards tests
def test_process_and_print_cards_no_data():
    with pytest.raises(FetchError, match="No data found"):
        process_and_print_cards([], ['id', 'name'])

def test_process_and_print_cards_with_fields(capfd):
    cards = [{'id': 'card_id', 'name': 'Card Name', 'badges': {'comments': 2}}]
    process_and_print_cards(cards, ['id', 'name', 'badges.comments'])

    captured = capfd.readouterr()
    expected_output = '{"id": "card_id", "name": "Card Name", "badges.comments": 2}\n'
    assert captured.out == expected_output

def test_process_and_print_cards_without_fields(capfd):
    cards = [{'id': 'card_id', 'name': 'Card Name', 'badges': {'comments': 2}}]
    process_and_print_cards(cards, None)

    captured = capfd.readouterr()
    expected_output = '{"id": "card_id", "name": "Card Name", "badges": {"comments": 2}}\n'
    assert captured.out == expected_output

# parse_arguments tests
def test_parse_arguments(monkeypatch):
    test_args = ['script_name', 'config.json']
    monkeypatch.setattr('sys.argv', test_args)
    args = parse_arguments()
    assert args.config_path == 'config.json'

# main function tests
def test_main_success(monkeypatch, temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])
    monkeypatch.setattr('builtins.print', lambda x: None)  # Suppress the output
    with mock.patch('trello.fetch_cards', return_value=[{'id': 'card_id', 'name': 'Card Name'}]):
        main()  # Expect no exceptions

def test_main_config_error(monkeypatch):
    monkeypatch.setattr('sys.argv', ['script_name', 'non_existent_file.json'])
    with pytest.raises(SystemExit) as e:
        main()
    assert e.type == SystemExit
    assert e.value.code == 1

def test_main_fetch_error(monkeypatch, temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    with mock.patch('trello.fetch_cards', side_effect=FetchError("Test FetchError")):
        with pytest.raises(SystemExit) as e:
            main()
    assert e.type == SystemExit
    assert e.value.code == 2

def test_main_unexpected_error(monkeypatch, temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])

    with mock.patch('trello.fetch_cards', side_effect=Exception("Test Exception")):
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
    config_file_path = temp_config_file({})  # Empty configuration
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

def test_main_no_cards(monkeypatch, temp_config_file, valid_config_data):
    config_file_path = temp_config_file(valid_config_data)
    monkeypatch.setattr('sys.argv', ['script_name', config_file_path])
    with mock.patch('trello.fetch_cards', return_value=[]):  # No cards
        with pytest.raises(SystemExit) as e:
            main()
        assert e.type == SystemExit
        assert e.value.code == 2


