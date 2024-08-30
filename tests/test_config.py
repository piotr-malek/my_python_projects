import pytest
import json
from unittest import mock
from trello.config import Config
from trello.errors import ConfigError

def test_config_init():
    config = Config('key', 'token', 'board_id', ['id', 'name'])
    assert config.key == 'key'
    assert config.token == 'token'
    assert config.board_id == 'board_id'
    assert config.fields == ['id', 'name']

def test_config_from_file_valid(temp_config_file):
    config_file_path = temp_config_file()
    config = Config.from_file(config_file_path)
    assert config.key == 'fake_key'
    assert config.token == 'fake_token'
    assert config.board_id == 'fake_board_id'
    assert config.fields == ['id', 'name', 'badges.comments']

def test_config_from_file_file_not_found():
    with pytest.raises(ConfigError, match="Config file not found"):
        Config.from_file('non_existent_file.json')

@pytest.mark.parametrize(
    "data, error_message",
    [
        (
            {'credential': {'access_token': 'fake_token'}, 'boardId': 'fake_board_id'},
            "Missing or empty 'key' or 'access_token' in config file."
        ),
        (
            {'credential': {'key': 'fake_key'}, 'boardId': 'fake_board_id'},
            "Missing or empty 'key' or 'access_token' in config file."
        ),
        (
            {'credential': {'key': 'fake_key', 'access_token': 'fake_token'}},
            "Missing or empty 'boardId' in config file."
        ),
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