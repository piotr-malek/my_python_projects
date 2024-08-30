import pytest
import json
import tempfile
from trello.config import Config

@pytest.fixture
def valid_config_data():
    return {
        'credential': {'key': 'fake_key', 'access_token': 'fake_token'},
        'boardId': 'fake_board_id',
        'fields': ['id', 'name', 'badges.comments']
    }

@pytest.fixture
def temp_config_file(valid_config_data):
    """Creates a temporary configuration file from valid_config_data."""
    def _create_temp_file(data=None):
        # Use default valid_config_data if no data is provided
        data_to_write = data if data else valid_config_data
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        json.dump(data_to_write, temp_file)
        temp_file.close()
        return temp_file.name
    return _create_temp_file

@pytest.fixture
def config():
    """Provides a simple Config object with default test data."""
    return Config(key='fake_key', token='fake_token', board_id='fake_board_id')