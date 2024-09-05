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
    """Creates a temporary configuration file. Supports empty data if provided."""
    def _create_temp_file(data=None):
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

        # Write the data if provided, otherwise use default valid config data
        if data is not None:
            if data:  # Only dump if data is not empty
                json.dump(data, temp_file)
        else:
            json.dump(valid_config_data, temp_file)

        temp_file.close()
        return temp_file.name

    return _create_temp_file

@pytest.fixture
def config():
    """Provides a simple Config object with default test data."""
    return Config(key='fake_key', token='fake_token', board_id='fake_board_id')