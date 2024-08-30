import json
from typing import List, Optional
from trello.errors import ConfigError

class Config:
    def __init__(self, key: str, token: str, board_id: str, fields: Optional[List[str]] = None):
        """Holds the configuration data required to access the Trello API."""
        self.key = key
        self.token = token
        self.board_id = board_id
        self.fields = fields

    @staticmethod
    def from_file(config_path: str) -> 'Config':
        """Loads the configuration from a JSON file."""
        try:
            with open(config_path) as config_file:
                config_data = json.load(config_file)

            # Ensure all required keys are present and not empty
            if not config_data:
                raise ConfigError("Config file is empty")

            if 'credential' not in config_data:
                raise ConfigError("Missing 'credential' section in config file.")
            
            credentials = config_data['credential']
            
            if not credentials.get('key') or not credentials.get('access_token'):
                raise ConfigError("Missing or empty 'key' or 'access_token' in config file.")
            
            if not config_data.get('boardId'):
                raise ConfigError("Missing or empty 'boardId' in config file.")

            return Config(
                key=credentials['key'],
                token=credentials['access_token'],
                board_id=config_data['boardId'],
                fields=config_data.get('fields')
            )

        except FileNotFoundError:
            raise ConfigError("Config file not found")
        except json.JSONDecodeError:
            raise ConfigError("Config file is not valid JSON")