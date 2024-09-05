from typing import List, Optional
import json
from trello.errors import ConfigError

class Config:
    def __init__(self, key: str, token: str, board_id: str, fields: Optional[List[str]] = None):
        """Holds the configuration data required to access the Trello API."""
        self.key = key
        self.token = token
        self.board_id = board_id
        self.fields = fields

    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Loads the configuration from a JSON file."""
        try:
            with open(config_path) as config_file:
                config_data = json.load(config_file)

            # Check if file with config data isn't empty
            if not config_data:
                raise ConfigError("Config file is empty")

            try:
                credentials = config_data['credential']
                key = credentials['key']
                token = credentials['access_token']
                board_id = config_data['boardId']
            except KeyError as e:
                raise ConfigError(f"Missing key in config file: {e}")

            # Check if any of the required keys are missing
            if not key or not token or not board_id:
                raise ConfigError("Missing or empty 'key', 'access_token', or 'boardId' in config file.")

            return cls(
                key=key,
                token=token,
                board_id=board_id,
                fields=config_data.get('fields')
            )

        except FileNotFoundError:
            raise ConfigError("Config file not found")
        except json.JSONDecodeError:
            raise ConfigError("Config file is not valid JSON")