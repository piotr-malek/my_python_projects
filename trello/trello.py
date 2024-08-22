import requests
import json
import sys
import argparse
import logging
from typing import List, Dict, Optional, Any

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConfigError(Exception):
    """Raised when there is an error with the configuration."""
    pass

class FetchError(Exception):
    """Raised when there is an error fetching data from the Trello API."""
    pass

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
            key = config_data['credential']['key']
            token = config_data['credential']['access_token']
            board_id = config_data['boardId']
            fields = config_data.get('fields', None)
            return Config(key, token, board_id, fields)
        except FileNotFoundError:
            raise ConfigError("Config file not found")
        except json.JSONDecodeError:
            raise ConfigError("Config file is not valid JSON")
        except KeyError as e:
            raise ConfigError(f"Missing key in config file: {e}")

def fetch_cards(config: Config) -> List[Dict[str, Any]]:
    """Fetches cards from the Trello board specified in the configuration."""
    url = f'https://api.trello.com/1/boards/{config.board_id}/cards'
    params = {'key': config.key, 'token': config.token}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError:
        raise FetchError(f"HTTP error occurred: {response.status_code}")
    except requests.exceptions.RequestException as e:
        raise FetchError(f"Request error occurred: {e}")

def extract_nested_field(record: Dict[str, Any], field: str) -> Optional[Any]:
    """Extracts a nested field value from a dictionary"""
    nested_fields = field.split('.')
    value: Any = record
    try:
        for nested_field in nested_fields:
            value = value[nested_field]
            if value is None:
                return None
    except (KeyError, TypeError):
        return None
    return value

def process_and_print_cards(cards: List[Dict[str, Any]], fields: Optional[List[str]]) -> None:
    """Processes and prints the fetched Trello cards according to specified fields."""
    if len(cards) == 0:
        raise FetchError("No data found")

    for card in cards:
        if fields:
            output = {}
            for field in fields:
                value = extract_nested_field(card, field)
                if value is not None:
                    output[field] = value
        else:
            output = card
        print(json.dumps(output))

def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments to get the path to the configuration file."""
    parser = argparse.ArgumentParser(description='Fetch Trello cards from a board')
    parser.add_argument('config_path', metavar='config_path', type=str,
                        help='Path to the config.json file')
    return parser.parse_args()

def main() -> None:
    """Main function that orchestrates the fetching and processing of Trello cards."""
    try:
        args = parse_arguments()
        config = Config.from_file(args.config_path)
        cards = fetch_cards(config)
        process_and_print_cards(cards, config.fields)

    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except FetchError as e:
        logger.error(f"Failed to fetch Trello cards: {e}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(3)

    """
    Exit Codes:
    1 - Configuration error
    2 - Fetching from Trello API error
    3 - Unexpected error
    """

if __name__ == "__main__":
    main()
