from typing import Any, Optional, Dict, List
import json
import argparse
from trello.errors import FetchError

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
    cards_list = list(cards)  # Convert generator to list to check if it's empty

    if not cards_list:  # If no cards were fetched
        raise FetchError("No data found")

    for card in cards_list:
        if fields:
            output = {}
            for field in fields:
                value = extract_nested_field(card, field)
                output[field] = value  # Include the field even if the value is None
        else:
            output = card
        print(json.dumps(output))

def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments to get the path to the configuration file."""
    parser = argparse.ArgumentParser(description='Fetch Trello cards from a board')
    parser.add_argument('config_path', metavar='config_path', type=str,
                        help='Path to the config.json file')
    return parser.parse_args()