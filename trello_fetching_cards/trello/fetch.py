import requests
from typing import Any  
from trello.errors import FetchError 
from trello.config import Config 

def fetch_cards(config: Config) -> Any:
    """Fetch trello cards page by page and yield results."""
    url = f'https://api.trello.com/1/boards/{config.board_id}/cards'
    params = {'key': config.key, 'token': config.token, 'limit': 1000}
    before = None

    while True:
        if before:
            params['before'] = before
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            cards = response.json()
            
            if not cards:
                break  # No more cards to fetch
            
            before = cards[-1]['id']  # Update `before` with the last card's ID
            
            yield cards  # Yield the current batch of cards
            
        except requests.exceptions.HTTPError:
            raise FetchError(f"HTTP error occurred: {response.status_code}")
        except requests.exceptions.RequestException as e:
            raise FetchError(f"Request error occurred: {e}")