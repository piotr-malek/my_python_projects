import requests
from typing import Iterator, Dict  
from trello.errors import FetchError 
from trello.config import Config 

def fetch_cards(config: Config) -> Iterator[Dict]:
    """Fetch trello cards one by one from all available pages."""
    url = f'https://api.trello.com/1/boards/{config.board_id}/cards'
    # Fetch 1000 cards at the time to limit the number of API requests
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
            
            for card in cards:
                yield card
            
        except requests.exceptions.HTTPError:
            raise FetchError(f"HTTP error occurred: {response.status_code}")
        except requests.exceptions.RequestException as e:
            raise FetchError(f"Request error occurred: {e}")