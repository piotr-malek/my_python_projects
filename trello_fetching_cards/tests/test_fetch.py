import responses
import pytest
import requests
from unittest import mock
from trello.fetch import fetch_cards
from trello.config import Config
from trello.errors import FetchError

@responses.activate
def test_fetch_cards_success(config):
    url = f"https://api.trello.com/1/boards/{config.board_id}/cards"

    # Mock the first batch of cards
    responses.add(
        responses.GET,
        f"{url}?key={config.key}&token={config.token}&limit=1000",
        json=[{'id': '1', 'name': 'First Card'}],
        status=200
    )
    # Mock the second batch of cards, indicating no more cards to fetch
    responses.add(
        responses.GET,
        f"{url}?key={config.key}&token={config.token}&limit=1000&before=1",
        json=[],
        status=200
    )

    # Execute the fetch_cards function and collect results from generator
    fetched_cards = list(fetch_cards(config))

    assert len(fetched_cards) == 1 # Check that we fetched exactly one batch of cards
    assert len(fetched_cards[0]) == 1 # Check that the batch contains exactly one card
    # Validate the content of the fetched card
    assert fetched_cards[0][0]['id'] == '1'
    assert fetched_cards[0][0]['name'] == 'First Card'

@responses.activate
def test_fetch_cards_http_error(temp_config_file):
    url = "https://api.trello.com/1/boards/fake_board_id/cards"
    
    # Ensure the URL matches exactly what is being requested
    responses.add(responses.GET, url, status=404)
    
    config_file_path = temp_config_file()
    config = Config.from_file(config_file_path)
    
    cards = fetch_cards(config)
    with pytest.raises(FetchError, match="HTTP error occurred: 404"):
        list(cards)  # Trigger the generator to raise the exception

def test_fetch_cards_request_exception(temp_config_file):
    config_file_path = temp_config_file()
    config = Config.from_file(config_file_path)

    with mock.patch('requests.get', side_effect=requests.exceptions.RequestException('Test Exception')):
        cards = fetch_cards(config)
        with pytest.raises(FetchError, match="Request error occurred: Test Exception"):
            list(cards)  # Trigger the generator to raise the exception