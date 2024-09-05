import pytest
from trello.utils import extract_nested_field, process_and_print_cards, parse_arguments
from trello.errors import FetchError

# extract_nested_field tests
@pytest.mark.parametrize(
    "record, field, expected_result",
    [
        # Successful case
        ({'a': {'b': {'c': 10}}}, 'a.b.c', 10),

        # Non-existent field
        ({'a': {'b': {}}}, 'a.b.c', None),

        # Type error
        ({'a': 10}, 'a.b.c', None),

        # Empty dictionary
        ({}, 'a.b.c', None),

        # Empty nested field
        ({'a': {'b': {}}}, 'a.b', {}),

        # Partial path
        ({'a': {'b': {'c': 10, 'd': 20}}}, 'a.b', {'c': 10, 'd': 20}),

        # Handling non-dictionary at nested level
        ({'a': {'b': 'string_value'}}, 'a.b.c', None),
    ]
)
def test_extract_nested_field(record, field, expected_result):
    assert extract_nested_field(record, field) == expected_result

# process_and_print_cards tests
def test_process_and_print_cards_with_fields(capfd):
    cards = [{'id': 'card_id', 'name': 'Card Name', 'badges': {'comments': 2}}]
    process_and_print_cards(cards, ['id', 'name', 'badges.comments'])

    captured = capfd.readouterr()
    expected_output = '{"id": "card_id", "name": "Card Name", "badges.comments": 2}\n'
    assert captured.out == expected_output

def test_process_and_print_cards_without_fields(capfd):
    cards = [{'id': 'card_id', 'name': 'Card Name', 'badges': {'comments': 2}}]
    process_and_print_cards(cards, None)

    captured = capfd.readouterr()
    expected_output = '{"id": "card_id", "name": "Card Name", "badges": {"comments": 2}}\n'
    assert captured.out == expected_output

# parse_arguments tests
def test_parse_arguments(monkeypatch):
    test_args = ['script_name', 'config.json']
    monkeypatch.setattr('sys.argv', test_args)
    args = parse_arguments()
    assert args.config_path == 'config.json'
