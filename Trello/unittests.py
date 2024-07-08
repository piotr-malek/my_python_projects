import unittest
from unittest.mock import patch, mock_open
import trello
import json
from parameterized import parameterized
import requests


class TestFetchTrelloCards(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.expected_result_no_fields = [
            {
                "id": "61ea9f71466afd2c1c34fc75",
                "badges": {
                    "attachmentsByType": {
                        "trello": {
                            "board": 0,
                            "card": 0
                        }
                    },
                    "externalSource": None,
                    "location": False,
                    "votes": 0,
                    "viewingMemberVoted": False,
                    "subscribed": False,
                    "fogbugz": "",
                    "checkItems": 0,
                    "checkItemsChecked": 0,
                    "checkItemsEarliestDue": None,
                    "comments": 0,
                    "attachments": 0,
                    "description": False,
                    "due": None,
                    "dueComplete": False,
                    "start": None,
                    "lastUpdatedByAi": False
                },
                "checkItemStates": [],
                "closed": False,
                "dueComplete": False,
                "dateLastActivity": "2022-03-02T12:28:16.675Z",
                "desc": "",
                "descData": {
                    "emoji": {}
                },
                "due": None,
                "dueReminder": None,
                "email": None,
                "idBoard": "61ea9df2f2c3488038e96f19",
                "idChecklists": [],
                "idList": "61ea9dfd1bd70284b916b96f",
                "idMembers": [],
                "idMembersVoted": [],
                "idShort": 11,
                "idAttachmentCover": None,
                "labels": [
                    {
                        "id": "61ea9df28166f38753df7830",
                        "idBoard": "61ea9df2f2c3488038e96f19",
                        "name": "",
                        "color": "green",
                        "uses": 2
                    }
                ],
                "idLabels": [
                    "61ea9df28166f38753df7830"
                ],
                "manualCoverAttachment": False,
                "name": "Plan activities during the summit",
                "pos": 65535,
                "shortLink": "2IOEaZ64",
                "shortUrl": "https://trello.com/c/2IOEaZ64",
                "start": None,
                "subscribed": False,
                "url": "https://trello.com/c/2IOEaZ64/11-plan-activities-during-the-summit",
                "cover": {
                    "idAttachment": None,
                    "color": None,
                    "idUploadedBackground": None,
                    "size": "normal",
                    "brightness": "dark",
                    "idPlugin": None
                },
                "isTemplate": False,
                "cardRole": None
            }
        ]

        cls.expected_result_with_fields = [
            {"name": "Plan activities during the summit", "badges.comments": 0}
        ]        

        cls.config_data_no_fields = json.dumps({
            "credential": {
                "access_token": "fake_access_token",
                "key": "fake_key"
            },
            "boardId": "fake_board_id",
            "fields": None
        })

        cls.config_data_with_fields = json.dumps({
            "credential": {
                "access_token": "fake_access_token",
                "key": "fake_key"
            },
            "boardId": "fake_board_id",
            "fields": ["name", "badges.comments"]
        })

    # Positive outcome tests, for when config file contains no fields (returns whole response) and when there are fields specified (script applies filtering)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_positive_no_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.expected_result_no_fields

        result = trello.fetch_trello_cards('config.json')

        self.assertEqual(result, self.expected_result_no_fields)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_positive_with_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.expected_result_no_fields

        result = trello.fetch_trello_cards('config.json')

        self.assertEqual(result, self.expected_result_with_fields)

    # HTTP errors tests

    @parameterized.expand([
        (401, 'Unauthorized access', 2),
        (403, 'Forbidden access', 2),
        (404, 'Board not found', 2),      
        (500, 'Server error', 2),
    ])
    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_http_errors_no_fields(self, status_code, expected_error_message, expected_exit_code, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = status_code
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_get.return_value)

        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        
        self.assertEqual(ex.exception.code, expected_exit_code)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_request_exception(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.side_effect = requests.exceptions.RequestException("Request failed")
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)

    @parameterized.expand([
        (401, 'Unauthorized access', 2),
        (403, 'Forbidden access', 2),
        (404, 'Board not found', 2),      
        (500, 'Server error', 2),
    ])

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_http_errors_with_fields(self, status_code, expected_error_message, expected_exit_code, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = status_code
        mock_get.return_value.json.return_value = {'message': expected_error_message}
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, expected_exit_code)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_request_exception(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.side_effect = requests.exceptions.RequestException("Request failed")
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)
    
    # Config file tests

    @patch('trello.requests.get')
    @patch('builtins.open')
    def test_config_file_not_found(self, mock_open, mock_get):
        mock_open.side_effect = FileNotFoundError
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 1)    

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open, read_data='invalid_json')
    def test_config_file_invalid_json(self, mock_open, mock_get):
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 1)    

    @parameterized.expand([
        ("missing_access_token", {
            "credential": {
                "access_token": None,
                "key": "fake_key"
            },
            "boardId": "fake_board_id",
            "fields": None
        }),
        ("missing_key", {
            "credential": {
                "access_token": "fake_access_token",
                "key": None
            },
            "boardId": "fake_board_id",
            "fields": None
        }),
        ("missing_board_id", {
            "credential": {
                "access_token": "fake_access_token",
                "key": "fake_key"
            },
            "boardId": None,
            "fields": None
        }),
        ("missing_access_token_with_fields", {
            "credential": {
                "access_token": None,
                "key": "fake_key"
            },
            "boardId": "fake_board_id",
            "fields": ["name", "badges.comments"]
        }),
        ("missing_key_with_fields", {
            "credential": {
                "access_token": "fake_access_token",
                "key": None
            },
            "boardId": "fake_board_id",
            "fields": ["name", "badges.comments"]
        }),
        ("missing_board_id_with_fields", {
            "credential": {
                "access_token": "fake_access_token",
                "key": "fake_key"
            },
            "boardId": None,
            "fields": ["name", "badges.comments"]
        }),
    ])
    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_config_missing_credentials(self, name, config_data, mock_open, mock_get):
        mock_open.return_value.read.return_value = json.dumps(config_data)

        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')

        self.assertEqual(ex.exception.code, 1, f"Failed test case: {name}")

    # API response tests

    @parameterized.expand([
        ("not_a_list", {"sample": "dictionary"}),
        ("none_response", None),
        ("string_response", "invalid_data")
    ])
    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_unexpected_data_format_no_fields(self, name, mock_data, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data

        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2, f'Failed test case: {name}')  

    @parameterized.expand([
        ("not_a_list", {"sample": "dictionary"}),
        ("none_response", None),
        ("string_response", "invalid_data")
    ])
    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_unexpected_data_format_with_fields(self, name, mock_data, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data

        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2, f'Failed test case: {name}')  

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_data_length_zero_with_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {}
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)        

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_data_length_more_than_zero_with_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {"name": "Card 1", "badges": {"comments": 5}},
            {"name": "Card 2", "badges": {"comments": 3}}
        ]
        result = trello.fetch_trello_cards('config.json')
        self.assertEqual(len(result), 2)   

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_data_length_zero_no_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {}
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)        

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_data_length_more_than_zero_no_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {
                "id": "61ea9f71466afd2c1c34fc75",
                "badges": {
                    "attachmentsByType": {
                        "trello": {
                            "board": 0,
                            "card": 0
                        }
                    }}},
            {
                "id": "71ea9f71466afd2c1c34fc75",
                "badges": {
                    "attachmentsByType": {
                        "trello": {
                            "board": 1,
                            "card": 1
                        }
                    }}}
        ]

        result = trello.fetch_trello_cards('config.json')
        self.assertEqual(len(result), 2)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_response_invalid_json_no_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_response_invalid_json_with_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_response_type_error_no_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_no_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = TypeError("Response has a TypeError")
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2) 

    @patch('trello.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_response_type_error_with_fields(self, mock_open, mock_get):
        mock_open.return_value.read.return_value = self.config_data_with_fields
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = TypeError("Response has a TypeError")
        with self.assertRaises(SystemExit) as ex:
            trello.fetch_trello_cards('config.json')
        self.assertEqual(ex.exception.code, 2)          

class TestExtractNestedFields(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self) :
        pass

    def test_valid_nested_field(self):
        record = {
            "badges": {
                "comments": 0
            },
            "name": "Plan activities during the summit"
        }
        field = "badges.comments"
        expected_value = 0
        result = trello.extract_nested_field(record, field)
        self.assertEqual(result, expected_value)

    def test_none_value(self):
        record = {
            "badges": {
                "comments": None
            },
            "name": "Plan activities during the summit"
        }
        field = "badges.comments"
        result = trello.extract_nested_field(record, field)
        self.assertIsNone(result)

    def test_non_existing_or_invalid_field(self):
        record = {
            "badges": {
                "comments": 0
            },
            "name": "Plan activities during the summit"
        }

        # Test for a non-existing nested field
        field_non_existing = "badges.likes"
        result_non_existing = trello.extract_nested_field(record, field_non_existing)
        self.assertIsNone(result_non_existing)

        # Test for a missing top-level field
        field_invalid = "invalid_field"
        result_invalid = trello.extract_nested_field(record, field_invalid)
        self.assertIsNone(result_invalid)

    def test_type_error(self):
        record = {
            "badges": "this is not a dict"
        }
        field = "badges.comments"
        result = trello.extract_nested_field(record, field)
        self.assertIsNone(result)

if __name__ == "__main__":
    unittest.main()