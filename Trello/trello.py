import requests
import json
import sys
import argparse

def fetch_trello_cards(config_path):
    try:
        with open(config_path) as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        sys.stderr.write('404 error - Config file not found\n')
        sys.exit(1)
    except json.JSONDecodeError:
        sys.stderr.write('500 error - Config file is not valid JSON\n')
        sys.exit(1)

    try:
        key = config['credential']['key']
        token = config['credential']['access_token']
        board_id = config['boardId']
        fields = config.get('fields', None)

        if key is None or token is None or board_id is None:
            raise KeyError('key, access_token, or board_id missing from the config file')

    except KeyError as e:
        sys.stderr.write(f'400 error: missing credentials in config file: {e}\n')
        sys.exit(1)

    params = {'key': key, 'token': token}
    url = f'https://api.trello.com/1/boards/{board_id}/cards'

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        sys.stderr.write(f'HTTP error with code {response.status_code} occurred: {e}\n')
        sys.exit(2)
    except requests.exceptions.RequestException as e:
        sys.stderr.write(f'500 error: request error occurred: {e}\n')
        sys.exit(2)

    try:
        data = response.json()
        if not isinstance(data, list):
            sys.stderr.write("Error 204: Unexpected data format.\n")
            sys.exit(2)

        results = []
        if len(data) > 0:
            for record in data:
                if fields:
                    output = {}
                    for field in fields:
                        value = extract_nested_field(record, field)
                        if value is not None:
                            output[field] = value
                    results.append(output)  # Append filtered record to results list
                    print(json.dumps(output))  # Print the filtered record as JSON line
                else:
                    # If no fields are specified, output the entire record
                    results.append(record)  # Append entire record to results list
                    print(json.dumps(record))  # Print the entire record as JSON line
        else:
            sys.stderr.write("Error 204: No data found.\n")
            sys.exit(2)

        return results

    except json.JSONDecodeError:
        sys.stderr.write("Error 500: JSON decoding error in response.\n")
        sys.exit(2)
    except TypeError as e:
        sys.stderr.write(f"Error 204: Unexpected data format: {e}\n")
        sys.exit(2)

def extract_nested_field(record, field):
    nested_fields = field.split('.')
    value = record
    try:
        for nested_field in nested_fields:
            value = value[nested_field]
            if value is None:
                return None
    except (KeyError, TypeError):
        return None
    return value

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Trello cards from a board')
    parser.add_argument('config_path', metavar='config_path', type=str,
                        help='Path to the config.json file')

    args = parser.parse_args()
    fetch_trello_cards(args.config_path)
