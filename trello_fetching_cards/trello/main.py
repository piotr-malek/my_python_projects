import sys
import logging
from trello.config import Config
from trello.fetch import fetch_cards
from trello.utils import process_and_print_cards, parse_arguments
from trello.errors import ConfigError, FetchError

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Main function that orchestrates the fetching and processing of Trello cards.
    
    Exit Codes:
    1 - Configuration error
    2 - Fetching from Trello API error
    3 - Unexpected error
    """
    try:
        args = parse_arguments()
        config = Config.from_file(args.config_path)

        for card in fetch_cards(config):
            process_and_print_cards([card], config.fields)
        else:
            logger.warning("Board has no cards.")  # Executed if no cards were yielded.

    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except FetchError as e:
        logger.error(f"Failed to fetch Trello cards: {e}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(3)

if __name__ == "__main__":
    main()