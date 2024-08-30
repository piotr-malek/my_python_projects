class ConfigError(Exception):
    """Raised when there is an error with the configuration."""
    pass

class FetchError(Exception):
    """Raised when there is an error fetching data from the Trello API."""
    pass