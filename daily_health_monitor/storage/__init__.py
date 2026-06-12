from storage.bigquery import BigQueryClient
from storage.repository import Repository

_bq = None
_repo = None


def get_bq():
    global _bq
    if _bq is None:
        from config import settings

        _bq = BigQueryClient(settings)
    return _bq


def get_repo():
    global _repo
    if _repo is None:
        from config import settings

        _repo = Repository(get_bq(), settings)
    return _repo


def reset_storage():
    """Clear cached clients (for tests)."""
    global _bq, _repo
    _bq = None
    _repo = None


def __getattr__(name):
    if name == "bq":
        return get_bq()
    if name == "repo":
        return get_repo()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
