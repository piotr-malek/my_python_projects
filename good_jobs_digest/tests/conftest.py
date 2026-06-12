"""Pytest defaults: disable BigQuery unless explicitly testing BQ."""

import os

os.environ.setdefault("BQ_ENABLED", "false")
