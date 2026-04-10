"""Shared constants for risk assessment (used by templates and tooling)."""

import pandas as pd

# Last date MODIS NDVI is used; Landsat continues after this (see ml_training.config MODIS_END).
MODIS_CUTOFF_DATE = pd.to_datetime("2024-08-31")
