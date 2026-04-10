"""
Descriptor encoding helpers for region-level categorical fields.

v1.1.1 behavior:
- basin_type is dropped from modeling (currently empty / low value)
- soil_texture_class is one-hot encoded with a stable vocabulary
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ml_training.config import REGION_DESCRIPTOR_STRING_COLUMNS


def build_descriptor_encodings(
    descriptors_df: pd.DataFrame,
    string_columns: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Build stable category vocabularies: {column: [sorted categories]}."""
    cols = list(string_columns or REGION_DESCRIPTOR_STRING_COLUMNS)
    out: Dict[str, List[str]] = {}
    for col in cols:
        if col not in descriptors_df.columns:
            continue
        series = descriptors_df[col].dropna()
        if series.empty:
            out[col] = {}
            continue
        uniques = sorted(series.astype(str).str.strip().unique())
        out[col] = uniques
    return out


def apply_descriptor_string_encodings(
    df: pd.DataFrame,
    encodings: Dict[str, List[str]],
) -> pd.DataFrame:
    """Apply one-hot encoding for configured descriptor columns.

    Produces columns named ``{col}__{category}`` and drops original text columns.
    Missing expected columns are created with zeros for shape stability.
    """
    if not encodings:
        return df
    out = df.copy()
    for col, categories in encodings.items():
        if col == "basin_type":
            if col in out.columns:
                out = out.drop(columns=[col])
            continue
        if not categories:
            if col in out.columns:
                out = out.drop(columns=[col])
            continue
        col_series = out[col].astype(str).str.strip() if col in out.columns else pd.Series("", index=out.index)
        for cat in categories:
            out[f"{col}__{cat}"] = (col_series == cat).astype(np.int8)
        if col in out.columns:
            out = out.drop(columns=[col])
    return out


def save_descriptor_encodings(encodings: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(encodings, f, indent=2, sort_keys=True)


def load_descriptor_encodings(path: Path) -> Dict[str, List[str]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {k: [str(x) for x in (v or [])] for k, v in data.items()}
