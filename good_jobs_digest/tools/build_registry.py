#!/usr/bin/env python3
"""Shim: run discovery/build_registry.py (employer discovery → curated_companies)."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
runpy.run_path(str(ROOT / "discovery" / "build_registry.py"), run_name="__main__")
