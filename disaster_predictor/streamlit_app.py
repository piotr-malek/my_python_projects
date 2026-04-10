"""
Backward-compatible Streamlit entry point.

Prefer: ``streamlit run risk_monitor/app.py`` from the ``disaster_predictor`` directory.

This module imports the new app so ``streamlit run streamlit_app.py`` still works.
"""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import risk_monitor.app  # noqa: F401, E402
