"""
conftest.py — repo-root pytest configuration.

WHY: `mock_sensor.py` lives in firmware/ but is not part of a package
(no firmware/__init__.py).  Adding firmware/ to sys.path lets test files
use bare `from mock_sensor import ...` imports without needing a package
install or a src-layout hack.
"""

import sys
import os

# Make `firmware/` importable as a plain-directory namespace so that
# `from mock_sensor import build_payload, SENSORS, jitter` resolves to
# firmware/mock_sensor.py regardless of where pytest is invoked from.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "firmware"))
