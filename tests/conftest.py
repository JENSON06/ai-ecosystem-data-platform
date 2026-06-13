"""
Shared pytest configuration.

Fixtures defined here are available to all test files automatically.
"""

import sys
from pathlib import Path

# Ensure project root is always on the path regardless of how pytest is invoked
sys.path.insert(0, str(Path(__file__).parent.parent))
