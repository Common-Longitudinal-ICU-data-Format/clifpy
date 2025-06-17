"""
Configuration file for pytest.
This file contains fixtures and configuration settings for pytest.
"""
import pytest
import os
import sys

# Add the src directory to the path so that imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Define fixtures that can be used across all tests
@pytest.fixture
def data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), '../data')
