"""
Test runner for all utility tests.
This file allows running all utility tests at once.
"""
import pytest
import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

if __name__ == "__main__":
    # Run all tests in the utils directory
    pytest.main(["-xvs", os.path.dirname(__file__)])
