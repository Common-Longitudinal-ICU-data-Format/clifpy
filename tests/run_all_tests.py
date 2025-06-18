"""
Main test runner for pyCLIF.
This file allows running all tests at once or specific test categories.
"""
import pytest
import os
import sys
import argparse

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def run_tests(category=None):
    """
    Run tests based on the specified category.
    
    Args:
        category (str, optional): Test category to run. Options: 'tables', 'utils', 'all'.
            If None, runs all tests.
    """
    if category == 'tables':
        test_path = os.path.join(os.path.dirname(__file__), 'tables')
    elif category == 'utils':
        test_path = os.path.join(os.path.dirname(__file__), 'utils')
    else:  # Run all tests
        test_path = os.path.dirname(__file__)
    
    return pytest.main(["-xvs", test_path])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run pyCLIF tests')
    parser.add_argument('--category', choices=['tables', 'utils', 'all'], 
                        default='all', help='Test category to run')
    
    args = parser.parse_args()
    exit_code = run_tests(args.category if args.category != 'all' else None)
    sys.exit(exit_code)
