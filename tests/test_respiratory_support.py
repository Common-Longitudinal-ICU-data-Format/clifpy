# tests/test_respiratory_support.py
import pytest
from ..src.pyclif.clif import CLIF
import pandas as pd
import os

def test_respiratory_support_processing():
    data_dir = os.path.join(os.path.dirname(__file__), '../data')
    c = CLIF(data_dir=data_dir)
    c.load(table_list=['respiratory_support'], sample_size=10)
    c.respiratory_support.validate()
    # Test the process function
    original_count = len(c.respiratory_support.df)
    c.respiratory_support.process_data()
    processed_count = len(c.respiratory_support.df)
    assert processed_count <= original_count
