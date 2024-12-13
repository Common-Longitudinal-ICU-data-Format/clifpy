# tests/test_clif.py
import pytest
from pyclif.clif import CLIF
import os

def test_clif_initialization():
    data_dir = os.path.join(os.path.dirname(__file__), '../data')
    c = CLIF(data_dir=data_dir)
    assert c.data_dir == data_dir

def test_clif_load():
    data_dir = os.path.join(os.path.dirname(__file__), '../data')
    c = CLIF(data_dir=data_dir)
    c.load(table_list=['patient', 'hospitalization'], sample_size=10)
    loaded = c.get_loaded_tables()
    assert 'patient' in loaded or 'hospitalization' in loaded
