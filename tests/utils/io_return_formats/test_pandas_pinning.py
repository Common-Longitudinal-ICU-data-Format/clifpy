"""The silent-failure defences around the pandas -> polars default flip.

polars supports df['col'], len(df) and boolean masks, so a caller that silently
receives polars where it expected pandas can look fine and diverge later. The defences:
pin every in-package caller, and guard the pandas boundary with a clear TypeError.
"""

import pandas as pd
import polars as pl
import pytest

from clifpy.tables.patient import Patient
from clifpy.utils.io import load_data


def test_base_table_rejects_a_polars_frame():
    """The boundary guard: a polars frame fails immediately, naming the fix.

    Without this the failure surfaces much later inside validation or summary stats
    as a confusing AttributeError.
    """
    df = pl.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    with pytest.raises(TypeError, match="return_format='pandas'"):
        Patient(data=df)


def test_base_table_accepts_pandas():
    df = pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    assert isinstance(Patient(data=df).df, pd.DataFrame)


def test_from_file_still_yields_pandas(demo_dir):
    """base_table.from_file is pinned, so the table layer is unaffected by the flip."""
    patient = Patient.from_file(data_directory=demo_dir, filetype="parquet",
                                timezone="US/Eastern")
    assert isinstance(patient.df, pd.DataFrame)


def test_pandas_format_matches_the_historic_default(demo_dir):
    """return_format='pandas' reproduces what a bare load_data() used to return."""
    df = load_data("vitals", demo_dir, "parquet", sample_size=50,
                   site_tz="US/Eastern", return_format="pandas")
    assert isinstance(df, pd.DataFrame)
    assert str(df["recorded_dttm"].dt.tz) == "US/Eastern"
    assert str(df["hospitalization_id"].dtype) == "string"


def test_bare_load_data_now_returns_polars(demo_dir):
    """The flip itself, asserted so the change is explicit rather than incidental."""
    assert isinstance(load_data("vitals", demo_dir, "parquet", sample_size=5),
                      pl.DataFrame)
