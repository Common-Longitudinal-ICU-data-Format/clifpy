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


def test_base_table_accepts_a_polars_frame():
    """The table layer stores polars now, so a polars frame is taken as-is."""
    df = pl.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    patient = Patient(data=df)
    assert isinstance(patient.data, pl.DataFrame)
    assert patient.data.equals(df)


def test_base_table_rejects_an_unsupported_type():
    """The boundary guard still names the accepted types."""
    with pytest.raises(TypeError, match="pandas DataFrame, polars"):
        Patient(data=[{"patient_id": "1"}])


def test_base_table_accepts_pandas():
    """pandas input is converted on the way in; .df hands pandas back."""
    df = pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    patient = Patient(data=df)
    assert isinstance(patient.data, pl.DataFrame)
    assert isinstance(patient.df, pd.DataFrame)


def test_df_still_yields_pandas(demo_dir):
    """.df stays pandas for the ~20 modules that read it that way."""
    patient = Patient.from_file(data_directory=demo_dir, filetype="parquet",
                                timezone="US/Eastern")
    assert isinstance(patient.data, pl.DataFrame)
    assert isinstance(patient.df, pd.DataFrame)


def test_df_conversion_is_cached_and_invalidated(demo_dir):
    """The pandas view is built once, and rebuilt after .df is reassigned."""
    patient = Patient.from_file(data_directory=demo_dir, filetype="parquet",
                                timezone="US/Eastern")
    assert patient.df is patient.df                      # cached
    patient.df = pl.DataFrame({"patient_id": ["9"]})     # setter invalidates
    assert patient.df["patient_id"].tolist() == ["9"]


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
