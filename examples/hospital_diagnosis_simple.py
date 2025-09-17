import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import pandas as pd
    from pathlib import Path

    # Add project root to sys.path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from clifpy.tables.hospital_diagnosis import HospitalDiagnosis
    from clifpy.utils import calculate_cci
    return HospitalDiagnosis, calculate_cci


@app.cell
def _(HospitalDiagnosis):
    # Initialize HospitalDiagnosis table
    table = HospitalDiagnosis(
        data_directory='/Users/vaishvik/Downloads/WORK/rush_clif',
        filetype='parquet',
        timezone="UTC"
    )
    return (table,)


@app.cell
def _(table):
    table.validate()
    return


@app.cell
def _(table):
    table.errors
    return


@app.cell
def _(table):
    # Get diagnosis summary
    summary = table.get_diagnosis_summary()
    summary
    return


@app.cell
def _(table):
    table.df.columns
    return


@app.cell
def _(calculate_cci, table):
    # Calculate Charlson Comorbidity Index
    cci_df = calculate_cci(table, hierarchy=True)
    return (cci_df,)


@app.cell
def _(cci_df):
    # Show first 10 CCI results
    cci_df.head(10)
    return


@app.cell
def _(cci_df):
    cci_df.shape
    return


@app.cell
def _(cci_df):
    cci_df.dtypes
    return


@app.cell
def _(cci_df):
    # CCI score statistics
    cci_df['cci_score'].describe()
    return


if __name__ == "__main__":
    app.run()
