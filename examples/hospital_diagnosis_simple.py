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
    from clifpy.utils.comorbidity import calculate_elix
    return HospitalDiagnosis, calculate_cci, calculate_elix


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


@app.cell
def _(calculate_elix, table):
    # Calculate Elixhauser Comorbidity Index
    elix_df = calculate_elix(table, hierarchy=True)
    return (elix_df,)


@app.cell
def _(elix_df):
    # Show first 10 Elixhauser results
    elix_df.head(10)
    return


@app.cell
def _(elix_df):
    elix_df.shape
    return


@app.cell
def _(elix_df):
    # Elixhauser score statistics
    elix_df['elix_score'].describe()
    return


@app.cell
def _(elix_df):
    for x in elix_df.columns:
        print(x, ' ->',elix_df[x].value_counts())
    return


@app.cell
def _(table):
    table.df[table.df['diagnosis_code'].str.contains('E',case=False)]['diagnosis_code'].unique()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
