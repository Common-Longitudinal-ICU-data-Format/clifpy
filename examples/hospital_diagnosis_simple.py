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
    return (HospitalDiagnosis,)


@app.cell
def _(HospitalDiagnosis):
    # Initialize HospitalDiagnosis table
    table = HospitalDiagnosis(
        data_directory='/Users/sudo_sage/Documents/WORK/rush_clif/',
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


if __name__ == "__main__":
    app.run()
