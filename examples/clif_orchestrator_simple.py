import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
    # ClifOrchestrator Simple Demo

    This notebook demonstrates basic usage of ClifOrchestrator to load patient, hospitalization, and ADT tables.
    """
    )
    return


@app.cell
def _():
    import os
    from clifpy.clif_orchestrator import ClifOrchestrator

    print("Libraries imported successfully!")
    return (ClifOrchestrator,)


@app.cell
def _(mo):
    mo.md(r"""## Configure and Initialize""")
    return


@app.cell
def _(ClifOrchestrator):
    # Create ClifOrchestrator instance
    co = ClifOrchestrator(
        data_directory='/Users/sudo_sage/Documents/work/clif_mimic',
        filetype='parquet',
        timezone='UTC',
        output_directory= None
    )

    print(f"ClifOrchestrator initialized")
    print(f"  Data Directory: {co.data_directory}")
    print(f"  File Type: {co.filetype}")
    print(f"  Timezone: {co.timezone}")
    print(f"  Output Directory: {co.output_directory}")
    return (co,)


@app.cell
def _(mo):
    mo.md(r"""## Load Tables""")
    return


@app.cell
def _(co):
    # Load the three main tables using the new generic load_table function
    try:
        # Load patient table
        co.load_table('patient')
        print("✅ Patient table loaded")

        # Load hospitalization table
        co.load_table('hospitalization')
        print("✅ Hospitalization table loaded")

        # Load ADT table
        co.load_table('adt')
        print("✅ ADT table loaded")

    except Exception as e:
        print(f"Error loading tables: {e}")
        print("Make sure your data files exist in the specified directory")
    return


@app.cell
def _(co, mo):
    # Show loaded tables
    loaded_tables = co.get_loaded_tables()

    mo.md(f"""
    ## Loaded Tables

    Currently loaded: **{', '.join(loaded_tables) if loaded_tables else 'No tables loaded'}**

    Table objects available:
    - `co.patient` - {type(co.patient).__name__ if co.patient else 'Not loaded'}
    - `co.hospitalization` - {type(co.hospitalization).__name__ if co.hospitalization else 'Not loaded'}
    - `co.adt` - {type(co.adt).__name__ if co.adt else 'Not loaded'}
    """)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Usage

    You can now work with the loaded tables:

    ```python
    # Access DataFrames
    patient_df = co.patient.df
    hosp_df = co.hospitalization.df
    adt_df = co.adt.df

    # Load additional tables using the new generic function
    co.load_table('labs')
    co.load_table('vitals')

    # Or load multiple tables at once using initialize
    co.initialize(['patient', 'hospitalization', 'labs', 'vitals'])

    # Run validation
    co.validate_all()

    # Get table summaries
    co.patient.get_summary()
    ```
    """
    )
    return


@app.cell
def _(co):
    co.validate_all()
    return


@app.cell
def _(co):
    co.get_loaded_tables() # gives list of active tables in co
    return


@app.cell
def _(co):
    co.get_tables_obj_list()
    return


@app.cell
def _(co):
    co.patient.errors
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
