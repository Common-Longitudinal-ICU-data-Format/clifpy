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
    return ClifOrchestrator, os


@app.cell
def _(mo):
    mo.md(r"""## Configure and Initialize""")
    return


@app.cell
def _(mo, os):
    # Create input widgets for configuration
    data_dir = mo.ui.text(
        value="./sample_data",
        label="Data Directory:",
        placeholder="Enter path to data directory"
    )

    filetype = mo.ui.dropdown(
        options=["csv", "parquet"],
        value="csv",
        label="File Type:"
    )

    timezone = mo.ui.dropdown(
        options=["UTC", "America/New_York", "America/Chicago", "America/Los_Angeles", "Europe/London"],
        value="UTC",
        label="Timezone:"
    )

    output_dir = mo.ui.text(
        value=os.path.join(os.getcwd(), "output"),
        label="Output Directory:",
        placeholder="Leave empty for default"
    )

    mo.vstack([
        data_dir,
        filetype,
        timezone,
        output_dir
    ])
    return data_dir, filetype, output_dir, timezone


@app.cell
def _(ClifOrchestrator, data_dir, filetype, output_dir, timezone):
    # Create ClifOrchestrator instance
    co = ClifOrchestrator(
        data_directory=data_dir.value,
        filetype=filetype.value,
        timezone=timezone.value,
        output_directory=output_dir.value if output_dir.value else None
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
    # Load the three main tables
    try:
        # Load patient table
        co.load_patient_data()
        print("✅ Patient table loaded")

        # Load hospitalization table
        co.load_hospitalization_data()
        print("✅ Hospitalization table loaded")

        # Load ADT table
        co.load_adt_data()
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
def _(co, mo):
    # Display basic info about each loaded table
    info = []

    if co.patient and hasattr(co.patient, 'df') and co.patient.df is not None:
        info.append(f"**Patient:** {len(co.patient.df)} rows, {len(co.patient.df.columns)} columns")

    if co.hospitalization and hasattr(co.hospitalization, 'df') and co.hospitalization.df is not None:
        info.append(f"**Hospitalization:** {len(co.hospitalization.df)} rows, {len(co.hospitalization.df.columns)} columns")

    if co.adt and hasattr(co.adt, 'df') and co.adt.df is not None:
        info.append(f"**ADT:** {len(co.adt.df)} rows, {len(co.adt.df.columns)} columns")

    if info:
        mo.md("## Table Information\n\n" + "\n\n".join(info))
    else:
        mo.md("*No data loaded. Check that your data files exist in the specified directory.*")
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

    # Run validation
    co.validate_all()

    # Get table summaries
    co.patient.get_summary()
    ```
    """
    )
    return


if __name__ == "__main__":
    app.run()
