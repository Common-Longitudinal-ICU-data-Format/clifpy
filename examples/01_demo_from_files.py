import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    from pathlib import Path

    # Add repository root to sys.path so top-level clifpy is importable when running uninstalled
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    header = mo.md(
        """
        # Demo: Load CLIF demo data from files

        This app loads CLIF demo Parquet files from `clifpy/data/clif_demo` using the `clifpy` table APIs with timezone `US/Eastern`, and writes validation outputs to `examples/output`.
        """
    )
    return mo, project_root


@app.cell
def _(mo, project_root):
    # Configuration
    DATA_DIR = (project_root / "clifpy" / "data" / "clif_demo").resolve()
    OUTPUT_DIR = (project_root / "examples" / "output").resolve()
    FILETYPE = "parquet"
    TIMEZONE = "US/Eastern"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    info = mo.md(
        f"""
        - **Data directory**: `{DATA_DIR}`  
        - **Output directory**: `{OUTPUT_DIR}`  
        - **Filetype**: `{FILETYPE}`  
        - **Timezone**: `{TIMEZONE}`
        """
    )
    print("Setup complete")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, TIMEZONE


@app.cell
def _(DATA_DIR, FILETYPE, OUTPUT_DIR, TIMEZONE, mo):
    # Import table classes from top-level clifpy exports
    from clifpy import (
        patient,
        hospitalization,
        adt,
        labs,
        vitals,
    )

    mo.md("## Load tables using from_file()")

    patient_table = patient.from_file(
        data_directory=str(DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
    )

    hospitalization_table = hospitalization.from_file(
        data_directory=str(DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
    )

    adt_table = adt.from_file(
        data_directory=str(DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
    )

    labs_table = labs.from_file(
        data_directory=str(DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
    )

    vitals_table = vitals.from_file(
        data_directory=str(DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
    )

    return (
        adt_table,
        hospitalization_table,
        labs_table,
        patient_table,
        vitals_table,
    )


@app.cell
def _(patient_table):
    patient_table.isvalid()
    return


@app.cell
def _(hospitalization_table):
    summary = hospitalization_table.get_summary_stats()
    summary
    return


@app.cell
def _(adt_table):
    locations = adt_table.get_location_categories()
    locations
    return


@app.cell
def _(labs_table):
    labs_table.get_lab_summary_stats()
    return


@app.cell
def _(labs_table):
    labs_summary = labs_table.get_lab_summary_stats()
    labs_summary
    return


@app.cell
def _(vitals_table):
    vitals_summary = vitals_table.get_vital_summary_stats()
    vitals_summary 
    return


if __name__ == "__main__":
    app.run()
