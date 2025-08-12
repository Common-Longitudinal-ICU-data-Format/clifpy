import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    from pathlib import Path

    # Add the src directory to the path so we can import clifpy
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / "src"))

    mo.md(
        """
        # Demo: Load CLIF demo data from files

        This app loads CLIF demo Parquet files from `src/clifpy/data/clif_demo` using the `clifpy` table APIs with timezone `US/Eastern`, and writes validation outputs to `examples/outpu`.
        """
    )
    return mo, project_root


@app.cell
def _(project_root):
    # Configuration
    DATA_DIR = (project_root / "src" / "clifpy" / "data" / "clif_demo").resolve()
    OUTPUT_DIR = (project_root / "examples" / "outpu").resolve()
    FILETYPE = "parquet"
    TIMEZONE = "US/Eastern"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Data directory:", DATA_DIR)
    print("Output directory:", OUTPUT_DIR)
    print("Filetype:", FILETYPE)
    print("Timezone:", TIMEZONE)

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
def _(
    adt_table,
    hospitalization_table,
    labs_table,
    mo,
    patient_table,
    vitals_table,
):
    mo.md("## Validation status")

    mo.md(
        f"""
        - **patient valid**: {patient_table.isvalid()}  
        - **hospitalization valid**: {hospitalization_table.isvalid()}  
        - **adt valid**: {adt_table.isvalid()}  
        - **labs valid**: {labs_table.isvalid()}  
        - **vitals valid**: {vitals_table.isvalid()}
        """
    )
    return


@app.cell
def _(mo, patient_table):
    mo.md("### patient sample")
    patient_table.df.head(10)
    return


@app.cell
def _(hospitalization_table, mo):
    mo.md("### hospitalization sample and summary")
    summary = hospitalization_table.get_summary_stats()
    mo.md(f"Total hospitalizations: {summary.get('total_hospitalizations', 0)}")
    hospitalization_table.df.head(10)
    return


@app.cell
def _(adt_table, mo):
    mo.md("### ADT locations")
    locations = adt_table.get_location_categories()
    mo.md(", ".join(f"`{loc}`" for loc in locations) or "No locations found")
    adt_table.df.head(10)
    return


@app.cell
def _(labs_table, mo):
    mo.md("### Labs sample and summary stats")
    labs_table.df.head(10)
    stats = labs_table.get_lab_summary_stats()
    if not stats.empty:
        stats.head(10)
    return


@app.cell
def _(mo, vitals_table):
    mo.md("### Vitals sample and summary stats")
    vitals_table.df.head(10)
    stats = vitals_table.get_vital_summary_stats()
    if not stats.empty:
        stats.head(10)
    return


@app.cell
def _(patient_table):
    # Ensure validation has run and display number of errors found
    patient_table.validate()
    print("patient validation errors:", len(patient_table.errors))

    return


if __name__ == "__main__":
    app.run()
