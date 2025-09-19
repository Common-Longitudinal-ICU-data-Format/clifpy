import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    # Add repository root to sys.path so top-level clifpy is importable when running uninstalled
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    mo.md("""
    # Outlier Handling for Vitals Data

    This notebook demonstrates how to use the **outlier handling functionality** in clifpy to clean vitals data by detecting and removing physiologically implausible values.

    ## What is Outlier Handling?

    Outlier handling in clifpy:
    - 🎯 **Identifies values outside clinically reasonable ranges** for each vital sign
    - 🔧 **Converts outliers to NaN** to preserve data structure
    - 📊 **Provides detailed statistics** on data cleaning impact
    - ⚙️ **Uses configurable ranges** (CLIF standard or custom)
    """)
    return


@app.cell
def _():
    # Configuration - Edit these parameters for your data
    DATA_DIR = "/Users/sudo_sage/Documents/work/rush_clif/"  # Edit this path
    FILETYPE = "parquet"  # Edit this: "csv", "parquet", etc.
    TIMEZONE = "US/Eastern"  # Edit this: "UTC", "US/Eastern", "US/Pacific", etc.
    SAMPLE_SIZE = 1000000  # Edit this: None for all data, or integer for sample
    return DATA_DIR, FILETYPE, TIMEZONE


@app.cell
def _(DATA_DIR, FILETYPE, TIMEZONE):
    # Import Vitals table class from top-level clifpy exports
    from clifpy import Vitals

    # Load vitals data using from_file method
    vitals_table = Vitals.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    print(f"Loaded vitals data:")
    print(f"- {len(vitals_table.df)} vital sign measurements")
    print(f"- {vitals_table.df['hospitalization_id'].nunique()} unique hospitalizations")
    print(f"- {vitals_table.df['vital_category'].nunique()} different vital signs")
    return (vitals_table,)


@app.cell
def _(vitals_table):
    from clifpy.utils import apply_outlier_handling

    # Create a copy of the data for comparison
    original_df = vitals_table.df.copy()

    print("Applying outlier handling...")
    print("=" * 50)

    # Apply outlier handling (this will modify vitals_table.df in-place)
    apply_outlier_handling(vitals_table)

    print("\nOutlier handling completed!")
    return (apply_outlier_handling,)


@app.cell
def _(DATA_DIR, FILETYPE, TIMEZONE):
    from clifpy import Labs

    # Load vitals data using from_file method
    Labs_table = Labs.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    return (Labs_table,)


@app.cell
def _(Labs_table, apply_outlier_handling):
    apply_outlier_handling(Labs_table)
    return


@app.cell
def _(Labs_table):
    Labs_table.df.lab_category.value_counts()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
