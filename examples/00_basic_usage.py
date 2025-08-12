import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    # Add the src directory to the path so we can import clifpy
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

    mo.md("""
    # clifpy Basic Usage Guide

    Welcome to clifpy! This notebook demonstrates the basic functionality of the clifpy package for working with CLIF (Common Longitudinal ICU Data Format) data.

    ## What is clifpy?

    clifpy is a Python package that provides:
    - 📊 Standardized data structures for ICU data
    - ✅ Automatic validation against CLIF schemas
    - 🔍 Easy data exploration and analysis
    - 📈 Clinical-specific methods for each table type

    Let's explore how to use it!
    """)
    return mo, pd


@app.cell
def _(mo):
    mo.md(
        """
    ## 1. Exploring Available Demo Data

    clifpy comes with demo datasets that you can use to learn and test. Let's see what's available:
    """
    )
    return


@app.cell
def _():
    from clifpy.data import get_demo_summary, list_demo_datasets

    # Show a summary of all demo datasets
    get_demo_summary()
    return (list_demo_datasets,)


@app.cell
def _(list_demo_datasets, mo):
    # Get detailed information about each dataset
    datasets_info = list_demo_datasets()

    mo.md("""
    ## 2. Loading Demo Data

    There are two ways to load demo data:

    1. **As table objects** (with automatic validation) - Recommended for most use cases
    2. **As raw DataFrames** (without validation) - For custom analysis

    Let's start with the recommended approach:
    """)
    return


@app.cell
def _(mo):
    from clifpy.data import load_demo_patient, load_demo_hospitalization, load_demo_adt

    mo.md("### Loading Patient Demographics")

    # Load the patient table
    patient_table = load_demo_patient()

    mo.md(f"""
    The patient table has been loaded with:
    - **{len(patient_table.df)} patients**
    - **Validation status:** {'✅ Valid' if patient_table.isvalid() else '❌ Has validation issues'}
    - **{len(patient_table.errors)} validation errors** (if any)

    Let's look at the data:
    """)
    return (
        load_demo_adt,
        load_demo_hospitalization,
        load_demo_patient,
        patient_table,
    )


@app.cell
def _(mo, patient_table):
    # Display first few rows
    mo.md("#### Patient Data Sample")
    patient_table.df.head()
    return


@app.cell
def _(mo, patient_table):
    # Check what columns are available
    mo.md(f"""
    #### Available Columns

    The patient table contains these columns:
    {', '.join(f'`{col}`' for col in patient_table.df.columns)}
    """)
    return


@app.cell
def _(mo, patient_table):
    # Show demographics summary
    mo.md("#### Demographics Summary")

    if 'sex_category' in patient_table.df.columns:
        sex_counts = patient_table.df['sex_category'].value_counts()
        mo.md(f"""
        **Sex Distribution:**
        {sex_counts.to_frame().to_markdown()}
        """)

    if 'race_category' in patient_table.df.columns:
        race_counts = patient_table.df['race_category'].value_counts()
        mo.md(f"""
        **Race Distribution:**
        {race_counts.to_frame().to_markdown()}
        """)
    return


@app.cell
def _(load_demo_hospitalization, mo):
    mo.md("### Loading Hospitalizations")

    # Load hospitalization data
    hosp_table = load_demo_hospitalization()

    mo.md(f"""
    The hospitalization table contains:
    - **{len(hosp_table.df)} hospitalizations**
    - **{hosp_table.df['patient_id'].nunique()} unique patients**
    - **Validation:** {'✅ Valid' if hosp_table.isvalid() else '❌ Has issues'}
    """)
    return (hosp_table,)


@app.cell
def _(hosp_table):
    # Show hospitalization data
    hosp_table.df.head()
    return


@app.cell
def _(hosp_table, mo):
    mo.md("#### Clinical Analysis Methods")

    # Use table-specific methods
    mortality_rate = hosp_table.get_mortality_rate()
    summary_stats = hosp_table.get_summary_stats()

    mo.md(f"""
    The hospitalization table provides several built-in analysis methods:

    **In-hospital Mortality Rate:** {mortality_rate:.1f}%

    **Age Statistics:**
    - Mean: {summary_stats.get('age_stats', {}).get('mean', 'N/A')} years
    - Median: {summary_stats.get('age_stats', {}).get('median', 'N/A')} years
    - Range: {summary_stats.get('age_stats', {}).get('min', 'N/A')} - {summary_stats.get('age_stats', {}).get('max', 'N/A')} years

    **Length of Stay:**
    - Mean: {summary_stats.get('length_of_stay_stats', {}).get('mean_days', 'N/A')} days
    - Median: {summary_stats.get('length_of_stay_stats', {}).get('median_days', 'N/A')} days
    """)
    return


@app.cell
def _(hosp_table, mo, pd):
    # Show discharge categories
    if 'discharge_category_counts' in hosp_table.get_summary_stats():
        discharge_df = pd.DataFrame(
            list(hosp_table.get_summary_stats()['discharge_category_counts'].items()),
            columns=['Discharge Category', 'Count']
        ).sort_values('Count', ascending=False)

        mo.md("**Discharge Categories:**")
        discharge_df
    return


@app.cell
def _(load_demo_adt, mo):
    mo.md("### Loading ADT (Admission/Discharge/Transfer) Data")

    # Load ADT data
    adt_table = load_demo_adt()

    mo.md(f"""
    The ADT table tracks patient movements:
    - **{len(adt_table.df)} transfer records**
    - **{adt_table.df['hospitalization_id'].nunique()} hospitalizations**
    - **Validation:** {'✅ Valid' if adt_table.isvalid() else '❌ Has issues'}
    """)
    return (adt_table,)


@app.cell
def _(adt_table):
    # Show ADT data
    adt_table.df.head()
    return


@app.cell
def _(adt_table, mo):
    # Use ADT-specific methods
    location_categories = adt_table.get_location_categories()
    adt_summary = adt_table.get_summary_stats()

    mo.md(f"""
    #### Location Analysis

    **Available locations:** {', '.join(f'`{loc}`' for loc in location_categories)}

    **Location distribution:**
    """)

    # Show location counts
    if 'location_category_counts' in adt_summary:
        for location, count in adt_summary['location_category_counts'].items():
            mo.md(f"- {location}: {count} records")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 3. Loading Raw DataFrames

    Sometimes you need just the raw data without validation. You can do this with `return_raw=True`:
    """
    )
    return


@app.cell
def _(load_demo_patient, mo, pd):
    # Load raw DataFrames
    patient_df = load_demo_patient(return_raw=True)

    mo.md(f"""
    ```python
    # Load raw DataFrame without validation
    patient_df = load_demo_patient(return_raw=True)
    ```

    This gives you a standard pandas DataFrame with shape: {patient_df.shape}
    """)

    # Show data types
    mo.md("**Data types:**")
    pd.DataFrame({
        'Column': patient_df.dtypes.index,
        'Type': patient_df.dtypes.values
    })
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 4. Understanding Validation

    When you load data as table objects, clifpy automatically validates:

    - ✅ Required columns are present
    - ✅ Data types are correct
    - ✅ Categorical values are valid
    - ✅ No duplicates on composite keys
    - ✅ Missing data analysis
    - ✅ Statistical summaries

    Let's look at validation errors (if any):
    """
    )
    return


@app.cell
def _(mo, patient_table):
    if patient_table.errors:
        mo.md(f"""
        ### Patient Table Validation Issues

        Found {len(patient_table.errors)} validation issues:
        """)

        # Show first few errors
        for i, error in enumerate(patient_table.errors[:3]):
            mo.md(f"""
            **Error {i+1}:** {error.get('type', 'Unknown')}
            - Details: {error}
            """)

        if len(patient_table.errors) > 3:
            mo.md(f"*... and {len(patient_table.errors) - 3} more errors*")
    else:
        mo.md("✅ No validation errors in patient table!")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 5. Working with Validation Outputs

    clifpy saves detailed validation results to files. Let's check what was generated:
    """
    )
    return


@app.cell
def _(mo, patient_table):
    import os

    if hasattr(patient_table, 'output_directory'):
        output_dir = patient_table.output_directory

        if os.path.exists(output_dir):
            files = sorted([f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))])

            mo.md(f"""
            ### Generated Files in `{output_dir}`

            clifpy creates several output files during validation:
            """)

            for file in files[:10]:  # Show first 10 files
                file_type = "CSV Report" if file.endswith('.csv') else "Log File" if file.endswith('.log') else "Other"
                mo.md(f"- `{file}` ({file_type})")

            if len(files) > 10:
                mo.md(f"*... and {len(files) - 10} more files*")

            mo.md("""
            These files contain:
            - **CSV files**: Statistical summaries, missing data reports, skewness analysis
            - **Log files**: Detailed validation logs with warnings and errors
            """)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 6. Common Usage Patterns

    Here are some common patterns for working with clifpy:

    ### Pattern 1: Filter and Analyze
    ```python
    # Get all ICU stays
    icu_stays = adt_table.filter_by_location_category('icu')

    # Get hospitalizations for a specific patient
    patient_hosps = hosp_table.filter_by_patient('patient_001')
    ```

    ### Pattern 2: Date Range Analysis
    ```python
    from datetime import datetime

    # Filter ADT events by date
    recent_events = adt_table.filter_by_date_range(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 12, 31)
    )
    ```

    ### Pattern 3: Combine Multiple Tables
    ```python
    # Join patient demographics with hospitalizations
    merged_df = pd.merge(
        patient_table.df,
        hosp_table.df,
        on='patient_id',
        how='inner'
    )
    ```
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 7. Next Steps

    Now that you understand the basics, you can:

    1. **Explore other tables** - As more tables are implemented (labs, vitals, etc.), they'll follow the same pattern

    2. **Use your own data** - Load your CLIF-formatted data:
       ```python
       from clifpy.tables.patient import patient

       patient = patient.from_file(
           data_directory="/path/to/your/data",
           filetype="parquet",
           timezone="US/Eastern"
       )
       ```

    3. **Build analyses** - Use the validated data for clinical research and quality improvement

    ## Resources

    - [CLIF Specification](https://clif-consortium.github.io/website/)
    - [clifpy GitHub](https://github.com/clif-consortium/clifpy)
    - Table-specific notebooks in the examples folder

    Happy CLIF-ing! 🎉
    """
    )
    return


if __name__ == "__main__":
    app.run()
