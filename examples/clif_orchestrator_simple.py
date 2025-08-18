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

    # Create wide dataset (NEW FUNCTIONALITY)
    wide_df = co.create_wide_dataset(
        tables_to_load=['vitals', 'labs'],
        category_filters={
            'vitals': ['heart_rate', 'sbp', 'spo2'],
            'labs': ['hemoglobin', 'sodium', 'glucose']
        },
        sample=True  # Use 20 random hospitalizations
    )

    # Convert to hourly aggregation (NEW FUNCTIONALITY)
    hourly_df = co.convert_wide_to_hourly(
        wide_df,
        aggregation_config={
            'mean': ['heart_rate', 'sbp'],
            'max': ['spo2'],
            'boolean': ['hemoglobin', 'sodium']
        }
    )

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
def _(mo):
    mo.md(
        r"""
    ## Wide Dataset Creation (NEW!)

    The ClifOrchestrator now supports creating wide time-series datasets by joining multiple tables with automatic pivoting.
    """
    )
    return


@app.cell
def _(co):
    # Create wide dataset with sample data
    try:
        print("Creating wide dataset...")
        wide_df = co.create_wide_dataset(
            tables_to_load=['vitals', 'labs'],
            category_filters={
                'vitals': ['heart_rate', 'sbp', 'spo2', 'respiratory_rate'],
                'labs': ['hemoglobin', 'sodium', 'glucose', 'creatinine']
            },
            sample=True,  # Use 20 random hospitalizations for demo
            show_progress=True
        )

        print(f"✅ Wide dataset created!")
        print(f"   Shape: {wide_df.shape}")
        print(f"   Columns: {list(wide_df.columns[:10])}...")  # Show first 10 columns
        print(f"   Time range: {wide_df['event_time'].min()} to {wide_df['event_time'].max()}")

    except Exception as e:
        print(f"❌ Error creating wide dataset: {e}")
        print("Note: Make sure vitals and labs tables exist in your data directory")
        wide_df = None

    return (wide_df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Hourly Aggregation (NEW!)

    Convert the wide dataset to hourly aggregation with different aggregation methods.
    """
    )
    return


@app.cell
def _(co, wide_df):
    # Convert to hourly aggregation if wide_df was created successfully
    if wide_df is not None:
        try:
            print("Converting to hourly aggregation...")
            hourly_df = co.convert_wide_to_hourly(
                wide_df,
                aggregation_config={
                    'mean': ['heart_rate', 'sbp', 'respiratory_rate'],
                    'max': ['spo2'],
                    'min': ['sbp'],
                    'boolean': ['hemoglobin', 'sodium', 'glucose'],
                    'first': ['creatinine']
                },
                memory_limit='4GB'
            )

            print(f"✅ Hourly dataset created!")
            print(f"   Shape: {hourly_df.shape}")
            print(f"   Unique hospitalizations: {hourly_df['hospitalization_id'].nunique()}")
            print(f"   Hour range: {hourly_df['nth_hour'].min()} to {hourly_df['nth_hour'].max()}")
            print(f"   Sample columns: {list(hourly_df.columns[:15])}...")

        except Exception as e:
            print(f"❌ Error creating hourly dataset: {e}")
            hourly_df = None
    else:
        print("⏭️ Skipping hourly aggregation (no wide dataset available)")
        hourly_df = None

    return (hourly_df,)


@app.cell
def _(wide_df):
    wide_df
    return


@app.cell
def _(hourly_df):
    hourly_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
