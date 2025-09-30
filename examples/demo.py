import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md("""
    # CLIFpy 101

    This tutorial demonstrates how to use CLIFpy to efficiently load, validate, and analyze CLIF data.

    **Note:** All examples below use the CLIF-MIMIC demo data built-into CLIFpy -- but the interface remains identical when working with your own site's local CLIF data.
    """)
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Initialize ClifOrchestrator

    The `ClifOrchestrator` provides a unified interface for managing multiple CLIF tables
    with consistent configuration (timezone, file paths, etc.).
    """
    )
    return


@app.cell
def _(mo):
    from clifpy import ClifOrchestrator

    # Initialize using config file (recommended)
    co = ClifOrchestrator(config_path="config/demo_data_config.yaml")

    mo.show_code()
    return (co,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Load Tables

    Load multiple tables at once. The orchestrator handles file I/O and ensures
    consistent timezone handling across all tables.
    """
    )
    return


@app.cell
def _(co, mo):
    # Load core tables
    co.initialize(tables=['patient', 'hospitalization', 'adt', 'vitals', 'labs'])

    mo.show_code()
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Access Table Data

    Each loaded table is accessible as an attribute with its underlying DataFrame
    available via the `.df` property.
    """
    )
    return


@app.cell
def _(co, mo):
    # Access DataFrames directly
    patient_df = co.patient.df
    vitals_df = co.vitals.df
    labs_df = co.labs.df

    # Example: patient demographics
    patient_df['sex_category'].value_counts()

    mo.show_code()
    return (patient_df, vitals_df, labs_df)


@app.cell
def _(mo, patient_df):
    # Display interactive table
    mo.ui.table(patient_df.head(10))
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Validate Data

    Validation checks schema compliance, required columns, data types, categorical values,
    and clinically reasonable ranges. Results include detailed error reports.
    """
    )
    return


@app.cell
def _(co, mo):
    # Validate all loaded tables
    co.validate_all()

    # Check validation status
    # for table_name in co.get_loaded_tables():
    #     table = getattr(co, table_name)
    #     status = "✅ Valid" if table.isvalid() else f"❌ {len(table.errors)} errors"
    #     print(f"{table_name:20s}: {status}")

    mo.show_code()
    return


@app.cell
def _(co):
    # Inspect errors (if any)
    if co.patient.errors:
        print("Patient table errors:")
        for error in co.patient.errors[:3]:
            print(f"  - {error}")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Use Table-Specific Methods

    Each table class provides specialized methods for common analyses.
    For example, hospitalizations offer mortality rates and length-of-stay statistics.
    """
    )
    return


@app.cell
def _(co, mo):
    # Hospitalization statistics
    mortality_rate = co.hospitalization.get_mortality_rate()
    summary = co.hospitalization.get_summary_stats()

    # Display native outputs
    summary

    mo.show_code()
    return (mortality_rate, summary)


@app.cell
def _(co, mo):
    # Vitals summary - distributions by category
    vital_summary = co.vitals.get_vital_summary_stats()
    vital_summary

    mo.show_code()
    return (vital_summary,)


@app.cell
def _(co, mo):
    # Labs summary - distributions by category
    lab_summary = co.labs.get_lab_category_stats()
    lab_summary

    mo.show_code()
    return (lab_summary,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Create Wide Dataset

    Transform narrow, category-based time-series data (vitals, labs) into wide format
    suitable for machine learning. This automatically joins multiple tables, pivots
    categories into columns, and handles high-performance processing with DuckDB.
    """
    )
    return


@app.cell
def _(co, mo):
    # Check system resources (best practice for large datasets)
    resources = co.get_sys_resource_info()

    mo.show_code()
    return (resources,)


@app.cell
def _(co, mo):
    # Create wide dataset
    co.create_wide_dataset(
        tables_to_load=['vitals', 'labs'],
        category_filters={
            'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'respiratory_rate'],
            'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine']
        },
        sample=True,  # Use 20 random hospitalizations for demo
        show_progress=True
    )

    wide_df = co.wide_df

    mo.show_code()
    return (wide_df,)


@app.cell
def _(mo, wide_df):
    mo.ui.table(wide_df.head(20))
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Convert to Hourly Aggregation

    Aggregate the wide dataset to hourly intervals. Different variables can use different
    aggregation methods: mean for continuous vitals, max/min for extremes, boolean for
    presence/absence of lab tests or medications.
    """
    )
    return


@app.cell
def _(co, mo):
    # Define aggregation methods per variable
    aggregation_config = {
        'mean': ['heart_rate', 'sbp', 'dbp', 'respiratory_rate'],
        'max': ['spo2'],
        'boolean': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine']
    }

    # Convert (automatically uses co.wide_df)
    hourly_df = co.convert_wide_to_hourly(
        aggregation_config=aggregation_config,
        memory_limit='4GB'
    )

    mo.show_code()
    return (hourly_df, aggregation_config)


@app.cell
def _(hourly_df, mo):
    mo.ui.table(hourly_df.head(20))
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Compute SOFA Scores

    Sequential Organ Failure Assessment (SOFA) scores assess organ dysfunction across 6 systems.
    CLIFpy automatically extracts required variables from loaded tables and handles missing data.
    """
    )
    return


@app.cell
def _(co, mo):
    # Compute SOFA scores
    sofa_df = co.compute_sofa_scores(id_name='hospitalization_id')

    # Show statistics
    sofa_df['sofa_total'].describe()

    mo.show_code()
    return (sofa_df,)


@app.cell
def _(mo, sofa_df):
    mo.ui.table(sofa_df)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Standardize Medication Units

    Convert medication doses to preferred units by medication category. This ensures
    consistent units for analysis and comparison. The function automatically pulls weight
    from vitals table when needed for weight-adjusted conversions.
    """
    )
    return


@app.cell
def _(co, mo):
    preferred_units = {
        "propofol": "mcg/kg/min",
        "fentanyl": "mcg/hr",
        "insulin": "u/hr",
        "midazolam": "mg/hr",
        "heparin": "u/min"
    }

    co.convert_dose_units_for_continuous_meds(preferred_units=preferred_units)

    # Access conversion results
    converted_df = co.medication_admin_continuous.df_converted
    conversion_summary = co.medication_admin_continuous.conversion_counts

    # Show conversion summary
    conversion_summary.head(10)

    mo.show_code()
    return (converted_df, conversion_summary)


@app.cell
def _(converted_df, mo):
    # Show converted medication data
    mo.ui.table(
        converted_df[['hospitalization_id', 'admin_dttm', 'med_category',
                      'med_dose', 'med_dose_unit', 'med_dose_converted',
                      'med_dose_unit_converted', '_convert_status']].head(20)
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Calculate Comorbidity Indices

    Compute Charlson and Elixhauser comorbidity indices from diagnosis codes.
    These indices quantify patient comorbidity burden and are commonly used for
    risk adjustment in clinical research.
    """
    )
    return


@app.cell
def _(co, mo):
    from clifpy.utils import calculate_cci, calculate_elix

    co.load_table('hospital_diagnosis')

    # Charlson Comorbidity Index
    cci_df = calculate_cci(co.hospital_diagnosis, hierarchy=True)

    # Elixhauser Comorbidity Index
    elix_df = calculate_elix(co.hospital_diagnosis, hierarchy=True)

    # Show statistics
    cci_df['cci_score'].describe()

    mo.show_code()
    return (cci_df, elix_df, calculate_cci, calculate_elix)


@app.cell
def _(cci_df, mo):
    mo.ui.table(cci_df.head(10))
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Handle Outliers

    Identify and remove physiologically implausible values from vitals and labs using
    CLIF-defined clinical ranges. Outliers are converted to NaN to preserve data structure.
    """
    )
    return


@app.cell
def _(co, mo):
    from clifpy.utils import apply_outlier_handling

    # Apply outlier handling
    apply_outlier_handling(co.vitals)

    mo.show_code()
    return (apply_outlier_handling,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Using Your Own Data

    ```python
    # Method 1: With config file (recommended)
    co = ClifOrchestrator(config_path='path/to/config.yaml')

    # Method 2: Direct parameters
    co = ClifOrchestrator(
        data_directory='/path/to/clif/data',
        filetype='parquet',
        timezone='US/Eastern',
        output_directory='/path/to/outputs'
    )

    co.initialize(tables=['patient', 'hospitalization', 'vitals', 'labs'])
    co.validate_all()
    ```

    **Resources:**
    - [User Guide](https://clif-consortium.github.io/pyCLIF/)
    - [GitHub](https://github.com/clif-consortium/pyCLIF)
    """
    )
    return


if __name__ == "__main__":
    app.run()
