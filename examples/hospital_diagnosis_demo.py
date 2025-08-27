import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import pandas as pd
    from pathlib import Path

    # Add repository root to sys.path so top-level clifpy is importable when running uninstalled
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    header = mo.md(
        """
        # Demo: Hospital Diagnosis Table

        This notebook demonstrates the `HospitalDiagnosis` table class and its key functions.
        The hospital_diagnosis table contains ICD diagnosis codes associated with hospitalizations,
        including diagnosis type (Principal/Secondary), code format (ICD-9/ICD-10), and present on admission indicators.
        """
    )
    return mo, pd, project_root


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
        ## Configuration
        - **Data directory**: `{DATA_DIR}`  
        - **Output directory**: `{OUTPUT_DIR}`  
        - **Filetype**: `{FILETYPE}`  
        - **Timezone**: `{TIMEZONE}`
        """
    )
    print("Setup complete")
    return OUTPUT_DIR, TIMEZONE


@app.cell
def _(OUTPUT_DIR, TIMEZONE, mo, pd):
    # Import HospitalDiagnosis from top-level clifpy exports
    from clifpy import HospitalDiagnosis

    mo.md("## Load Hospital Diagnosis Table")

    # Since we don't have actual hospital_diagnosis data files, we'll create sample data
    # In real usage, you would load from files using:
    # hospital_diagnosis_table = HospitalDiagnosis.from_file(
    #     data_directory=str(DATA_DIR),
    #     filetype=FILETYPE,
    #     timezone=TIMEZONE,
    #     output_directory=str(OUTPUT_DIR),
    # )

    # Create sample hospital diagnosis data
    sample_data = pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H001', 'H002', 'H002', 'H003', 'H003', 'H003', 'H004', 'H005'],
        'diagnosis_code': ['I50.9', 'J44.1', 'E11.9', 'I25.10', 'I10', 'N18.6', 'E78.5', 'I48.91', 'J18.9', 'I21.9'],
        'diagnosis_code_format': ['ICD-10-CM', 'ICD-10-CM', 'ICD-10-CM', 'ICD-10-CM', 'ICD-10-CM', 
                                'ICD-10-CM', 'ICD-10-CM', 'ICD-10-CM', 'ICD-10-CM', 'ICD-9-CM'],
        'diagnosis_name': [
            'Heart failure, unspecified',
            'Chronic obstructive pulmonary disease with acute exacerbation',
            'Type 2 diabetes mellitus without complications',
            'Atherosclerotic heart disease of native coronary artery without angina pectoris',
            'Essential hypertension',
            'End stage renal disease',
            'Hyperlipidemia, unspecified',
            'Unspecified atrial fibrillation',
            'Pneumonia, unspecified organism',
            'Acute myocardial infarction, unspecified'
        ],
        'diagnosis_type': ['Principal', 'Secondary', 'Secondary', 'Principal', 'Secondary', 
                          'Principal', 'Secondary', 'Secondary', 'Principal', 'Principal'],
        'present_on_admission': ['Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'No', 'Yes', 'Yes', 'No', 'Yes']
    })

    # Initialize the table with sample data
    hospital_diagnosis_table = HospitalDiagnosis(
        data_directory=".",
        filetype="parquet",
        timezone=TIMEZONE,
        output_directory=str(OUTPUT_DIR),
        data=sample_data
    )

    mo.md(f"**Sample data loaded**: {len(sample_data)} diagnosis records")
    return (hospital_diagnosis_table,)


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 1: Get Diagnosis Types")

    diagnosis_types = hospital_diagnosis_table.get_diagnosis_types()

    mo.md(f"**Available diagnosis types**: {', '.join(diagnosis_types)}")
    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 2: Get Code Formats")

    code_formats = hospital_diagnosis_table.get_code_formats()

    mo.md(f"**Available code formats**: {', '.join(code_formats)}")
    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 3: Filter by Diagnosis Type")

    principal_diagnoses = hospital_diagnosis_table.filter_by_diagnosis_type('Principal')

    filter_display_text = f"**Principal diagnoses found**: {len(principal_diagnoses)} records\n\n"
    if not principal_diagnoses.empty:
        filter_display_text += "**Sample principal diagnoses**:\n"
        for _, diag_row in principal_diagnoses[['hospitalization_id', 'diagnosis_code', 'diagnosis_name']].head(3).iterrows():
            filter_display_text += f"- {diag_row['hospitalization_id']}: {diag_row['diagnosis_code']} - {diag_row['diagnosis_name']}\n"

    mo.md(filter_display_text)
    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 4: Present on Admission Statistics")

    poa_stats = hospital_diagnosis_table.get_poa_statistics()

    if poa_stats:
        poa_display_text = f"**Total diagnoses**: {poa_stats['total_diagnoses']}\n\n**Present on Admission Breakdown**:\n"
        for status_poa_stats, count_poa_stats in poa_stats['poa_counts'].items():
            percentage_poa_stats = poa_stats['poa_percentages'][status_poa_stats]
            poa_display_text += f"- **{status_poa_stats}**: {count_poa_stats} ({percentage_poa_stats}%)\n"
    mo.md(poa_display_text)

    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 5: Diagnoses per Hospitalization")

    diagnoses_per_hosp = hospital_diagnosis_table.get_diagnoses_per_hospitalization()

    if not diagnoses_per_hosp.empty:
        hosp_display_text = "**Diagnosis counts by hospitalization**:\n\n"
        for _, hosp_row in diagnoses_per_hosp.head().iterrows():
            hosp_display_text += f"- **{hosp_row['hospitalization_id']}**: {hosp_row['total_diagnoses']} total ({hosp_row['principal_diagnoses']} principal, {hosp_row['secondary_diagnoses']} secondary)\n"
    mo.md(hosp_display_text)
    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 6: Most Common Diagnoses")

    common_diagnoses = hospital_diagnosis_table.get_most_common_diagnoses(top_n=5)

    if not common_diagnoses.empty:
        common_display_text = "**Top 5 most common diagnoses**:\n\n"
        for i, (_, common_row) in enumerate(common_diagnoses.iterrows(), 1):
            diagnosis_name = common_row.get('diagnosis_name', 'N/A')
            common_display_text += f"{i}. **{common_row['diagnosis_code']}** - {diagnosis_name} ({common_row['count']} occurrences)\n"
    mo.md(common_display_text)
    return


@app.cell
def _(hospital_diagnosis_table, mo):
    mo.md("## Function 7: Summary Statistics")

    summary_stats = hospital_diagnosis_table.get_summary_stats()

    if summary_stats:
        stats_display_text = "**Hospital Diagnosis Summary Statistics**:\n\n"
        stats_display_text += f"- **Total diagnosis records**: {summary_stats.get('total_diagnosis_records', 'N/A')}\n"
        stats_display_text += f"- **Unique hospitalizations**: {summary_stats.get('unique_hospitalizations', 'N/A')}\n"
        stats_display_text += f"- **Unique diagnosis codes**: {summary_stats.get('unique_diagnosis_codes', 'N/A')}\n"

        if 'avg_diagnoses_per_hospitalization' in summary_stats:
            stats_display_text += f"- **Average diagnoses per hospitalization**: {summary_stats['avg_diagnoses_per_hospitalization']}\n"

        if summary_stats.get('diagnosis_type_counts'):
            stats_display_text += "\n**Diagnosis Type Distribution**:\n"
            for dtype, count in summary_stats['diagnosis_type_counts'].items():
                stats_display_text += f"- {dtype}: {count}\n"

        if summary_stats.get('code_format_counts'):
            stats_display_text += "\n**Code Format Distribution**:\n"
            for format_type, count in summary_stats['code_format_counts'].items():
                stats_display_text += f"- {format_type}: {count}\n"
    mo.md(stats_display_text)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Usage Notes

    This notebook demonstrates the core functionality of the `HospitalDiagnosis` table class:

    1. **get_diagnosis_types()** - Returns unique diagnosis types in the dataset
    2. **get_code_formats()** - Returns available ICD code formats  
    3. **filter_by_diagnosis_type()** - Filters records by diagnosis type (Principal, Secondary, Other)
    4. **get_poa_statistics()** - Calculates present on admission statistics
    5. **get_diagnoses_per_hospitalization()** - Shows diagnosis counts per hospitalization
    6. **get_most_common_diagnoses()** - Identifies the most frequent diagnosis codes
    7. **get_summary_stats()** - Provides comprehensive summary statistics

    In production, you would load actual hospital diagnosis data using:
    ```python
    hospital_diagnosis_table = HospitalDiagnosis.from_file(
        data_directory="path/to/data",
        filetype="parquet", 
        timezone="UTC",
        output_directory="path/to/output"
    )
    ```
    """
    )
    return


if __name__ == "__main__":
    app.run()
