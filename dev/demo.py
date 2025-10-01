import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import pandas as pd
    from pathlib import Path

    from clifpy.clif_orchestrator import ClifOrchestrator
    from clifpy.utils.outlier_handler import (
        apply_outlier_handling,
        get_outlier_summary,
    )
    from clifpy.utils.comorbidity import calculate_cci


@app.cell
def _():
    mo.md(
        """
    # CLIFpy Demo Notebook


    """
    )
    return


@app.cell
def _():
    # config_path = Path(__file__).resolve().parents[1] / "config" / "demo_data_config.yaml"

    co = ClifOrchestrator(config_path="config/demo_data_config.yaml")
    co.initialize(
        tables=[
            "patient",
            "hospitalization",
            "adt",
            "labs",
            "vitals",
            "medication_admin_continuous",
            "respiratory_support",
            "patient_assessments",
        ]
    )
    return (co,)


@app.cell
def _(co, config_path):
    loaded = sorted(co.get_loaded_tables())
    mo.md(
        f"""
    ## 1. Start from a Configuration

    I like to begin with the exact config file that ships with the project—no
    mystery constants, just a path. Once you swap this file for your site-specific
    config, the rest of the notebook runs unchanged.

    ```python
    co = ClifOrchestrator(config_path={config_path!r})

    co.initialize(tables={loaded})
    ```

    CLIFpy keeps the timezone (`{co.timezone}`) and data directory consistent across
    every table, so you're never juggling mismatched settings.
    """
    )
    return


@app.cell
def _(co):
    patient_columns = [
        "patient_id",
        "birth_date",
        "sex_category",
        "race_category",
    ]
    patient_preview = co.patient.df[patient_columns].head()
    patient_summary = co.patient.get_summary()
    return patient_preview, patient_summary


@app.cell
def _(patient_preview, patient_summary):
    mo.md(
        """
    ## 2. Explore a Core Table

    Before doing any analysis, I sanity-check the demographics table. A quick
    `head()` plus the built-in summary gives me shape, column types, and missing
    value counts without extra code.
    """
    )
    patient_preview
    patient_summary
    return


@app.cell
def _(co):
    validation_report = {}
    for name in co.get_loaded_tables():
        table = getattr(co, name)
        table.validate()
        validation_report[name] = table.isvalid()
    return (validation_report,)


@app.cell
def _(validation_report):
    mo.md(
        """
    ## 4. Validate Everything Up Front

    Once the orchestrator is loaded, I run `validate()` across the board. It keeps
    surprises out of downstream notebooks and writes detailed reports to the output
    folder automatically.
    """
    )
    validation_report
    return


@app.cell
def _(co):
    stitched = ClifOrchestrator(
        config_path=co.config_path,
        stitch_encounter=True,
        stitch_time_interval=6,
    )
    stitched.initialize(tables=["hospitalization", "adt"])
    encounter_mapping = stitched.get_encounter_mapping().head()
    return (encounter_mapping,)


@app.cell
def _(encounter_mapping):
    mo.md(
        """
    ## 5. Stitch Related Encounters

    When patients bounce between units, the `stitch_encounter` flag gives me a
    single encounter block. Here's the first few rows of the mapping, ready to join
    back to any table.
    """
    )
    encounter_mapping
    return


@app.cell
def _(co):
    resources = co.get_sys_resource_info(print_summary=False)

    co.create_wide_dataset(
        tables_to_load=["vitals", "labs"],
        category_filters={
            "vitals": ["heart_rate", "sbp", "spo2"],
            "labs": ["hemoglobin", "sodium"],
        },
        sample=True,
        show_progress=False,
    )
    wide_preview = co.wide_df.head()

    hourly_df = co.convert_wide_to_hourly(
        co.wide_df,
        aggregation_config={
            "mean": ["heart_rate", "sbp"],
            "max": ["spo2"],
            "first": ["hemoglobin", "sodium"],
        },
        show_progress=False,
    )
    hourly_preview = hourly_df.head()
    return hourly_preview, resources, wide_preview


@app.cell
def _(hourly_preview, resources, wide_preview):
    mo.md(
        """
    ## 6. Build a Wide Dataset Safely

    I like to check available memory before pivoting Vitals and Labs-the helper does
    it for me. Sampling keeps demos snappy, but drop the flag once you're ready for
    the full cohort.
    """
    )
    mo.md(
        f"Available RAM: {resources['memory_available_gb']:.1f} GB — recommended threads: {resources['max_recommended_threads']}"
    )
    wide_preview
    hourly_preview
    return


@app.cell
def _(co):
    outlier_report = get_outlier_summary(co.vitals)
    apply_outlier_handling(co.vitals)
    cleaned_fraction = 1 - co.vitals.df["vital_value"].isna().mean()
    return cleaned_fraction, outlier_report


@app.cell
def _(cleaned_fraction, outlier_report):
    mo.md(
        """
    ## 7. Clean Physiologic Outliers

    `get_outlier_summary` lets me review the impact before mutating the table. After
    `apply_outlier_handling`, I confirm how much signal remains.
    """
    )
    outlier_report
    mo.md(f"Non-null vital values after cleaning: {cleaned_fraction:.2%}")
    return


@app.cell
def _(co):
    preferred_units = {
        "norepinephrine": "mcg/kg/min",
        "epinephrine": "mcg/kg/min",
        "dopamine": "mcg/kg/min",
        "dobutamine": "mcg/kg/min",
    }
    converted_df, counts_df = co.convert_dose_units_for_continuous_meds(
        preferred_units=preferred_units,
        save_to_table=False,
        show_intermediate=False,
    )
    return


@app.cell
def _(converted_head, counts_head):
    mo.md(
        """
    ## 8. Normalize Infusion Doses

    Weight-based units are the default for SOFA, so I run the converter early and
    glance at both the converted rows and the frequency table to spot odd units.
    """
    )
    converted_head
    counts_head
    return


@app.cell
def _(co):
    co.load_table("hospital_diagnosis")
    cci_scores = calculate_cci(co.hospital_diagnosis)
    return


@app.cell
def _(cci_scores_head):
    mo.md(
        """
    ## 9. Compute Comorbidity Scores

    The Charlson index is just one helper away once the hospital diagnosis table is
    loaded. The same function works on your production data with the configuration
    swap we set up earlier.
    """
    )
    cci_scores_head
    return


@app.cell
def _(co):
    preferred_units = {
        "norepinephrine": "mcg/kg/min",
        "epinephrine": "mcg/kg/min",
        "dopamine": "mcg/kg/min",
        "dobutamine": "mcg/kg/min",
    }
    co.convert_dose_units_for_continuous_meds(preferred_units=preferred_units)
    sofa_scores = co.compute_sofa_scores(id_name="hospitalization_id")
    return


@app.cell
def _(sofa_head):
    mo.md(
        """
    ## 10. Summarize Organ Failure (SOFA)

    With doses converted, `compute_sofa_scores` fills out the full organ profile per
    hospitalization. Here's a peek at the results.
    """
    )
    sofa_head
    return


@app.cell
def _():
    mo.md(
        """
    ## 11. Next Steps

    You're ready to point these cells at your site configuration. Swap the config
    path in the first code block, rerun, and start layering on your own analyses.
    """
    )
    return


if __name__ == "__main__":
    app.run()
