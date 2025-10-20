import marimo

__generated_with = "0.16.1"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# SOFA Score Computation Demo""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Compute Sequential Organ Failure Assessment (SOFA) scores

    SOFA scores assess organ dysfunction across 6 systems: cardiovascular, coagulation, liver, respiratory, CNS, and renal.

    CLIFpy computes SOFA scores from your clinical data tables automatically.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Basic Usage""")
    return


@app.cell
def _():
    from clifpy.clif_orchestrator import ClifOrchestrator

    # Initialize orchestrator
    co = ClifOrchestrator(config_path='config/demo_data_config.yaml')

    preferred_units_for_sofa = {
        "norepinephrine": "mcg/kg/min",
        "epinephrine": "mcg/kg/min", 
        "dopamine": "mcg/kg/min", 
        "dobutamine": "mcg/kg/min"
    }

    co.convert_dose_units_for_continuous_meds(preferred_units_for_sofa)

    # Compute SOFA scores (default: by encounter_block)
    # sofa_scores = co.compute_sofa_scores(id_name='hospitalization_id')

    # mo.show_code()
    return (co,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""The function automatically loads required tables and computes SOFA scores. Results are stored in `co.sofa_df`.""")
    return


@app.cell
def _(co, mo):
    mo.ui.table(co.sofa_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Encounter Block vs Hospitalization ID""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    By default, SOFA scores are computed per **encounter_block**, which groups related hospitalizations:

    - Multiple hospitalizations within 6 hours are treated as one encounter
    - SOFA represents the worst values across the entire encounter
    - Useful for readmissions and transfers
    """
    )
    return


@app.cell
def _(co, mo):
    # Show encounter mapping
    if co.encounter_mapping is not None:
        mo.ui.table(co.encounter_mapping.head(10))
    else:
        mo.md("No encounter mapping available (single hospitalizations)")
    return


@app.cell
def _(co, mo):
    # Alternative: compute per individual hospitalization
    sofa_by_hosp = co.compute_sofa_scores(id_name='hospitalization_id')

    mo.md(f"""
    **Comparison:**

    - By encounter_block: {len(co.sofa_df)} rows

    - By hospitalization_id: {len(sofa_by_hosp)} rows
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Time Filtering with Cohort DataFrame""")
    return


@app.cell
def _(co, mo, pd):
    # Define a cohort with specific time windows
    cohort_df = pd.DataFrame({
        'hospitalization_id': ['23559586', '20626031'],
        'start_time': pd.to_datetime(['2137-01-01 14:29:00+00:00', '2132-12-14 21:00:00+00:00']),
        'end_time': pd.to_datetime(['2137-08-25 14:00:00+00:00', '2132-12-15 09:00:00+00:00'])
    })

    # Compute SOFA for specific time windows
    sofa_filtered = co.compute_sofa_scores(
        cohort_df=cohort_df,
        id_name='hospitalization_id'
    )

    mo.show_code()
    return


@app.cell
def _():
    # mo.ui.table(sofa_filtered)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The computation includes intermediate values:

    - `p_f`: PaO2/FiO2 ratio using measured PaO2

    - `p_f_imputed`: PaO2/FiO2 ratio using SpO2-derived PaO2 (when PaO2 missing)
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Required Data Summary""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    SOFA computation requires these variables:

    **Labs:** creatinine, platelet_count, po2_arterial, bilirubin_total

    **Vitals:** map, spo2

    **Assessments:** gcs_total

    **Medications:** norepinephrine_mcg_kg_min, epinephrine_mcg_kg_min, dopamine_mcg_kg_min, dobutamine_mcg_kg_min

    **Respiratory:** device_category, fio2_set

    ⚠️ **Important:** Medication doses must be pre-converted to standard units (mcg/kg/min).
    """
    )
    return


@app.cell(hide_code=True)
def _import():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    # Add parent directory to path for development
    sys.path.append(str(Path().absolute().parent))
    return mo, pd


@app.cell
def _(co):
    co.wide_df
    return


if __name__ == "__main__":
    app.run()
