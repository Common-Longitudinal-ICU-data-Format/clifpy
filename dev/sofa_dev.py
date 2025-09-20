import marimo

__generated_with = "0.16.0"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Init""")
    return


@app.cell
def _():
    import pandas as pd
    from pathlib import Path
    import duckdb

    # # Import individual table classes
    # from clifpy.tables.vitals import Vitals
    # from clifpy.tables.labs import Labs
    # from clifpy.tables.medication_admin_continuous import MedicationAdminContinuous

    from clifpy.utils import sofa
    return pd, sofa


@app.cell
def _(pd):
    cohort_df = pd.DataFrame({
        'hospitalization_id': ['23559586', '20626031'], 
        'start_time': pd.to_datetime(
            ['2132-12-15 14:29:00+00:00', '2132-12-14 21:00:00+00:00']), 
        'end_time': pd.to_datetime(
            ['2137-08-25 14:00:00+00:00', '2137-09-02 09:00:00+00:00'])})
    return (cohort_df,)


@app.cell
def _(cohort_df, sofa):
    from clifpy.clif_orchestrator import ClifOrchestrator
    co = ClifOrchestrator(config_path = 'config/config.yaml')

    wide_df = co.create_wide_dataset(
        tables_to_load=['vitals', 'labs', 'patient_assessments', 'medication_admin_continuous', 'respiratory_support'],
        category_filters=sofa.REQUIRED_SOFA_CATEGORIES_BY_TABLE, 
        cohort_df=cohort_df
    )
    return (wide_df,)


@app.cell
def _(pd):
    _cohort_df = pd.DataFrame({
        'hospitalization_id': ['23559586', '20626031'], 
        'start_time': pd.to_datetime(['2137-01-01 14:29:00+00:00', '2132-12-14 21:00:00+00:00']), 
        'end_time': pd.to_datetime(['2137-08-25 14:00:00+00:00', '2132-12-15 09:00:00+00:00'])})
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(sofa, wide_df):
    sofa.agg_extremal_values_by_id(
        wide_df=wide_df,
        extremal_type='latest',
        id_name = 'hospitalization_id'
    )
    return


@app.cell
def _(wide_df):
    wide_df
    return


if __name__ == "__main__":
    app.run()
