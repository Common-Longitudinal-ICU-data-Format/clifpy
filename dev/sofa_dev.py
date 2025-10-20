import marimo

__generated_with = "0.16.1"
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Dev for LOCF""")
    return


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
def _():
    from clifpy.clif_orchestrator import ClifOrchestrator

    # Initialize orchestrator
    co = ClifOrchestrator(config_path='config/demo_data_config.yaml', timezone='UTC')

    preferred_units_for_sofa = {
        "norepinephrine": "mcg/kg/min",
        "epinephrine": "mcg/kg/min", 
        "dopamine": "mcg/kg/min", 
        "dobutamine": "mcg/kg/min"
    }

    co.convert_dose_units_for_continuous_meds(preferred_units_for_sofa)

    sofa_df = co.compute_sofa_scores(
        id_name='hospitalization_id',
        # cohort_df=cohort_df2
    )
    return ClifOrchestrator, co, preferred_units_for_sofa


@app.cell
def _(co):
    co.wide_df
    return


@app.cell
def _(pd):
    # for testing purpose
    # fio2_set () and map (51-89)
    cohort_df2 = pd.DataFrame({
        'hospitalization_id': ['20044587', '29974575'], 
        'start_time': pd.to_datetime(
            ['2113-08-25 17:43:00 UTC', '2131-03-06 13:28:00 UTC']), 
        'end_time': pd.to_datetime(
            ['2113-08-25 17:45:00 UTC', '2131-03-06 18:00:00 UTC'])})
    return (cohort_df2,)


@app.cell
def _(ClifOrchestrator, preferred_units_for_sofa):
    co2 = ClifOrchestrator(config_path='config/demo_data_config.yaml', timezone='UTC')

    co2.convert_dose_units_for_continuous_meds(preferred_units_for_sofa)

    co2.create_wide_dataset(
        tables_to_load=['vitals', 'respiratory_support'],
        category_filters={
            'vitals':['map'], 
            'respiratory_support': ['fio2_set'],
            'medication_admin_continuous': ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
        }, 
        # cohort_df=cohort_df2,
        return_dataframe=True
    )

    # co.wide_df
    return


@app.cell
def _(co, cohort_df2):
    co.compute_sofa_scores(
        id_name='hospitalization_id',
        cohort_df=cohort_df2
    )
    return


@app.cell
def _(co, cohort_df, sofa):
    wide_df_raw = co.create_wide_dataset(
        tables_to_load=['vitals', 'labs', 'patient_assessments', 'medication_admin_continuous', 'respiratory_support'],
        category_filters=sofa.REQUIRED_SOFA_CATEGORIES_BY_TABLE, 
        cohort_df=cohort_df,
        return_dataframe=True
    )
    return (wide_df_raw,)


@app.cell
def _(pd):
    _cohort_df = pd.DataFrame({
        'hospitalization_id': ['23559586', '20626031'], 
        'start_time': pd.to_datetime(['2137-01-01 14:29:00+00:00', '2132-12-14 21:00:00+00:00']), 
        'end_time': pd.to_datetime(['2137-08-25 14:00:00+00:00', '2132-12-15 09:00:00+00:00'])})
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(sofa, wide_df_raw):
    wide_df = sofa._impute_pao2_from_spo2(wide_df=wide_df_raw)
    return


@app.cell
def _():
    # worst = sofa._agg_extremal_values_by_id(
    #     wide_df=wide_df,
    #     extremal_type='worst',
    #     id_name = 'hospitalization_id'
    # )
    return


@app.cell
def _(mo, worst):
    mo.ui.table(worst)
    return


@app.cell
def _(sofa, worst):
    sofa._compute_sofa_from_extremal_values(
        extremal_df=worst,
        id_name = 'hospitalization_id'
    )
    return


@app.cell
def _(co):
    co.encounter_mapping
    return


@app.cell
def _(co, mo):
    mo.ui.table(co.wide_df)
    return


@app.cell
def _(co):
    co.compute_sofa_scores(id_name='hospitalization_id', fill_na_scores_with_zero=False)
    return


@app.cell
def _(co):
    co.sofa_df
    return


if __name__ == "__main__":
    app.run()
