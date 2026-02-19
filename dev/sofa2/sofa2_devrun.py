import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium", sql_output="pandas")

with app.setup:
    import marimo as mo

    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    COHORT_SIZE = 20000
    PREFIX = ''
    DEMO_CONFIG_PATH = PREFIX + 'config/demo_data_config.yaml'
    CONFIG_PATH = PREFIX + 'config/mimic_config.yaml'


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # SOFA-2 Dev Runner

    Interactive notebook to test `calculate_sofa2` and `calculate_sofa2_daily` from `clifpy.utils.sofa2`.
    """)
    return


@app.cell
def _(load_data):
    ecmo_rel = load_data('ecmo_mcs', config_path=CONFIG_PATH, return_rel=True)
    return


@app.cell
def _():
    import pandas as pd
    pd.read_parquet('/Users/wliao0504/code/clif/ucmc-clif-data/2.1.0/clif_ecmo_mcs.parquet')
    return


@app.cell
def _():
    from clifpy import load_data
    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    return adt_rel, load_data


@app.cell
def cohort_c(adt_rel):
    cohort_df = mo.sql(
        f"""
        -- Create cohort: ICU admissions with 24h windows
        FROM adt_rel a
        --SEMI JOIN '{PREFIX}.dev/cohort_hosp_ids.csv' c USING (hospitalization_id)
        SELECT hospitalization_id, in_dttm as start_dttm, in_dttm + INTERVAL '24 hours' as end_dttm
        WHERE location_category = 'icu'
        -- LIMIT {COHORT_SIZE}
        """
    )
    return (cohort_df,)


@app.cell
def _(adt_rel):
    daily_cohort_df = mo.sql(
        f"""
        -- Create cohort: ICU admissions with 24h windows
        FROM adt_rel a
        SEMI JOIN '{PREFIX}.dev/cohort_hosp_ids.csv' c USING (hospitalization_id)
        SELECT hospitalization_id, in_dttm as start_dttm, out_dttm as end_dttm
        WHERE location_category = 'icu'
        -- LIMIT {COHORT_SIZE}
        """
    )
    return (daily_cohort_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## `calculate_sofa2`

    Calculate SOFA-2 scores for arbitrary time windows with optional QA intermediates.
    """)
    return


@app.cell
def _(cohort_df):
    from clifpy.utils.sofa2 import calculate_sofa2

    sofa2_results = calculate_sofa2(
        cohort_df,
        clif_config_path=CONFIG_PATH,
        return_rel=False,
        dev=False,
    )
    return (sofa2_results,)


@app.cell
def _(sofa2_results):
    sofa2_results
    return


@app.cell
def _(sofa2_results):
    _df = mo.sql(
        f"""
        FROM sofa2_results
        -- select rows where the platelet_dttm_offset is negative
        WHERE pao2_dttm_offset < INTERVAL '0 hour'
            OR spo2_dttm_offset < INTERVAL '0 hour'
            OR fio2_dttm_offset < INTERVAL '0 hour'
            OR bilirubin_dttm_offset < INTERVAL '0 hour'
            OR creatinine_dttm_offset < INTERVAL '0 hour'
            OR potassium_dttm_offset < INTERVAL '0 hour'
            OR ph_dttm_offset < INTERVAL '0 hour'
            OR bicarbonate_dttm_offset < INTERVAL '0 hour'
            OR platelet_dttm_offset < INTERVAL '0 hour'
        	OR norepi_epi_maxsum_dttm_offset < INTERVAL '0 hour'
            OR dopa_max_dttm_offset < INTERVAL '0 hour'
            OR gcs_min_dttm_offset < INTERVAL '0 hour'
        """
    )
    return


@app.cell
def _(sofa2_results):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.hist(sofa2_results['sofa2_total'], bins=range(0, 26), edgecolor='black', weights=[1/len(sofa2_results)] * len(sofa2_results))
    ax.set_xlabel('SOFA Total Score')
    ax.set_ylabel('Proportion')
    ax.set_title('Distribution of SOFA-2 Total Scores')
    ax.set_xticks(range(0, 25))
    fig
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## `calculate_sofa2_daily`

    Calculate daily SOFA-2 scores with carry-forward logic for missing data.

    - Day 1 missing → score as 0

    - Day 2+ missing → forward-fill from last observation
    """)
    return


@app.cell
def _(daily_cohort_df):
    from clifpy.utils.sofa2 import calculate_sofa2_daily

    sofa2_daily_results = calculate_sofa2_daily(
        daily_cohort_df,
        clif_config_path=CONFIG_PATH,
        return_rel=False,
    )
    return (sofa2_daily_results,)


@app.cell
def _(sofa2_daily_results):
    sofa2_daily_results
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## QA Intermediates

    Explore intermediate relations from `calculate_sofa2(dev=True)` for debugging.
    """)
    return


@app.cell
def _(sofa2_intermediates):
    # List available intermediate relations
    list(sofa2_intermediates.keys())
    return


@app.cell
def _(sofa2_intermediates):
    # Example: inspect a specific intermediate (uncomment to use)
    # sofa2_intermediates['brain_score'].df()
    sofa2_intermediates
    return


if __name__ == "__main__":
    app.run()
