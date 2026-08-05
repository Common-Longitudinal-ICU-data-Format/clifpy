import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium", sql_output="native")

with app.setup:
    import os
    os.environ['POLARS_IMPORT_INTERVAL_AS_STRUCT'] = '1'

    import marimo as mo
    import polars as pl
    import altair as alt

    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    COHORT_SIZE = 10000
    CONFIG_PATH = 'config/mimic_config.yaml'


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # SOFA-2 Validation

    Computes SOFA-2 scores for the **first 24 hours** of ICU admission and visualizes:

    - Distribution of total SOFA-2 scores (0–24)

    - In-hospital mortality rate by SOFA-2 score
    """)
    return


@app.cell
def _():
    from clifpy import load_data

    adt_lf = (
        load_data('adt', config_path=CONFIG_PATH,
                  filters={'location_category': 'icu'},
                  columns=['hospitalization_id', 'in_dttm'],
                  return_rel=True)
        .pl().lazy()
    )
    hosp_lf = (
        load_data('hospitalization', config_path=CONFIG_PATH,
                  columns=['hospitalization_id', 'discharge_category'],
                  return_rel=True)
        .pl().lazy()
    )
    return adt_lf, hosp_lf


@app.cell
def _(adt_lf):
    cohort_df = (
        adt_lf
        .sort('in_dttm')
        .group_by('hospitalization_id')
        .first()
        .select(
            'hospitalization_id',
            pl.col('in_dttm').alias('start_dttm'),
            (pl.col('in_dttm') + pl.duration(hours=24)).alias('end_dttm'),
        )
        .collect()
        .head(COHORT_SIZE)
        #.to_pandas()
    )
    return (cohort_df,)


@app.cell
def _(cohort_df):
    from clifpy.utils.sofa2 import calculate_sofa2

    sofa2_pd = calculate_sofa2(
        cohort_df,
        clif_config_path=CONFIG_PATH,
        return_rel=False,
    )
    return (sofa2_pd,)


@app.cell
def _(sofa2_pd):
    sofa2_results = pl.from_pandas(sofa2_pd)
    return (sofa2_results,)


@app.cell
def _(sofa2_results):
    sofa2_results
    return


@app.cell(hide_code=True)
def _(sofa2_results):
    n = sofa2_results.height
    mean_score = sofa2_results['sofa2_total'].mean()
    median_score = sofa2_results['sofa2_total'].median()
    mo.md(f"""
    ## Cohort Summary

    - **N (ICU admissions)**: {n:,}

    - **Mean SOFA-2 total**: {mean_score:.1f}

    - **Median SOFA-2 total**: {median_score:.0f}
    """)
    return


@app.cell
def _(sofa2_results):
    hist_chart = (
        alt.Chart(sofa2_results.select('sofa2_total'))
        .mark_bar()
        .encode(
            x=alt.X('sofa2_total:O', title='SOFA-2 Total Score'),
            y=alt.Y('count():Q', title='Number of ICU Admissions'),
            tooltip=[
                alt.Tooltip('sofa2_total:O', title='Score'),
                alt.Tooltip('count():Q', title='Count', format=','),
            ],
        )
        .properties(
            title='Distribution of SOFA-2 Total Scores (First 24h of ICU)',
            width='container',
            height=400,
        )
    )
    mo.ui.altair_chart(hist_chart)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## In-Hospital Mortality by SOFA-2 Score
    """)
    return


@app.cell
def _(hosp_lf, sofa2_results):
    hosp_mortality = (
        sofa2_results.lazy()
        .select('hospitalization_id', 'sofa2_total')
        .join(hosp_lf, on='hospitalization_id', how='left')
        .with_columns(
            (pl.col('discharge_category') == 'Expired').cast(pl.Int8).alias('expired')
        )
        .collect()
    )
    return (hosp_mortality,)


@app.cell
def _(hosp_mortality):
    hosp_mortality_by_score = (
        hosp_mortality
        .group_by('sofa2_total')
        .agg(
            pl.col('expired').sum().alias('deaths'),
            pl.col('expired').count().alias('n'),
            (pl.col('expired').mean() * 100).alias('mortality_rate'),
        )
        .sort('sofa2_total')
    )
    return (hosp_mortality_by_score,)


@app.cell
def _(hosp_mortality_by_score):
    hosp_mortality_by_score
    return


@app.cell
def _(hosp_mortality_by_score):
    mortality_chart = (
        alt.Chart(hosp_mortality_by_score)
        .mark_bar()
        .encode(
            x=alt.X('sofa2_total:O', title='SOFA-2 Total Score'),
            y=alt.Y('mortality_rate:Q', title='In-Hospital Mortality Rate (%)'),
            tooltip=[
                alt.Tooltip('sofa2_total:O', title='Score'),
                alt.Tooltip('mortality_rate:Q', title='Mortality %', format='.1f'),
                alt.Tooltip('deaths:Q', title='Deaths', format=','),
                alt.Tooltip('n:Q', title='Total', format=','),
            ],
        )
        .properties(
            title='In-Hospital Mortality Rate by SOFA-2 Score (First 24h of ICU)',
            width='container',
            height=400,
        )
    )
    mo.ui.altair_chart(mortality_chart)
    return


if __name__ == "__main__":
    app.run()
