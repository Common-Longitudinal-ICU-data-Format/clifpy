import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import duckdb
    return mo, pd


@app.cell
def _():
    from clifpy import load_data

    labs_rel = load_data('labs', config_path='config/demo_data_config.yaml', return_rel=True)
    return (labs_rel,)


@app.cell
def _():
    # creatinine 
    # platelet_count
    return


@app.cell
def _(pd):
    cohort_df = pd.DataFrame({
        'hospitalization_id': ['23559586', '20626031'], 
        'start_dttm': pd.to_datetime(
            ['2132-12-15 14:29:00+00:00', '2132-12-14 21:00:00+00:00']), 
        'end_dttm': pd.to_datetime(
            ['2137-08-25 14:00:00+00:00', '2137-09-02 09:00:00+00:00'])})

    cohort_df
    return


@app.cell
def _(labs_rel, mo):
    labs_agg = mo.sql(
        f"""
        --EXPLAIN ANALYZE
        FROM
            labs_rel t
            SEMI JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
            AND t.lab_result_dttm >= c.start_dttm
            AND t.lab_result_dttm <= c.end_dttm
            AND t.lab_category IN (
                'platelet_count',
                'creatinine',
                'bilirubin_total',
                'po2_arterial'
            )
        SELECT
            hospitalization_id,
            -- MIN aggregations (worse = lower)
            MIN(lab_value_numeric) FILTER(lab_category = 'platelet_count') AS platelet_count,
            MIN(lab_value_numeric) FILTER(lab_category = 'po2_arterial') AS po2_arterial,
            -- MAX aggregations (worse = higher)
            MAX(lab_value_numeric) FILTER(lab_category = 'creatinine') AS creatinine,
            MAX(lab_value_numeric) FILTER(lab_category = 'bilirubin_total') AS bilirubin_total,
        GROUP BY
            hospitalization_id
        """
    )
    return (labs_agg,)


@app.cell
def _(labs_agg, mo):
    df = mo.sql(
        f"""
        -- EXPLAIN ANALYSE
        FROM labs_agg
        SELECT hospitalization_id
        	, hemostasis: CASE -- in 10^3/uL
                WHEN platelet_count > 150 THEN 0
                WHEN platelet_count <= 150 THEN 1
                WHEN platelet_count <= 100 THEN 2
                WHEN platelet_count <= 80 THEN 3
                WHEN platelet_count <= 50 THEN 4
                ELSE NULL END
            , liver: CASE -- in mg/dL
                WHEN bilirubin_total <= 1.2 THEN 0
                WHEN bilirubin_total <= 3.0 THEN 1
                WHEN bilirubin_total <= 6.0 THEN 2
                WHEN bilirubin_total <= 12.0 THEN 3
                WHEN bilirubin_total > 12.0 THEN 4
                ELSE NULL END
        """
    )
    return (df,)


@app.cell
def _(df):
    df.df()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
