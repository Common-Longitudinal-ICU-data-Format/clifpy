import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium", sql_output="native")


@app.cell
def _():
    COHORT_SIZE = 100000
    DEMO_CONFIG_PATH = 'config/demo_data_config.yaml'
    MIMIC_CONFIG_PATH = 'config/config.yaml'
    CONFIG_PATH = MIMIC_CONFIG_PATH
    return COHORT_SIZE, CONFIG_PATH


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import duckdb
    return (mo,)


@app.cell
def _(CONFIG_PATH):
    from clifpy import load_data
    labs_rel = load_data('labs', config_path=CONFIG_PATH, return_rel=True)
    crrt_rel = load_data('crrt_therapy', config_path=CONFIG_PATH, return_rel=True)
    assessments_rel = load_data('patient_assessments', config_path=CONFIG_PATH, return_rel=True)
    hosp_rel = load_data('hospitalization', config_path=CONFIG_PATH, return_rel=True)
    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    return adt_rel, assessments_rel, crrt_rel, labs_rel


@app.cell
def _(COHORT_SIZE, adt_rel, mo):
    cohort_df = mo.sql(
        f"""
        FROM adt_rel a
        SEMI JOIN '.dev/cohort_hosp_ids.csv' c USING (hospitalization_id)
        SELECT hospitalization_id, in_dttm as start_dttm, in_dttm + INTERVAL '24 hours' as end_dttm
        WHERE location_category = 'icu'
        LIMIT {COHORT_SIZE}
        """
    )
    return (cohort_df,)


@app.cell
def _(labs_rel, mo):
    labs_agg = mo.sql(
        f"""
        FROM
            labs_rel t
            SEMI JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
            AND t.lab_result_dttm >= c.start_dttm
            AND t.lab_result_dttm <= c.end_dttm
        SELECT
            hospitalization_id,
            -- MIN aggregations (worse = lower)
            MIN(lab_value_numeric) FILTER(lab_category = 'platelet_count') AS platelet_count,
            MIN(lab_value_numeric) FILTER(lab_category = 'po2_arterial') AS po2_arterial,
            -- MAX aggregations (worse = higher)
            MAX(lab_value_numeric) FILTER(lab_category = 'creatinine') AS creatinine,
            MAX(lab_value_numeric) FILTER(lab_category = 'bilirubin_total') AS bilirubin_total,
        WHERE t.lab_category IN (
                'platelet_count',
                'creatinine',
                'bilirubin_total',
                'po2_arterial'
            )
        GROUP BY
            hospitalization_id
        """
    )
    return (labs_agg,)


@app.cell
def _(crrt_rel, mo):
    # Detect if patient received RRT during the time window
    rrt_flag = mo.sql(
        f"""
        FROM crrt_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT hospitalization_id, 1 AS has_rrt
        """
    )
    return (rrt_flag,)


@app.cell
def _(assessments_rel, mo):
    gcs_agg = mo.sql(
        f"""
        FROM assessments_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            hospitalization_id,
            MIN(numerical_value) AS gcs_min
        WHERE assessment_category = 'gcs_total'
        GROUP BY hospitalization_id
        """
    )
    return (gcs_agg,)


@app.cell
def _(cohort_df, gcs_agg, labs_agg, mo, rrt_flag):
    sofa_scores = mo.sql(
        f"""
        -- EXPLAIN ANALYSE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id)
        LEFT JOIN rrt_flag r USING (hospitalization_id)
        LEFT JOIN gcs_agg g USING (hospitalization_id)
        SELECT
            c.hospitalization_id,
            c.start_dttm,
            c.end_dttm,
            -- Hemostasis (platelets in 10^3/uL)
            hemostasis: CASE
                WHEN l.platelet_count > 150 THEN 0
                WHEN l.platelet_count <= 150 THEN 1
                WHEN l.platelet_count <= 100 THEN 2
                WHEN l.platelet_count <= 80 THEN 3
                WHEN l.platelet_count <= 50 THEN 4
                ELSE NULL END,
            -- Liver (bilirubin in mg/dL)
            liver: CASE
                WHEN l.bilirubin_total <= 1.2 THEN 0
                WHEN l.bilirubin_total <= 3.0 THEN 1
                WHEN l.bilirubin_total <= 6.0 THEN 2
                WHEN l.bilirubin_total <= 12.0 THEN 3
                WHEN l.bilirubin_total > 12.0 THEN 4
                ELSE NULL END,
            -- Kidney (creatinine in mg/dL, with RRT override)
            kidney: CASE
                WHEN r.has_rrt = 1 THEN 4
                WHEN l.creatinine > 3.50 THEN 3
                WHEN l.creatinine <= 3.50 THEN 2
                WHEN l.creatinine <= 2.0 THEN 1
                WHEN l.creatinine <= 1.20 THEN 0
                ELSE NULL END,
            -- Brain (GCS)
            brain: CASE
                WHEN g.gcs_min >= 15 THEN 0
                WHEN g.gcs_min >= 13 THEN 1
                WHEN g.gcs_min >= 9 THEN 2
                WHEN g.gcs_min >= 6 THEN 3
                WHEN g.gcs_min >= 3 THEN 4
                ELSE NULL END,
            -- Intermediate values for debugging
            l.platelet_count,
            l.bilirubin_total,
            l.creatinine,
            l.po2_arterial,
            COALESCE(r.has_rrt, 0) AS has_rrt,
            g.gcs_min
        """
    )
    return (sofa_scores,)


@app.cell
def _(sofa_scores):
    sofa_scores.df()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
