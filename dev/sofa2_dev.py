import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium", sql_output="native")


@app.cell
def _():
    COHORT_SIZE = 100000
    PREFIX = ''
    DEMO_CONFIG_PATH = PREFIX + 'config/demo_data_config.yaml'
    MIMIC_CONFIG_PATH = PREFIX + 'config/config.yaml'
    CONFIG_PATH = MIMIC_CONFIG_PATH
    return COHORT_SIZE, CONFIG_PATH, PREFIX


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(CONFIG_PATH):
    from clifpy import load_data
    labs_rel = load_data('labs', config_path=CONFIG_PATH, return_rel=True)
    crrt_rel = load_data('crrt_therapy', config_path=CONFIG_PATH, return_rel=True)
    assessments_rel = load_data('patient_assessments', config_path=CONFIG_PATH, return_rel=True)
    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    vitals_rel = load_data('vitals', config_path=CONFIG_PATH, return_rel=True)
    meds_rel = load_data('medication_admin_continuous', config_path=CONFIG_PATH, return_rel=True)
    return adt_rel, assessments_rel, crrt_rel, labs_rel, meds_rel, vitals_rel


@app.cell
def _(COHORT_SIZE, PREFIX, adt_rel, mo):
    cohort_df = mo.sql(
        f"""
        FROM adt_rel a
        SEMI JOIN '{PREFIX}.dev/cohort_hosp_ids.csv' c USING (hospitalization_id)
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
def _(meds_rel):
    # TODO: Replace with convert_dose_units_by_med_category_rel() once it outputs pyrelation
    # Will enable automatic unit normalization for heterogeneous vasopressor dose units
    meds_unit_converted = meds_rel
    return (meds_unit_converted,)


@app.cell
def _(mo, vitals_rel):
    map_agg = mo.sql(
        f"""
        FROM vitals_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            hospitalization_id,
            MIN(vital_value) AS map_min
        WHERE vital_category = 'map'
        GROUP BY hospitalization_id
        """
    )
    return (map_agg,)


@app.cell
def _(meds_unit_converted, mo):
    vasopressor_agg = mo.sql(
        f"""
        FROM meds_unit_converted t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            hospitalization_id,
            -- Max doses for primary vasopressors (norepinephrine + epinephrine)
            MAX(med_dose) FILTER(med_category = 'norepinephrine') AS norepi_max,
            MAX(med_dose) FILTER(med_category = 'epinephrine') AS epi_max,
            -- Other vasopressors
            MAX(med_dose) FILTER(med_category = 'dopamine') AS dopamine_max,
            MAX(med_dose) FILTER(med_category = 'dobutamine') AS dobutamine_max,
            MAX(med_dose) FILTER(med_category = 'vasopressin') AS vasopressin_max,
            MAX(med_dose) FILTER(med_category = 'phenylephrine') AS phenylephrine_max,
            -- Flag for any other vasopressor/inotrope
            CASE WHEN MAX(med_dose) FILTER(med_category IN ('dopamine', 'dobutamine', 'vasopressin', 'phenylephrine', 'milrinone', 'angiotensin_ii')) > 0
                 THEN 1 ELSE 0 END AS has_other_vasopressor
        WHERE med_category IN ('norepinephrine', 'epinephrine', 'dopamine', 'dobutamine', 'vasopressin', 'phenylephrine', 'milrinone', 'angiotensin_ii')
            AND med_route_category = 'iv'
            AND mar_action_category NOT IN ('stop', 'verify', 'not_given')
        GROUP BY hospitalization_id
        """
    )
    return (vasopressor_agg,)


@app.cell
def _(cohort_df, map_agg, mo, vasopressor_agg):
    cv_agg = mo.sql(
        f"""
        FROM cohort_df c
        LEFT JOIN map_agg m USING (hospitalization_id)
        LEFT JOIN vasopressor_agg v USING (hospitalization_id)
        SELECT
            c.hospitalization_id,
            -- Derived values for debugging
            COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) AS norepi_epi_sum,
            COALESCE(v.has_other_vasopressor, 0) AS has_other_vasopressor,
            m.map_min,
            -- CV Score
            cardiovascular: CASE
                -- Score 4: High-dose norepi+epi OR medium-dose + other
                WHEN COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) > 0.4 THEN 4
                WHEN COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) > 0.2
                     AND COALESCE(v.has_other_vasopressor, 0) = 1 THEN 4
                -- Score 3: Medium-dose norepi+epi OR low-dose + other
                WHEN COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) > 0.2 THEN 3
                WHEN COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) <= 0.2
                     AND COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) > 0
                     AND COALESCE(v.has_other_vasopressor, 0) = 1 THEN 3
                -- Score 2: Low-dose norepi+epi OR any other vasopressor
                WHEN COALESCE(v.norepi_max, 0) + COALESCE(v.epi_max, 0) > 0 THEN 2
                WHEN COALESCE(v.has_other_vasopressor, 0) = 1 THEN 2
                -- Score 1: MAP < 70, no vasopressors
                WHEN m.map_min < 70 THEN 1
                -- Score 0: MAP >= 70, no vasopressors
                WHEN m.map_min >= 70 THEN 0
                ELSE NULL
            END
        """
    )
    return (cv_agg,)


@app.cell
def _(cohort_df, cv_agg, gcs_agg, labs_agg, mo, rrt_flag):
    sofa_scores = mo.sql(
        f"""
        -- EXPLAIN ANALYZE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id)
        LEFT JOIN rrt_flag r USING (hospitalization_id)
        LEFT JOIN gcs_agg g USING (hospitalization_id)
        LEFT JOIN cv_agg cv USING (hospitalization_id)
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
            -- Cardiovascular
            cv.cardiovascular,
            -- Intermediate values for debugging
            l.platelet_count,
            l.bilirubin_total,
            l.creatinine,
            l.po2_arterial,
            COALESCE(r.has_rrt, 0) AS has_rrt,
            g.gcs_min,
            cv.norepi_epi_sum,
            cv.map_min
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
