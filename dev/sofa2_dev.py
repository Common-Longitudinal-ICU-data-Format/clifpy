import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium", sql_output="native")

with app.setup:
    import marimo as mo

    COHORT_SIZE = 100000
    PREFIX = ''
    DEMO_CONFIG_PATH = PREFIX + 'config/demo_data_config.yaml'
    MIMIC_CONFIG_PATH = PREFIX + 'config/config.yaml'
    CONFIG_PATH = MIMIC_CONFIG_PATH

    from memory_tracker import track_memory, get_report, clear_report

    from clifpy import load_data
    labs_rel = load_data('labs', config_path=CONFIG_PATH, return_rel=True)
    crrt_rel = load_data('crrt_therapy', config_path=CONFIG_PATH, return_rel=True)
    assessments_rel = load_data('patient_assessments', config_path=CONFIG_PATH, return_rel=True)
    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    vitals_rel = load_data('vitals', config_path=CONFIG_PATH, return_rel=True)
    meds_rel = load_data('medication_admin_continuous', config_path=CONFIG_PATH, return_rel=True)
    resp_rel = load_data('respiratory_support', config_path=CONFIG_PATH, return_rel=True)


@app.cell
def _():
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
def _():
    labs_agg = mo.sql(
        f"""
        EXPLAIN ANALYZE
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
def _(labs_agg):
    labs_agg.df()
    return


@app.cell
def _():
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
def _():
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
def _():
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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Medication
    """)
    return


@app.cell
def _():
    from clifpy.utils.unit_converter import convert_dose_units_by_med_category

    _preferred_units = {
          'norepinephrine': 'mcg/kg/min',
          'epinephrine': 'mcg/kg/min',
          'dopamine': 'mcg/kg/min',
          'dobutamine': 'mcg/kg/min',
          'vasopressin': 'u/min',
          'phenylephrine': 'mcg/kg/min',
      }

    with track_memory('med_unit_convert'):
        meds_unit_converted, _ = convert_dose_units_by_med_category(
            med_df=meds_rel,
            vitals_df=vitals_rel,
            return_rel=True,
            preferred_units=_preferred_units,
            override=True
        )
    return (meds_unit_converted,)


@app.cell
def _(meds_unit_converted):
    # Centralized MAR deduplication for vasopressors (preserves original dose values)
    meds_deduped = mo.sql(
        f"""
        FROM meds_unit_converted
        SELECT
            hospitalization_id,
            admin_dttm,
            med_category,
            mar_action_category,
            med_dose
        WHERE med_category IN ('norepinephrine', 'epinephrine', 'dopamine', 'dobutamine', 'vasopressin', 'phenylephrine')
            AND med_route_category = 'iv'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY hospitalization_id, admin_dttm, med_category
            ORDER BY
                CASE WHEN mar_action_category IS NULL THEN 10
                    WHEN mar_action_category IN ('verify', 'not_given') THEN 9
                    WHEN mar_action_category = 'stop' THEN 8
                    WHEN mar_action_category = 'going' THEN 7
                    ELSE 1 END,
                CASE WHEN med_dose > 0 THEN 1 ELSE 2 END,
                med_dose DESC
        ) = 1
        """
    )
    return (meds_deduped,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Vasopressor
    """)
    return


@app.cell
def _(cohort_df, meds_deduped):
    # Cell 1: Get most recent dose BEFORE start_dttm for each vasopressor (initial state)
    # Uses meds_deduped which already has MAR deduplication and vasopressor filtering
    vaso_initial_state = mo.sql(
        f"""
        FROM cohort_df c
        CROSS JOIN (
            SELECT DISTINCT med_category
            FROM meds_deduped
        ) cats
        ASOF LEFT JOIN meds_deduped t
            ON c.hospitalization_id = t.hospitalization_id
            AND cats.med_category = t.med_category
            AND c.start_dttm >= t.admin_dttm
        SELECT
            c.hospitalization_id,
            c.start_dttm AS admin_dttm,
            t.med_category,
            t.mar_action_category,
            CASE WHEN t.mar_action_category = 'stop' THEN 0 ELSE t.med_dose END AS med_dose
        WHERE t.admin_dttm IS NOT NULL
        """
    )
    return (vaso_initial_state,)


@app.cell
def _(vaso_initial_state):
    with track_memory('vaso_initial_state'):
        vaso_initial_state.df()
    return


@app.cell
def _(meds_deduped):
    # Cell 2: Get all vasopressor events during [start_dttm, end_dttm] with MAR dedup
    vaso_in_window = mo.sql(
        f"""
        FROM meds_deduped t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            t.admin_dttm,
            t.med_category,
            t.mar_action_category,
            CASE WHEN t.mar_action_category = 'stop' THEN 0 ELSE t.med_dose END AS med_dose
        """
    )
    return (vaso_in_window,)


@app.cell
def _(vaso_in_window, vaso_initial_state):
    vaso_events = mo.sql(
        f"""
        --EXPLAIN ANALYSE
        SELECT hospitalization_id, admin_dttm, med_category, med_dose, mar_action_category
        FROM vaso_initial_state
        UNION ALL
        SELECT hospitalization_id, admin_dttm, med_category, med_dose, mar_action_category
        FROM vaso_in_window
        """
    )
    return (vaso_events,)


@app.cell
def _(vaso_events):
    with track_memory('vaso_events'):
        vaso_events.df()
    return


@app.cell
def _(vaso_events):
    vaso_events.df()
    return


@app.cell
def _():
    # Cell 4: Pivot to wide format (one column per vasopressor)
    vaso_wide = mo.sql(
        f"""
        EXPLAIN ANALYSE
        FROM vaso_events
        PIVOT (
            MAX(med_dose)
            FOR med_category IN (
                'norepinephrine' AS norepi_raw,
                'epinephrine' AS epi_raw,
                'dopamine' AS dopamine_raw,
                'dobutamine' AS dobutamine_raw,
                'vasopressin' AS vasopressin_raw,
                'phenylephrine' AS phenylephrine_raw
            )
            GROUP BY hospitalization_id, admin_dttm
        )
        """
    )
    return (vaso_wide,)


@app.cell
def _(vaso_wide):
    # with track_memory('vaso_wide'):
    vaso_wide.df()
    return


@app.cell
def _(vaso_wide):
    # Cell 5: Forward-fill doses using LAST_VALUE IGNORE NULLS
    vaso_filled = mo.sql(
        f"""
        FROM vaso_wide
        SELECT
            hospitalization_id,
            admin_dttm,
            norepi_raw,
            epi_raw,
            COALESCE(LAST_VALUE(norepi_raw IGNORE NULLS) OVER w, 0) AS norepi,
            COALESCE(LAST_VALUE(epi_raw IGNORE NULLS) OVER w, 0) AS epi,
            COALESCE(LAST_VALUE(dopamine_raw IGNORE NULLS) OVER w, 0) AS dopamine,
            COALESCE(LAST_VALUE(dobutamine_raw IGNORE NULLS) OVER w, 0) AS dobutamine,
            COALESCE(LAST_VALUE(vasopressin_raw IGNORE NULLS) OVER w, 0) AS vasopressin,
            COALESCE(LAST_VALUE(phenylephrine_raw IGNORE NULLS) OVER w, 0) AS phenylephrine
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY admin_dttm
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        """
    )
    return (vaso_filled,)


@app.cell
def _(vaso_filled):
    # Cell 6: Track episode duration for >=60 min enforcement (SOFA-2 Note 8)
    vaso_with_duration = mo.sql(
        f"""
        WITH with_lag AS (
            -- Step 1: Compute LAG values (cannot be nested in window function)
            FROM vaso_filled
            SELECT
                *,
                LAG(norepi) OVER w AS prev_norepi,
                LAG(epi) OVER w AS prev_epi
            WINDOW w AS (PARTITION BY hospitalization_id ORDER BY admin_dttm)
        ),
        episode_groups AS (
            -- Step 2: Assign episode IDs using the pre-computed LAG values
            FROM with_lag
            SELECT
                *,
                SUM(CASE WHEN norepi > 0 AND COALESCE(prev_norepi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS norepi_episode,
                SUM(CASE WHEN epi > 0 AND COALESCE(prev_epi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS epi_episode
            WINDOW w AS (PARTITION BY hospitalization_id ORDER BY admin_dttm)
        ),
        with_episode_start AS (
            -- Step 3: Get episode start time for each vasopressor
            FROM episode_groups
            SELECT
                *,
                FIRST_VALUE(admin_dttm) OVER (PARTITION BY hospitalization_id, norepi_episode ORDER BY admin_dttm) AS norepi_episode_start,
                FIRST_VALUE(admin_dttm) OVER (PARTITION BY hospitalization_id, epi_episode ORDER BY admin_dttm) AS epi_episode_start
        )
        -- Step 4: Apply 60-min threshold
        FROM with_episode_start
        SELECT
            hospitalization_id,
            admin_dttm,
            norepi,
            epi,
            dopamine,
            dobutamine,
            vasopressin,
            phenylephrine,
            CASE WHEN norepi > 0 AND DATEDIFF('minute', norepi_episode_start, admin_dttm) >= 60
                 THEN norepi ELSE 0 END AS norepi_valid,
            CASE WHEN epi > 0 AND DATEDIFF('minute', epi_episode_start, admin_dttm) >= 60
                 THEN epi ELSE 0 END AS epi_valid
        """
    )
    return (vaso_with_duration,)


@app.cell
def _(cohort_df, vaso_with_duration):
    # Cell 7: Final aggregation - MAX concurrent norepi + epi sum
    vaso_concurrent = mo.sql(
        f"""
        FROM vaso_with_duration f
        JOIN cohort_df c ON f.hospitalization_id = c.hospitalization_id
        SELECT
            f.hospitalization_id,
            MAX(f.norepi_valid + f.epi_valid) AS norepi_epi_max_concurrent,
            MAX(f.norepi_valid) AS norepi_max,
            MAX(f.epi_valid) AS epi_max,
            CASE WHEN MAX(f.dopamine) > 0 OR MAX(f.dobutamine) > 0
                  OR MAX(f.vasopressin) > 0 OR MAX(f.phenylephrine) > 0
                 THEN 1 ELSE 0 END AS has_other_vasopressor
        WHERE f.admin_dttm >= c.start_dttm
          AND f.admin_dttm <= c.end_dttm
        GROUP BY f.hospitalization_id
        """
    )
    return (vaso_concurrent,)


@app.cell
def _(cohort_df, map_agg, vaso_concurrent):
    # Cell 8: Cardiovascular score using vaso_concurrent
    cv_agg = mo.sql(
        f"""
        FROM cohort_df c
        LEFT JOIN map_agg m USING (hospitalization_id)
        LEFT JOIN vaso_concurrent v USING (hospitalization_id)
        SELECT
            c.hospitalization_id,
            -- Define aliases (DuckDB allows reuse in same SELECT)
            COALESCE(v.norepi_epi_max_concurrent, 0) AS norepi_epi_sum,
            COALESCE(v.has_other_vasopressor, 0) AS has_other_vaso,
            m.map_min,
            -- CV Score (reuse aliases)
            cardiovascular: CASE
                WHEN norepi_epi_sum > 0.4 THEN 4
                WHEN norepi_epi_sum > 0.2 AND has_other_vaso = 1 THEN 4
                WHEN norepi_epi_sum > 0.2 THEN 3
                WHEN norepi_epi_sum > 0 AND has_other_vaso = 1 THEN 3
                WHEN norepi_epi_sum > 0 THEN 2
                WHEN has_other_vaso = 1 THEN 2
                WHEN map_min < 70 THEN 1
                WHEN map_min >= 70 THEN 0
                ELSE NULL
            END
        """
    )
    return (cv_agg,)


@app.cell
def _():
    mo.md(r"""
    ### Respiratory
    """)
    return


@app.cell
def _():
    # FiO2 aggregation with imputation based on device category
    fio2_agg = mo.sql(
        f"""
        WITH fio2_imputed AS (
            FROM resp_rel t
            SEMI JOIN cohort_df c ON
                t.hospitalization_id = c.hospitalization_id
                AND t.recorded_dttm >= c.start_dttm
                AND t.recorded_dttm <= c.end_dttm
            SELECT
                hospitalization_id,
                recorded_dttm,
                device_category,
                -- Impute FiO2 based on device
                CASE
                    WHEN fio2_set IS NOT NULL AND fio2_set > 0 THEN fio2_set
                    WHEN device_category = 'Room_Air' THEN 0.21
                    -- Nasal cannula: impute from lpm_set
                    WHEN device_category = 'Nasal_Cannula' THEN
                        CASE WHEN lpm_set <= 1 THEN 0.24
                             WHEN lpm_set <= 2 THEN 0.28
                             WHEN lpm_set <= 3 THEN 0.32
                             WHEN lpm_set <= 4 THEN 0.36
                             WHEN lpm_set <= 5 THEN 0.40
                             WHEN lpm_set <= 6 THEN 0.44
                             ELSE 0.50 END
                    ELSE fio2_set
                END AS fio2_imputed,
                -- Flag for advanced respiratory support (scores 3-4 require this)
                CASE WHEN device_category IN ('IMV', 'NIPPV', 'CPAP', 'High_Flow_NC')
                     THEN 1 ELSE 0 END AS is_advanced_support
        )
        FROM fio2_imputed
        SELECT
            hospitalization_id,
            MAX(fio2_imputed) AS fio2_max,
            MAX(is_advanced_support) AS has_advanced_support
        GROUP BY hospitalization_id
        """
    )
    return (fio2_agg,)


@app.cell
def _(fio2_agg, labs_agg):
    # P/F Ratio calculation using worst FiO2 with worst PaO2 (conservative approach)
    pf_ratio = mo.sql(
        f"""
        FROM labs_agg l
        LEFT JOIN fio2_agg f USING (hospitalization_id)
        SELECT
            l.hospitalization_id,
            l.po2_arterial,
            f.fio2_max,
            f.has_advanced_support,
            -- P/F ratio (PaO2 in mmHg / FiO2 as fraction)
            CASE WHEN f.fio2_max > 0 AND l.po2_arterial IS NOT NULL
                 THEN l.po2_arterial / f.fio2_max
                 ELSE NULL END AS pf_ratio
        """
    )
    return (pf_ratio,)


@app.cell
def _(pf_ratio):
    # Respiratory score calculation
    resp_agg = mo.sql(
        f"""
        FROM pf_ratio
        SELECT
            hospitalization_id,
            pf_ratio,
            has_advanced_support,
            -- Respiratory Score
            respiratory: CASE
                -- Score 4: P/F <=75 AND advanced support
                WHEN pf_ratio <= 75 AND has_advanced_support = 1 THEN 4
                -- Score 3: P/F <=150 AND advanced support
                WHEN pf_ratio <= 150 AND has_advanced_support = 1 THEN 3
                -- Score 2: P/F <=225 (no vent requirement)
                WHEN pf_ratio <= 225 THEN 2
                -- Score 1: P/F <=300
                WHEN pf_ratio <= 300 THEN 1
                -- Score 0: P/F >300
                WHEN pf_ratio > 300 THEN 0
                ELSE NULL
            END
        """
    )
    return (resp_agg,)


@app.cell
def _(cohort_df, cv_agg, gcs_agg, labs_agg, resp_agg, rrt_flag):
    sofa_scores = mo.sql(
        f"""
        -- EXPLAIN ANALYZE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id)
        LEFT JOIN rrt_flag r USING (hospitalization_id)
        LEFT JOIN gcs_agg g USING (hospitalization_id)
        LEFT JOIN cv_agg cv USING (hospitalization_id)
        LEFT JOIN resp_agg resp USING (hospitalization_id)
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
            -- Respiratory
            resp.respiratory,
            -- SOFA Total (sum of all 6 components)
            sofa_total: COALESCE(hemostasis, 0)
                + COALESCE(liver, 0)
                + COALESCE(kidney, 0)
                + COALESCE(brain, 0)
                + COALESCE(cv.cardiovascular, 0)
                + COALESCE(resp.respiratory, 0),
            -- Intermediate values for debugging
            l.platelet_count,
            l.bilirubin_total,
            l.creatinine,
            l.po2_arterial,
            COALESCE(r.has_rrt, 0) AS has_rrt,
            g.gcs_min,
            cv.norepi_epi_sum,
            cv.map_min,
            resp.pf_ratio,
            resp.has_advanced_support
        """
    )
    return (sofa_scores,)


@app.cell
def _(sofa_scores):
    sofa_scores.df()
    return


@app.cell
def _():
    get_report()
    return


if __name__ == "__main__":
    app.run()
