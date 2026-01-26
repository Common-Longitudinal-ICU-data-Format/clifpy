import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium", sql_output="pandas")

with app.setup:
    import marimo as mo
    import duckdb

    COHORT_SIZE = 1000
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
    ecmo_rel = load_data('ecmo_mcs', config_path=CONFIG_PATH, return_rel=True)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Cohort
    """)
    return


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
def _(cohort_df):
    labs_agg = mo.sql(
        f"""
        -- Lab aggregations for hemostasis, liver, kidney subscores
        -- EXPLAIN ANALYZE
        FROM labs_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_result_dttm >= c.start_dttm
            AND t.lab_result_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
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
        GROUP BY t.hospitalization_id, c.start_dttm
        """
    )
    return (labs_agg,)


@app.cell
def _(labs_agg):
    labs_agg.df()
    return


@app.cell
def _(cohort_df):
    rrt_flag = mo.sql(
        f"""
        -- Detect if patient received RRT during the time window
        FROM crrt_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT t.hospitalization_id, c.start_dttm, 1 AS has_rrt
        """
    )
    return (rrt_flag,)


@app.cell
def _(cohort_df):
    gcs_agg = mo.sql(
        f"""
        -- GCS aggregation for brain subscore
        FROM assessments_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            MIN(numerical_value) AS gcs_min
        WHERE assessment_category = 'gcs_total'
        GROUP BY t.hospitalization_id, c.start_dttm
        """
    )
    return (gcs_agg,)


@app.cell
def _(cohort_df):
    map_agg = mo.sql(
        f"""
        -- MAP aggregation for cardiovascular subscore
        FROM vitals_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            MIN(vital_value) AS map_min
        WHERE vital_category = 'map'
        GROUP BY t.hospitalization_id, c.start_dttm
        """
    )
    return (map_agg,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # CV
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Medication
    """)
    return


@app.cell
def _(cohort_df):
    from clifpy.utils.unit_converter import convert_dose_units_by_med_category

    # FIXME: add filtering
    _preferred_units = {
          'norepinephrine': 'mcg/kg/min',
          'epinephrine': 'mcg/kg/min',
          'dopamine': 'mcg/kg/min',
          'dobutamine': 'mcg/kg/min',
          'vasopressin': 'u/min',
          'phenylephrine': 'mcg/kg/min',
          'milrinone': 'mcg/kg/min',
          'angiotensin': 'ng/kg/min',
          'isoproterenol': 'mcg/kg/min',
      }

    cohort_vasopressors = list(_preferred_units.keys())  

    cohort_meds_rel = mo.sql(
        f"""
        FROM meds_rel t
        INNER JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT *
        WHERE t.med_category IN {cohort_vasopressors}
        """
    )

    cohort_vitals_rel = mo.sql(
        f"""
        FROM vitals_rel t
        INNER JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT *
        """
    )

    with track_memory('med_unit_convert'):
        meds_unit_converted, _ = convert_dose_units_by_med_category(
            med_df=cohort_meds_rel,
            vitals_df=cohort_vitals_rel,
            return_rel=True,
            preferred_units=_preferred_units,
            override=True
        )
    return cohort_meds_rel, meds_unit_converted


@app.cell
def _(cohort_meds_rel):
    cohort_meds_rel.df()
    return


@app.cell
def _(meds_unit_converted):
    meds_unit_converted.df()
    return


@app.cell
def _(meds_unit_converted):
    # Centralized MAR deduplication for vasopressors (preserves original dose values)
    meds_deduped = mo.sql(
        f"""
        FROM meds_unit_converted t
        --SEMI JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT
            t.hospitalization_id,
            t.admin_dttm,
            t.med_category,
            t.mar_action_category,
            t.med_dose
        --WHERE t.med_category IN ('norepinephrine', 'epinephrine', 'dopamine', 'dobutamine', 'vasopressin', 'phenylephrine', 'milrinone', 'angiotensin', 'isoproterenol')
            --AND t.med_route_category = 'iv'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.hospitalization_id, t.admin_dttm, t.med_category
            ORDER BY
                CASE WHEN t.mar_action_category IS NULL THEN 10
                    WHEN t.mar_action_category IN ('verify', 'not_given') THEN 9
                    WHEN t.mar_action_category = 'stop' THEN 8
                    WHEN t.mar_action_category = 'going' THEN 7
                    ELSE 1 END,
                CASE WHEN t.med_dose > 0 THEN 1 ELSE 2 END,
                t.med_dose DESC
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
    vaso_pre_window = mo.sql(
        f"""
        -- Get most recent dose BEFORE start_dttm for each vasopressor (pre-window state)
        -- Uses meds_deduped which already has MAR deduplication and vasopressor filtering
        -- NOTE: For medications, we ALWAYS forward-fill (not fallback), so this is always included
        WITH med_cats AS (
            SELECT DISTINCT med_category FROM meds_deduped
        ),
        cohort_meds AS (
            FROM cohort_df c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN meds_deduped t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm > t.admin_dttm
        SELECT
            cm.hospitalization_id,
            cm.start_dttm,  -- window identity
            t.admin_dttm,
            t.admin_dttm - cm.start_dttm AS time_since_start,
            cm.med_category,
            t.mar_action_category,
            CASE WHEN t.mar_action_category = 'stop' THEN 0 ELSE t.med_dose END AS med_dose
        WHERE t.hospitalization_id IS NOT NULL
            -- NOTE: should prob filter out all the stop / 0-dose as well
            AND t.med_dose != 0
        """
    )
    return (vaso_pre_window,)


@app.cell
def _(vaso_pre_window):
    with track_memory('vaso_pre_window'):
        vaso_pre_window
    return


@app.cell
def _(cohort_df, meds_deduped):
    vaso_in_window = mo.sql(
        f"""
        -- Get all vasopressor events during [start_dttm, end_dttm] with MAR dedup
        -- INNER JOIN to carry window identity (non-overlapping windows assumed)
        FROM meds_deduped t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            t.admin_dttm,
            t.admin_dttm - c.start_dttm AS time_since_start,
            t.med_category,
            t.mar_action_category,
            CASE WHEN t.mar_action_category = 'stop' THEN 0 ELSE t.med_dose END AS med_dose
        """
    )
    return (vaso_in_window,)


@app.cell
def _(vaso_in_window, vaso_pre_window):
    vaso_events = mo.sql(
        f"""
        -- Combine pre-window and in-window vaso events
        -- NOTE: For medications, we ALWAYS include pre-window (forward-fill), not fallback
        --EXPLAIN ANALYZE
        SELECT hospitalization_id, start_dttm, admin_dttm, time_since_start, med_category, med_dose, mar_action_category
        FROM vaso_pre_window
        UNION ALL
        SELECT hospitalization_id, start_dttm, admin_dttm, time_since_start, med_category, med_dose, mar_action_category
        FROM vaso_in_window
        -- Removed ORDER BY: unnecessary before PIVOT, sorting handled by window functions downstream
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
def _(vaso_events):
    vaso_wide_ = mo.sql(
        f"""
        -- EXPLAIN ANALYSE
        -- this is now just a backup copy
        PIVOT vaso_events
        ON med_category
        USING ANY_VALUE(med_dose)
        GROUP BY hospitalization_id, admin_dttm
        """
    )
    return


@app.cell
def _(vaso_wide):
    vaso_wide.df()
    return


@app.cell
def _(vaso_events):
    # Cell 4: Pivot to wide format (one column per vasopressor)
    vaso_wide = mo.sql(
        f"""
        --EXPLAIN ANALYSE
        FROM vaso_events
        PIVOT (
            ANY_VALUE(med_dose)
            FOR med_category IN (
                'norepinephrine' AS norepi_raw,
                'epinephrine' AS epi_raw,
                'dopamine' AS dopamine_raw,
                'dobutamine' AS dobutamine_raw,
                'vasopressin' AS vasopressin_raw,
                'phenylephrine' AS phenylephrine_raw,
                'milrinone' AS milrinone_raw,
                'angiotensin' AS angiotensin_raw,
                'isoproterenol' AS isoproterenol_raw
            )
            GROUP BY hospitalization_id, admin_dttm
        )
        """
    )
    return (vaso_wide,)


@app.cell
def _(vaso_wide):
    vaso_wide.df()
    return


@app.cell
def _(vaso_wide):
    with track_memory('vaso_wide'):
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
            --norepi_raw,
            --epi_raw,
            COALESCE(LAST_VALUE(norepi_raw IGNORE NULLS) OVER w, 0) AS norepi,
            COALESCE(LAST_VALUE(epi_raw IGNORE NULLS) OVER w, 0) AS epi,
            COALESCE(LAST_VALUE(dopamine_raw IGNORE NULLS) OVER w, 0) AS dopamine,
            COALESCE(LAST_VALUE(dobutamine_raw IGNORE NULLS) OVER w, 0) AS dobutamine,
            COALESCE(LAST_VALUE(vasopressin_raw IGNORE NULLS) OVER w, 0) AS vasopressin,
            COALESCE(LAST_VALUE(phenylephrine_raw IGNORE NULLS) OVER w, 0) AS phenylephrine,
            COALESCE(LAST_VALUE(milrinone_raw IGNORE NULLS) OVER w, 0) AS milrinone,
            COALESCE(LAST_VALUE(angiotensin_raw IGNORE NULLS) OVER w, 0) AS angiotensin,
            COALESCE(LAST_VALUE(isoproterenol_raw IGNORE NULLS) OVER w, 0) AS isoproterenol
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY admin_dttm
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        --ORDER BY hospitalization_id, admin_dttm
        """
    )
    return (vaso_filled,)


@app.cell
def _(vaso_filled):
    vaso_filled.df()
    return


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
            	-- cumsum of whenever a new episode starts, defined by switching from 0 to non-zero
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
            milrinone,
            angiotensin,
            isoproterenol,
            CASE WHEN norepi > 0 AND DATEDIFF('minute', norepi_episode_start, admin_dttm) >= 60
                 THEN norepi ELSE 0 END AS norepi_valid,
            CASE WHEN epi > 0 AND DATEDIFF('minute', epi_episode_start, admin_dttm) >= 60
                 THEN epi ELSE 0 END AS epi_valid
        """
    )
    return (vaso_with_duration,)


@app.cell
def _(vaso_with_duration):
    vaso_with_duration.df()
    return


@app.cell
def _(cohort_df, vaso_with_duration):
    vaso_concurrent = mo.sql(
        f"""
        -- Final aggregation - MAX concurrent norepi + epi sum
        FROM vaso_with_duration f
        JOIN cohort_df c ON f.hospitalization_id = c.hospitalization_id
        SELECT
            f.hospitalization_id,
            c.start_dttm,  -- window identity
            MAX(f.norepi_valid + f.epi_valid) AS norepi_epi_max_concurrent,
            MAX(f.norepi_valid) AS norepi_max,
            MAX(f.epi_valid) AS epi_max,
            CASE WHEN MAX(f.dopamine) > 0 OR MAX(f.dobutamine) > 0
                  OR MAX(f.vasopressin) > 0 OR MAX(f.phenylephrine) > 0
                  OR MAX(f.milrinone) > 0 OR MAX(f.angiotensin) > 0
                  OR MAX(f.isoproterenol) > 0
                 THEN 1 ELSE 0 END AS has_other_vasopressor
        WHERE f.admin_dttm >= c.start_dttm
          AND f.admin_dttm <= c.end_dttm
        GROUP BY f.hospitalization_id, c.start_dttm
        """
    )
    return (vaso_concurrent,)


@app.cell
def _(cohort_df, map_agg, vaso_concurrent):
    cv_agg = mo.sql(
        f"""
        -- Cardiovascular score using vaso_concurrent
        FROM cohort_df c
        LEFT JOIN map_agg m USING (hospitalization_id, start_dttm)
        LEFT JOIN vaso_concurrent v USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id,
            c.start_dttm,  -- window identity
            -- Define aliases (DuckDB allows reuse in same SELECT)
            COALESCE(v.norepi_epi_max_concurrent, 0) AS norepi_epi_sum,
            COALESCE(v.has_other_vasopressor, 0) AS has_other_vaso,
            m.map_min,
            -- CV Score (reuse aliases)
            cardiovascular: CASE
                WHEN norepi_epi_sum > 0.4 THEN 4
                WHEN norepi_epi_sum > 0.2 AND has_other_vaso = 1 THEN 4
                WHEN norepi_epi_sum > 0.2 THEN 3
                WHEN norepi_epi_sum > 0 AND has_other_vaso = 1 THEN 3 -- norepi_epi_sum <= 0.2 implied
                WHEN norepi_epi_sum > 0 THEN 2 -- has_other_vaso = 0 implied
                WHEN has_other_vaso = 1 THEN 2
            	-- here on implies no pressor as norepi_epi_sum <= 0 and has_other_vaso = 0
                WHEN map_min < 70 THEN 1
                WHEN map_min >= 70 THEN 0
                ELSE NULL
            END
        -- NOTE: REVIEWED
        """
    )
    return (cv_agg,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Respiratory
    """)
    return


@app.cell
def _(cohort_df):
    # Get latest FiO2 measurement BEFORE start_dttm (pre-window state)
    # Used as fallback when no in-window measurements exist
    fio2_pre_window = mo.sql(
        f"""
        -- Get latest FiO2 measurement BEFORE start_dttm (pre-window state)
        -- Used as fallback when no in-window measurements exist
        FROM cohort_df c
        ASOF LEFT JOIN resp_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.recorded_dttm
        SELECT
            c.hospitalization_id,
            c.start_dttm,  -- window identity
            t.recorded_dttm,
            t.recorded_dttm - c.start_dttm AS time_since_start,
            t.device_category,
            t.fio2_set,
            t.lpm_set
        WHERE t.hospitalization_id IS NOT NULL
        -- NOTE: REVIEWED
        """
    )
    return (fio2_pre_window,)


@app.cell
def _(cohort_df):
    # Get all FiO2 measurements during [start_dttm, end_dttm]
    # INNER JOIN to carry window identity (non-overlapping windows assumed)
    fio2_in_window = mo.sql(
        f"""
        -- Get all FiO2 measurements during [start_dttm, end_dttm]
        -- INNER JOIN to carry window identity (non-overlapping windows assumed)
        FROM resp_rel t
        INNER JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            t.recorded_dttm,
            t.recorded_dttm - c.start_dttm AS time_since_start,
            t.device_category,
            t.fio2_set,
            t.lpm_set
        -- NOTE: REVIEWED
        """
    )
    return (fio2_in_window,)


@app.cell
def _(fio2_in_window, fio2_pre_window):
    # Apply FiO2 imputation logic with fallback: use pre_window only when no in_window data
    fio2_imputed = mo.sql(
        f"""
        -- Apply FiO2 imputation logic with fallback: use pre_window only when no in_window data
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM fio2_in_window
        ),
        pre_window_fallback AS (
            -- Only for windows WITHOUT in-window data
            FROM fio2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT *
        ),
        combined AS (
            FROM fio2_in_window SELECT *
            UNION ALL
            FROM pre_window_fallback SELECT *
        )
        FROM combined t
        SELECT
            t.hospitalization_id,
            t.start_dttm,
            t.recorded_dttm,
            t.time_since_start,
            t.device_category,
            -- Impute FiO2 based on device
            CASE
                WHEN t.fio2_set IS NOT NULL AND t.fio2_set > 0 THEN t.fio2_set
                WHEN LOWER(t.device_category) = 'room air' THEN 0.21
                -- Nasal cannula: impute from lpm_set
                WHEN LOWER(t.device_category) = 'nasal cannula' THEN
                    CASE WHEN t.lpm_set <= 1 THEN 0.24
                         WHEN t.lpm_set <= 2 THEN 0.28
                         WHEN t.lpm_set <= 3 THEN 0.32
                         WHEN t.lpm_set <= 4 THEN 0.36
                         WHEN t.lpm_set <= 5 THEN 0.40
                         WHEN t.lpm_set <= 6 THEN 0.44
                         ELSE 0.50 END
                ELSE t.fio2_set
            END AS fio2_imputed,
            -- Flag for advanced respiratory support (scores 3-4 require this)
            CASE WHEN LOWER(t.device_category) IN ('imv', 'nippv', 'cpap', 'high flow nc')
                 THEN 1 ELSE 0 END AS is_advanced_support
        WHERE t.fio2_set IS NOT NULL OR t.device_category IS NOT NULL
        -- NOTE: REVIEWED
        """
    )
    return (fio2_imputed,)


@app.cell
def _(cohort_df):
    # PaO2 measurements with fallback: use pre_window only when no in_window data
    pao2_measurements = mo.sql(
        f"""
        -- PaO2 measurements with fallback: use pre_window only when no in_window data
        WITH pao2_pre_window AS (
            -- Get latest PaO2 measurement BEFORE start_dttm (pre-window state)
            FROM cohort_df c
            ASOF LEFT JOIN labs_rel t
                ON c.hospitalization_id = t.hospitalization_id
                AND c.start_dttm > t.lab_result_dttm
            SELECT
                c.hospitalization_id,
                c.start_dttm,  -- window identity
                t.lab_result_dttm,
                t.lab_result_dttm - c.start_dttm AS time_since_start,
                t.lab_value_numeric AS pao2
            WHERE t.hospitalization_id IS NOT NULL
                AND t.lab_category = 'po2_arterial'
                AND t.lab_value_numeric IS NOT NULL
        ),
        pao2_in_window AS (
            -- Get all PaO2 measurements during [start_dttm, end_dttm]
            -- INNER JOIN to carry window identity (non-overlapping windows assumed)
            FROM labs_rel t
            INNER JOIN cohort_df c ON
                t.hospitalization_id = c.hospitalization_id
                AND t.lab_result_dttm >= c.start_dttm
                AND t.lab_result_dttm <= c.end_dttm
            SELECT
                t.hospitalization_id,
                c.start_dttm,  -- window identity
                t.lab_result_dttm,
                t.lab_result_dttm - c.start_dttm AS time_since_start,
                t.lab_value_numeric AS pao2
            WHERE t.lab_category = 'po2_arterial'
                AND t.lab_value_numeric IS NOT NULL
        ),
        windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM pao2_in_window
        ),
        pre_window_fallback AS (
            -- Only for windows WITHOUT in-window data
            FROM pao2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT *
        )
        FROM pao2_in_window SELECT *
        UNION ALL
        FROM pre_window_fallback SELECT *
        -- NOTE: REVIEWED
        """
    )
    return (pao2_measurements,)


@app.cell
def _(cohort_df):
    # SpO2 measurements for S/F ratio calculation with fallback logic
    # Per spec 4: Use SpO2:FiO2 only when PaO2:FiO2 unavailable AND SpO2 < 98%
    spo2_measurements = mo.sql(
        f"""
        -- SpO2 measurements for S/F ratio calculation with fallback logic
        -- Per spec 4: Use SpO2:FiO2 only when PaO2:FiO2 unavailable AND SpO2 < 98%
        WITH spo2_pre_window AS (
            -- Get latest SpO2 measurement BEFORE start_dttm (pre-window state)
            FROM cohort_df c
            ASOF LEFT JOIN vitals_rel t
                ON c.hospitalization_id = t.hospitalization_id
                AND c.start_dttm > t.recorded_dttm
            SELECT
                c.hospitalization_id,
                c.start_dttm,  -- window identity
                t.recorded_dttm,
                t.recorded_dttm - c.start_dttm AS time_since_start,
                t.vital_value AS spo2
            WHERE t.hospitalization_id IS NOT NULL
                AND t.vital_category = 'spo2'
                AND t.vital_value IS NOT NULL
                AND t.vital_value < 98  -- per spec 4: only use when SpO2 < 98%
                AND t.vital_value > 0
        ),
        spo2_in_window AS (
            -- Get all SpO2 measurements during [start_dttm, end_dttm]
            -- INNER JOIN to carry window identity (non-overlapping windows assumed)
            FROM vitals_rel t
            JOIN cohort_df c ON
                t.hospitalization_id = c.hospitalization_id
                AND t.recorded_dttm >= c.start_dttm
                AND t.recorded_dttm <= c.end_dttm
            SELECT
                t.hospitalization_id,
                c.start_dttm,  -- window identity
                t.recorded_dttm,
                t.recorded_dttm - c.start_dttm AS time_since_start,
                t.vital_value AS spo2
            WHERE t.vital_category = 'spo2'
                AND t.vital_value IS NOT NULL
                AND t.vital_value < 98  -- per spec 4: only use when SpO2 < 98%
                AND t.vital_value > 0
        ),
        windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM spo2_in_window
        ),
        pre_window_fallback AS (
            -- Only for windows WITHOUT in-window data
            FROM spo2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT *
        )
        FROM spo2_in_window SELECT *
        UNION ALL
        FROM pre_window_fallback SELECT *
        -- NOTE: REVIEWED
        """
    )
    return (spo2_measurements,)


@app.cell
def _(fio2_imputed, pao2_measurements):
    # Concurrent P/F ratio using ASOF JOIN, with window identity
    concurrent_pf = mo.sql(
        f"""
        -- Concurrent P/F ratio using ASOF JOIN, with window identity
        FROM pao2_measurements p
        ASOF JOIN fio2_imputed f
            ON p.hospitalization_id = f.hospitalization_id
            AND p.start_dttm = f.start_dttm  -- same window
            -- find the most recent FiO2 before or at the PaO2 time
            AND f.recorded_dttm <= p.lab_result_dttm
        SELECT
            p.hospitalization_id,
            p.start_dttm,  -- window identity
            p.pao2,
            p.lab_result_dttm AS pao2_dttm,
            f.fio2_imputed,
            f.recorded_dttm AS fio2_dttm,
            f.device_category,
            f.is_advanced_support,
            pao2_dttm - fio2_dttm AS pf_time_gap,
            p.pao2 / f.fio2_imputed AS pf_ratio
        WHERE f.fio2_imputed IS NOT NULL
            AND f.fio2_imputed > 0
            -- 4-hour lookback tolerance
            AND pf_time_gap <= INTERVAL '240 minutes' -- 4 hrs
        -- NOTE: REVIEWED
        """
    )
    return (concurrent_pf,)


@app.cell
def _(fio2_imputed, spo2_measurements):
    # Concurrent S/F ratio using ASOF JOIN (SpO2:FiO2 for when PaO2 unavailable), with window identity
    concurrent_sf = mo.sql(
        f"""
        -- Concurrent S/F ratio using ASOF JOIN (SpO2:FiO2 for when PaO2 unavailable), with window identity
        FROM spo2_measurements s
        ASOF JOIN fio2_imputed f
            ON s.hospitalization_id = f.hospitalization_id
            AND s.start_dttm = f.start_dttm  -- same window
            -- find the most recent FiO2 before or at the SpO2 time
            AND f.recorded_dttm <= s.recorded_dttm
        SELECT
            s.hospitalization_id,
            s.start_dttm,  -- window identity
            s.spo2,
            s.recorded_dttm AS spo2_dttm,
            f.fio2_imputed,
            f.recorded_dttm AS fio2_dttm,
            f.device_category,
            f.is_advanced_support,
            spo2_dttm - fio2_dttm AS sf_time_gap,
            s.spo2 / f.fio2_imputed AS sf_ratio
        WHERE f.fio2_imputed IS NOT NULL
            AND f.fio2_imputed > 0
            -- 4-hour tolerance
            AND sf_time_gap <= INTERVAL '240 minutes'
        -- NOTE: REVIEWED
        """
    )
    return (concurrent_sf,)


@app.cell
def _(concurrent_pf, concurrent_sf):
    resp_agg = mo.sql(
        f"""
        -- Combine P/F and S/F ratios with priority: use S/F only when P/F unavailable per window
        WITH pf_worst AS (
            -- Worst P/F ratio per window (actual PaO2)
            FROM concurrent_pf
            SELECT
                hospitalization_id,
                start_dttm,  -- window identity
                MIN(pf_ratio) AS ratio,
                ARG_MIN(is_advanced_support, pf_ratio) AS has_advanced_support,
                ARG_MIN(device_category, pf_ratio) AS device_category,
                ARG_MIN(pao2, pf_ratio) AS pao2_at_worst,
                ARG_MIN(fio2_imputed, pf_ratio) AS fio2_at_worst,
                'pf' AS ratio_type
            GROUP BY hospitalization_id, start_dttm
        ),
        sf_worst AS (
            -- Worst S/F ratio per window (only for windows WITHOUT P/F data)
            FROM concurrent_sf
            ANTI JOIN (SELECT DISTINCT hospitalization_id, start_dttm FROM concurrent_pf) p
                USING (hospitalization_id, start_dttm)
            SELECT
                hospitalization_id,
                start_dttm,  -- window identity
                MIN(sf_ratio) AS ratio,
                ARG_MIN(is_advanced_support, sf_ratio) AS has_advanced_support,
                ARG_MIN(device_category, sf_ratio) AS device_category,
                ARG_MIN(spo2, sf_ratio) AS spo2_at_worst,
                ARG_MIN(fio2_imputed, sf_ratio) AS fio2_at_worst,
                'sf' AS ratio_type
            GROUP BY hospitalization_id, start_dttm
        )
        -- Combine: P/F takes priority, S/F only for windows without P/F
        FROM pf_worst
        SELECT hospitalization_id, start_dttm, ratio, has_advanced_support, device_category, ratio_type,
               pao2_at_worst, NULL AS spo2_at_worst, fio2_at_worst
        UNION ALL
        FROM sf_worst
        SELECT hospitalization_id, start_dttm, ratio, has_advanced_support, device_category, ratio_type,
               NULL AS pao2_at_worst, spo2_at_worst, fio2_at_worst
        """
    )
    return (resp_agg,)


@app.cell
def _(cohort_df):
    # ECMO flag for respiratory scoring (Note 7) with window identity
    # Per SOFA-2: ECMO for respiratory failure = 4 points regardless of P/F ratio
    ecmo_flag = mo.sql(
        f"""
        -- ECMO flag for respiratory scoring (Note 7) with window identity
        -- Per SOFA-2: ECMO for respiratory failure = 4 points regardless of P/F ratio
        FROM ecmo_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            1 AS has_ecmo
        """
    )
    return (ecmo_flag,)


@app.cell
def _(ecmo_flag, resp_agg):
    resp_score = mo.sql(
        f"""
        -- Respiratory score calculation with ECMO override and ratio-type-specific cutoffs
        -- P/F cutoffs: 0=>300, 1=≤300, 2=≤225, 3=≤150+vent, 4=≤75+vent
        -- S/F cutoffs: 0=>300, 1=≤300, 2=≤250, 3=≤200+vent, 4=≤120+vent
        FROM resp_agg r
        LEFT JOIN ecmo_flag e USING (hospitalization_id, start_dttm)
        SELECT
            r.hospitalization_id,
            r.start_dttm,  -- window identity
            r.ratio,
            r.ratio_type,
            r.has_advanced_support,
            r.device_category,
            r.pao2_at_worst,
            r.spo2_at_worst,
            r.fio2_at_worst,
            COALESCE(e.has_ecmo, 0) AS has_ecmo,
            -- Respiratory Score with ratio-type-specific cutoffs
            respiratory: CASE
                -- ECMO override (Note 7): 4 points regardless of ratio
                WHEN COALESCE(e.has_ecmo, 0) = 1 THEN 4
                -- P/F ratio cutoffs
                WHEN r.ratio_type = 'pf' AND r.ratio <= 75 AND r.has_advanced_support = 1 THEN 4
                WHEN r.ratio_type = 'pf' AND r.ratio <= 150 AND r.has_advanced_support = 1 THEN 3
                WHEN r.ratio_type = 'pf' AND r.ratio <= 225 THEN 2
                WHEN r.ratio_type = 'pf' AND r.ratio <= 300 THEN 1
                WHEN r.ratio_type = 'pf' AND r.ratio > 300 THEN 0
                -- S/F ratio cutoffs (different thresholds per SOFA-2 spec)
                WHEN r.ratio_type = 'sf' AND r.ratio <= 120 AND r.has_advanced_support = 1 THEN 4
                WHEN r.ratio_type = 'sf' AND r.ratio <= 200 AND r.has_advanced_support = 1 THEN 3
                WHEN r.ratio_type = 'sf' AND r.ratio <= 250 THEN 2
                WHEN r.ratio_type = 'sf' AND r.ratio <= 300 THEN 1
                WHEN r.ratio_type = 'sf' AND r.ratio > 300 THEN 0
                ELSE NULL
            END
        """
    )
    return (resp_score,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Combine
    """)
    return


@app.cell
def _(cohort_df, cv_agg, gcs_agg, labs_agg, resp_score, rrt_flag):
    sofa_scores = mo.sql(
        f"""
        -- CRITICAL: LEFT JOIN from cohort_df to preserve all cohort rows
        -- Windows without data → NULL scores (expected behavior)
        -- EXPLAIN ANALYZE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id, start_dttm)
        LEFT JOIN rrt_flag r USING (hospitalization_id, start_dttm)
        LEFT JOIN gcs_agg g USING (hospitalization_id, start_dttm)
        LEFT JOIN cv_agg cv USING (hospitalization_id, start_dttm)
        LEFT JOIN resp_score resp USING (hospitalization_id, start_dttm)
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
            resp.ratio AS pf_sf_ratio,
            resp.ratio_type,
            resp.has_advanced_support
        """
    )
    return (sofa_scores,)


@app.cell
def _(sofa_scores):
    sofa_scores
    return


@app.cell
def _():
    get_report()
    return


if __name__ == "__main__":
    app.run()
