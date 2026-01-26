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
    # CV subscore
    """)
    return


@app.cell
def _():
    # Define vasopressor list and preferred units for later unit conversion
    pressor_preferred_units = {
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

    cohort_vasopressors = list(pressor_preferred_units.keys())
    return cohort_vasopressors, pressor_preferred_units


@app.cell
def _(cohort_df):
    # Vitals needed for unit conversion (weight for mcg/kg/min)
    cohort_vitals = mo.sql(
        f"""
        FROM vitals_rel t
        INNER JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT *
        """
    )
    return (cohort_vitals,)


@app.cell
def _(cohort_df, cohort_vasopressors):
    pressor_at_start = mo.sql(
        f"""
        -- Forward-filled event AT start_dttm for each vasopressor
        -- Operates on meds_rel directly (filter by window FIRST)
        -- Includes raw columns for later dedup and unit conversion
        WITH med_cats AS (
            SELECT UNNEST({cohort_vasopressors}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_df c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm >= t.admin_dttm  -- include events AT start_dttm
        SELECT
            cm.hospitalization_id,
            cm.start_dttm,  -- window identity
            cm.start_dttm AS admin_dttm,  -- forward-filled timestamp AT start_dttm
            cm.med_category,
            t.mar_action_category,
            t.med_dose,
            t.med_dose_unit  -- needed for unit conversion
        WHERE t.hospitalization_id IS NOT NULL
            AND t.mar_action_category != 'stop'
            AND t.med_dose > 0  -- only if actively infusing
        -- TODO: REVIEWED
        """
    )
    return (pressor_at_start,)


@app.cell
def _(pressor_at_start):
    with track_memory('pressor_at_start'):
        pressor_at_start
    return


@app.cell
def _(cohort_vasopressors):
    pressor_in_window = mo.sql(
        f"""
        -- In-window vasopressor events with STRICT inequality
        -- Operates on meds_rel directly (filter by window FIRST)
        -- Includes raw columns for later dedup and unit conversion
        FROM meds_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm > c.start_dttm   -- strict >
            AND t.admin_dttm < c.end_dttm     -- strict <
        SELECT
            t.hospitalization_id,
            c.start_dttm,  -- window identity
            t.admin_dttm,
            t.med_category,
            t.mar_action_category,
            t.med_dose,
            t.med_dose_unit  -- needed for unit conversion
        WHERE t.med_category IN {cohort_vasopressors}
        """
    )
    return (pressor_in_window,)


@app.cell
def _(cohort_df, cohort_vasopressors):
    pressor_at_end = mo.sql(
        f"""
        -- Forward-filled event AT end_dttm for each vasopressor
        -- Operates on meds_rel directly (filter by window FIRST)
        -- Includes raw columns for later dedup and unit conversion
        WITH med_cats AS (
            SELECT UNNEST({cohort_vasopressors}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_df c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm -- FIXME: maybe refactor this out into a new cell since its also used in pressor_at_start ?
        ASOF LEFT JOIN meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.end_dttm >= t.admin_dttm  -- include events AT end_dttm
        SELECT
            cm.hospitalization_id,
            cm.start_dttm,  -- window identity
            cm.end_dttm AS admin_dttm,  -- forward-filled timestamp AT end_dttm
            cm.med_category,
            t.mar_action_category,
            t.med_dose,
            t.med_dose_unit  -- needed for unit conversion
        WHERE t.hospitalization_id IS NOT NULL
            -- AND t.mar_action_category != 'stop'
            -- AND t.med_dose > 0  -- only if actively infusing
        """
    )
    return (pressor_at_end,)


@app.cell
def _(pressor_at_end, pressor_at_start, pressor_in_window):
    pressor_events_raw = mo.sql(
        f"""
        -- Combine forward-filled boundary events and in-window events
        -- Raw data before dedup and unit conversion
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_at_start
        UNION ALL
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_in_window
        UNION ALL
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_at_end
        """
    )
    return (pressor_events_raw,)


@app.cell
def _(pressor_events_raw):
    # MAR deduplication on window-filtered data
    pressor_events_deduped = mo.sql(
        f"""
        -- Centralized MAR deduplication for vasopressors
        FROM pressor_events_raw t
        SELECT
            t.hospitalization_id,
            t.start_dttm,
            t.admin_dttm,
            t.med_category,
            t.mar_action_category,
            t.med_dose,
            t.med_dose_unit
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.hospitalization_id, t.start_dttm, t.admin_dttm, t.med_category
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
    return (pressor_events_deduped,)


@app.cell
def _(cohort_vitals, pressor_events_deduped, pressor_preferred_units):
    from clifpy.utils.unit_converter import convert_dose_units_by_med_category

    # Unit conversion on deduped data (least data to process)
    with track_memory('pressor_unit_convert'):
        pressor_events, _ = convert_dose_units_by_med_category(
            med_df=pressor_events_deduped,
            vitals_df=cohort_vitals,
            return_rel=True,
            preferred_units=pressor_preferred_units,
            override=True
        )
    return (pressor_events,)


@app.cell
def _(pressor_events):
    with track_memory('pressor_events'):
        pressor_events.df()
    return


@app.cell
def _(pressor_events):
    # Pivot only epi + norepi to wide format (need concurrent sum)
    epi_ne_wide = mo.sql(
        f"""
        FROM pressor_events
        PIVOT (
            ANY_VALUE(med_dose)
            FOR med_category IN (
                'norepinephrine' AS norepi_raw,
                'epinephrine' AS epi_raw
            )
            GROUP BY hospitalization_id, start_dttm, admin_dttm
        )
        -- WHERE med_category IN ('norepinephrine', 'epinephrine')
        """
    )
    return (epi_ne_wide,)


@app.cell
def _(epi_ne_wide):
    # Forward-fill epi + norepi only
    epi_ne_filled = mo.sql(
        f"""
        FROM epi_ne_wide
        SELECT
            hospitalization_id,
            start_dttm,
            admin_dttm,
            COALESCE(LAST_VALUE(norepi_raw IGNORE NULLS) OVER w, 0) AS norepi,
            COALESCE(LAST_VALUE(epi_raw IGNORE NULLS) OVER w, 0) AS epi
        WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        """
    )
    return (epi_ne_filled,)


@app.cell
def _(epi_ne_filled):
    # Duration validation for epi + norepi only (SOFA-2 spec 8: >=60min)
    epi_ne_duration = mo.sql(
        f"""
        -- Episode detection and 60-min validation for epi + norepi only
        WITH with_lag AS (
            FROM epi_ne_filled
            SELECT
                *,
                LAG(norepi) OVER w AS prev_norepi,
                LAG(epi) OVER w AS prev_epi
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        episode_groups AS (
            -- Assign episode IDs (cumsum when transitioning from 0 to non-zero)
            FROM with_lag
            SELECT
                *,
                SUM(CASE WHEN norepi > 0 AND COALESCE(prev_norepi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS norepi_episode,
                SUM(CASE WHEN epi > 0 AND COALESCE(prev_epi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS epi_episode
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        with_episode_start AS (
            -- Get episode start time for each pressor
            FROM episode_groups
            SELECT
                *,
                FIRST_VALUE(admin_dttm) OVER (PARTITION BY hospitalization_id, start_dttm, norepi_episode ORDER BY admin_dttm) AS norepi_episode_start,
                FIRST_VALUE(admin_dttm) OVER (PARTITION BY hospitalization_id, start_dttm, epi_episode ORDER BY admin_dttm) AS epi_episode_start
        )
        -- Apply 60-min threshold
        FROM with_episode_start
        SELECT --*,
            hospitalization_id,
            start_dttm,
            admin_dttm,
            norepi,
            epi,
            norepi_episode_start,
            epi_episode_start,
            -- Validated doses (only count if episode >= 60 min)
            CASE WHEN norepi > 0 AND DATEDIFF('minute', norepi_episode_start, admin_dttm) >= 60
                 THEN norepi ELSE 0 END AS norepi_valid,
            CASE WHEN epi > 0 AND DATEDIFF('minute', epi_episode_start, admin_dttm) >= 60
                 THEN epi ELSE 0 END AS epi_valid
        -- RESUME: FIXME the calculation
        """
    )
    return (epi_ne_duration,)


@app.cell
def _(epi_ne_duration):
    # Pre-aggregate epi + norepi to one row per window (avoids inequality join)
    epi_ne_agg = mo.sql(
        f"""
        FROM epi_ne_duration
        SELECT
            hospitalization_id,
            start_dttm,
            MAX(norepi_valid + epi_valid) AS norepi_epi_sum,
            MAX(norepi_valid) AS norepi_max,
            MAX(epi_valid) AS epi_max
        GROUP BY hospitalization_id, start_dttm
        """
    )
    return (epi_ne_agg,)


@app.cell
def _(pressor_events):
    # Duration validation for dopamine + 6 other pressors in long format
    # NOTE: No forward-fill needed - works on sparse events with LAG
    other_pressor_duration = mo.sql(
        f"""
        WITH filtered AS (
            FROM pressor_events
            SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose
            WHERE med_category NOT IN ('norepinephrine', 'epinephrine')

        ),
        with_lag AS (
            FROM filtered
            SELECT
                *,
                LAG(med_dose) OVER w AS prev_dose
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm, med_category ORDER BY admin_dttm)
        ),
        episode_groups AS (
            FROM with_lag
            SELECT
                *,
                SUM(CASE WHEN med_dose > 0 AND COALESCE(prev_dose, 0) = 0 THEN 1 ELSE 0 END) OVER w AS episode_id
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm, med_category ORDER BY admin_dttm)
        ),
        with_start AS (
            FROM episode_groups
            SELECT
                *,
                FIRST_VALUE(admin_dttm) OVER (
                    PARTITION BY hospitalization_id, start_dttm, med_category, episode_id
                    ORDER BY admin_dttm
                ) AS episode_start
        )
        -- Apply 60-min threshold (spec 8)
        FROM with_start
        SELECT
            hospitalization_id,
            start_dttm,
            admin_dttm,
            med_category,
            med_dose,
            CASE WHEN med_dose > 0 AND DATEDIFF('minute', episode_start, admin_dttm) >= 60
                 THEN med_dose ELSE 0 END AS dose_valid
        """
    )
    return (other_pressor_duration,)


@app.cell
def _(other_pressor_duration):
    # Pre-aggregate dopamine + others to one row per window
    other_pressor_agg = mo.sql(
        f"""
        FROM other_pressor_duration
        SELECT
            hospitalization_id,
            start_dttm,
            MAX(dose_valid) FILTER (WHERE med_category = 'dopamine') AS dopamine_max,
            CASE WHEN MAX(dose_valid) FILTER (WHERE med_category != 'dopamine') > 0
                 THEN 1 ELSE 0 END AS has_other_non_dopa
        GROUP BY hospitalization_id, start_dttm
        """
    )
    return (other_pressor_agg,)


@app.cell
def _(cohort_df, epi_ne_agg, other_pressor_agg):
    # Simple equality join of pre-aggregated tables (no expensive inequality conditions)
    pressor_agg = mo.sql(
        f"""
        FROM cohort_df c
        LEFT JOIN epi_ne_agg ne USING (hospitalization_id, start_dttm)
        LEFT JOIN other_pressor_agg op USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id,
            c.start_dttm,
            COALESCE(ne.norepi_epi_sum, 0) AS norepi_epi_sum,
            COALESCE(ne.norepi_max, 0) AS norepi_max,
            COALESCE(ne.epi_max, 0) AS epi_max,
            COALESCE(op.dopamine_max, 0) AS dopamine_max,
            COALESCE(op.has_other_non_dopa, 0) AS has_other_non_dopa
        """
    )
    return (pressor_agg,)


@app.cell
def _(cohort_df, map_agg, pressor_agg):
    cv_score = mo.sql(
        f"""
        -- Cardiovascular score with spec 8 (all pressors 60min) and spec 10 (dopamine-only)
        FROM cohort_df c
        LEFT JOIN map_agg m USING (hospitalization_id, start_dttm)
        LEFT JOIN pressor_agg v USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id,
            c.start_dttm,  -- window identity
            -- Define aliases (DuckDB allows reuse in same SELECT)
            COALESCE(v.norepi_epi_sum, 0) AS norepi_epi_sum,
            COALESCE(v.dopamine_max, 0) AS dopamine_max,
            COALESCE(v.has_other_non_dopa, 0) AS has_other_non_dopa,
            -- Composite "other" flag (dopamine OR other non-dopa)
            CASE WHEN COALESCE(v.dopamine_max, 0) > 0 OR COALESCE(v.has_other_non_dopa, 0) = 1
                 THEN 1 ELSE 0 END AS has_other_vaso,
            m.map_min,
            -- CV Score with spec 10 dopamine-only scoring
            cardiovascular: CASE
                -- Spec 10: Dopamine-only scoring (no norepi/epi, no other pressors)
                WHEN norepi_epi_sum = 0 AND has_other_non_dopa = 0 AND dopamine_max > 40 THEN 4
                WHEN norepi_epi_sum = 0 AND has_other_non_dopa = 0 AND dopamine_max > 20 THEN 3
                WHEN norepi_epi_sum = 0 AND has_other_non_dopa = 0 AND dopamine_max > 0 THEN 2
                -- Standard norepi/epi scoring
                WHEN norepi_epi_sum > 0.4 THEN 4
                WHEN norepi_epi_sum > 0.2 AND has_other_vaso = 1 THEN 4
                WHEN norepi_epi_sum > 0.2 THEN 3
                WHEN norepi_epi_sum > 0 AND has_other_vaso = 1 THEN 3
                WHEN norepi_epi_sum > 0 THEN 2
                WHEN has_other_vaso = 1 THEN 2
                -- No pressor (norepi_epi_sum = 0 and has_other_vaso = 0)
                WHEN map_min < 70 THEN 1
                WHEN map_min >= 70 THEN 0
                ELSE NULL
            END
        """
    )
    return (cv_score,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Respiratory subscore
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
def _(cohort_df, cv_score, gcs_agg, labs_agg, resp_score, rrt_flag):
    sofa_scores = mo.sql(
        f"""
        -- CRITICAL: LEFT JOIN from cohort_df to preserve all cohort rows
        -- Windows without data → NULL scores (expected behavior)
        -- EXPLAIN ANALYZE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id, start_dttm)
        LEFT JOIN rrt_flag r USING (hospitalization_id, start_dttm)
        LEFT JOIN gcs_agg g USING (hospitalization_id, start_dttm)
        LEFT JOIN cv_score cv USING (hospitalization_id, start_dttm)
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
