import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium", sql_output="native")

with app.setup:
    import marimo as mo
    import duckdb

    COHORT_SIZE = 100
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
def _():
    labs_agg = mo.sql(
        f"""
        -- EXPLAIN ANALYZE
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
def _():
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

    cohort_meds_rel = duckdb.sql(
        f"""
        FROM meds_rel t
        SEMI JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT *
        WHERE t.med_category IN {cohort_vasopressors}
        """
    )

    cohort_vitals_rel = duckdb.sql(
        f"""
        FROM vitals_rel t
        SEMI JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
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
    # Cell 1: Get most recent dose BEFORE start_dttm for each vasopressor (initial state)
    # Uses meds_deduped which already has MAR deduplication and vasopressor filtering
    vaso_initial_state = mo.sql(
        f"""
        FROM meds_deduped t
        JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id
        SELECT
            t.hospitalization_id,
            c.start_dttm AS admin_dttm,
            t.med_category,
            ARG_MAX(
                CASE WHEN t.mar_action_category = 'stop' THEN 0 ELSE t.med_dose END,
                t.admin_dttm
            ) AS med_dose
        WHERE t.admin_dttm < c.start_dttm
        GROUP BY t.hospitalization_id, c.start_dttm, t.med_category
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
    return


@app.cell
def _():
    vaso_events = mo.sql(
        f"""
        EXPLAIN ANALYZE
        SELECT * FROM (
            SELECT hospitalization_id, admin_dttm, med_category, med_dose
            FROM vaso_initial_state
            UNION ALL
            SELECT hospitalization_id, admin_dttm, med_category, med_dose
            FROM vaso_in_window
        )
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
            FIRST(med_dose)
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
                  OR MAX(f.milrinone) > 0 OR MAX(f.angiotensin) > 0
                  OR MAX(f.isoproterenol) > 0
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
    # Respiratory
    """)
    return


@app.cell
def _():
    # FiO2 imputation - keep individual rows for ASOF JOIN with PaO2
    # CRITICAL: Don't aggregate here - need concurrent matching with PaO2
    fio2_imputed = mo.sql(
        f"""
        FROM resp_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            t.recorded_dttm,
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
        """
    )
    return (fio2_imputed,)


@app.cell
def _():
    # Individual PaO2 measurements (keep each row for ASOF JOIN with FiO2)
    po2_measurements = mo.sql(
        f"""
        FROM labs_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_result_dttm >= c.start_dttm
            AND t.lab_result_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            t.lab_result_dttm,
            t.lab_value_numeric AS po2_arterial
        WHERE t.lab_category = 'po2_arterial'
            AND t.lab_value_numeric IS NOT NULL
        """
    )
    return (po2_measurements,)


@app.cell
def _():
    # SpO2 measurements with Severinghaus equation to impute PaO2
    # Only used when SpO2 < 97% (oxygen-hemoglobin dissociation curve too flat above this)
    # Reference: sofa_v2_polars.py lines 852-893
    spo2_imputed = mo.sql(
        f"""
        FROM vitals_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id,
            t.recorded_dttm AS lab_result_dttm,
            t.vital_value AS spo2,
            -- Severinghaus equation (simplified for DuckDB)
            -- PaO2 ≈ (11700 / ((100/SpO2) - 1))^(1/3) * 2 - 50^(1/3) * 2
            -- Approximation: PaO2 ≈ 11700 / ((100/SpO2) - 1) when SpO2 < 97%
            POWER(11700.0 / ((100.0 / t.vital_value) - 1), 0.333333) * 2 AS po2_arterial
        WHERE t.vital_category = 'spo2'
            AND t.vital_value IS NOT NULL
            AND t.vital_value < 97
            AND t.vital_value > 0
        """
    )
    return (spo2_imputed,)


@app.cell
def _(po2_measurements, spo2_imputed):
    # Combine actual PaO2 with imputed PaO2 (from SpO2)
    # Per Note 4: Use SpO2:FiO2 only when PaO2:FiO2 is unavailable
    # Strategy: Include imputed values only for hospitalizations without actual PaO2
    po2_combined = mo.sql(
        f"""
        WITH actual_po2 AS (
            FROM po2_measurements
            SELECT *, 0 AS is_imputed
        ),
        hosp_with_actual AS (
            SELECT DISTINCT hospitalization_id FROM po2_measurements
        ),
        imputed_po2 AS (
            FROM spo2_imputed s
            ANTI JOIN hosp_with_actual h ON s.hospitalization_id = h.hospitalization_id
            SELECT s.hospitalization_id, s.lab_result_dttm, s.po2_arterial, 1 AS is_imputed
        )
        FROM actual_po2
        SELECT hospitalization_id, lab_result_dttm, po2_arterial, is_imputed
        UNION ALL
        FROM imputed_po2
        SELECT hospitalization_id, lab_result_dttm, po2_arterial, is_imputed
        """
    )
    return (po2_combined,)


@app.cell
def _(fio2_imputed, po2_combined):
    # Concurrent P/F ratio using ASOF JOIN
    # Each PaO2 (actual or imputed from SpO2) is matched with the most recent FiO2 within 4-hour tolerance
    # CRITICAL: This ensures P/F ratio uses concurrent measurements, not independent aggregations
    concurrent_pf = mo.sql(
        f"""
        FROM po2_combined p
        ASOF JOIN fio2_imputed f
            ON p.hospitalization_id = f.hospitalization_id
            AND p.lab_result_dttm >= f.recorded_dttm
        SELECT
            p.hospitalization_id,
            p.lab_result_dttm,
            p.po2_arterial,
            f.fio2_imputed,
            f.device_category,
            f.is_advanced_support,
            p.po2_arterial / f.fio2_imputed AS pf_ratio
        WHERE f.fio2_imputed IS NOT NULL
            AND f.fio2_imputed > 0
            -- 4-hour tolerance (DuckDB ASOF JOIN doesn't have built-in tolerance)
            AND p.lab_result_dttm - f.recorded_dttm <= INTERVAL '240 minutes'
        """
    )
    return (concurrent_pf,)


@app.cell
def _(concurrent_pf):
    resp_agg = mo.sql(
        f"""
        -- Aggregate to worst (minimum) concurrent P/F ratio per hospitalization
        -- Use ARG_MIN to get the device/support status at the worst P/F
        FROM concurrent_pf
        SELECT
            hospitalization_id,
            -- Worst (minimum) concurrent P/F ratio
            MIN(pf_ratio) AS pf_ratio,
            -- Device at worst P/F (for advanced support check)
            ARG_MIN(device_category, pf_ratio) AS device_category,
            ARG_MIN(is_advanced_support, pf_ratio) AS has_advanced_support,
            -- For debugging
            ARG_MIN(po2_arterial, pf_ratio) AS po2_at_worst_pf,
            ARG_MIN(fio2_imputed, pf_ratio) AS fio2_at_worst_pf
        GROUP BY hospitalization_id
        """
    )
    return (resp_agg,)


@app.cell
def _(ecmo_rel, cohort_df):
    # ECMO flag for respiratory scoring (Note 7)
    # Per SOFA-2: ECMO for respiratory failure = 4 points regardless of P/F ratio
    ecmo_flag = mo.sql(
        f"""
        FROM ecmo_rel t
        SEMI JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT
            t.hospitalization_id,
            1 AS has_ecmo
        """
    )
    return (ecmo_flag,)


@app.cell
def _(ecmo_flag, resp_agg):
    # Respiratory score calculation with ECMO override
    resp_score = mo.sql(
        f"""
        FROM resp_agg r
        LEFT JOIN ecmo_flag e USING (hospitalization_id)
        SELECT
            r.hospitalization_id,
            r.pf_ratio,
            r.has_advanced_support,
            r.device_category,
            r.po2_at_worst_pf,
            r.fio2_at_worst_pf,
            COALESCE(e.has_ecmo, 0) AS has_ecmo,
            -- Respiratory Score
            respiratory: CASE
                -- ECMO override (Note 7): 4 points regardless of P/F ratio
                WHEN COALESCE(e.has_ecmo, 0) = 1 THEN 4
                -- Score 4: P/F <=75 AND advanced support
                WHEN r.pf_ratio <= 75 AND r.has_advanced_support = 1 THEN 4
                -- Score 3: P/F <=150 AND advanced support
                WHEN r.pf_ratio <= 150 AND r.has_advanced_support = 1 THEN 3
                -- Score 2: P/F <=225 (no vent requirement)
                WHEN r.pf_ratio <= 225 THEN 2
                -- Score 1: P/F <=300
                WHEN r.pf_ratio <= 300 THEN 1
                -- Score 0: P/F >300
                WHEN r.pf_ratio > 300 THEN 0
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
        -- EXPLAIN ANALYZE
        FROM cohort_df c
        LEFT JOIN labs_agg l USING (hospitalization_id)
        LEFT JOIN rrt_flag r USING (hospitalization_id)
        LEFT JOIN gcs_agg g USING (hospitalization_id)
        LEFT JOIN cv_agg cv USING (hospitalization_id)
        LEFT JOIN resp_score resp USING (hospitalization_id)
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
