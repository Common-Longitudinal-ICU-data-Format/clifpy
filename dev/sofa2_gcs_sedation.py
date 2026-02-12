import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium", sql_output="pandas")

with app.setup:
    import marimo as mo
    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    COHORT_SIZE = 1000
    VIZ_N = 20
    CONFIG_PATH = 'config/mimic_config.yaml'

    SEDATION_DRUGS = [
        'propofol', 'dexmedetomidine', 'ketamine', 'midazolam',
        'fentanyl', 'hydromorphone', 'morphine', 'remifentanil',
        'pentobarbital', 'lorazepam',
    ]
    SEDATION_DRUGS_TUPLE = tuple(SEDATION_DRUGS)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # SOFA-2 Brain QA: Sedation vs GCS Alignment

    **Footnote c** of SOFA-2 states: *for sedated patients, use the last GCS before sedation;
    if unknown, score 0.*

    The current implementation finds the earliest sedation drug admin per window and
    invalidates all subsequent GCS. This notebook explores whether GCS is commonly
    administered before sedation begins — if not, many windows would score brain=0,
    which could systematically affect scores.

    **Key questions:**

    - How often is GCS measured before sedation onset?

    - How often is GCS measured during active sedation episodes?

    - What is the typical gap between consecutive GCS measurements?
    """)
    return


@app.cell
def _():
    from clifpy import load_data

    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    hosp_rel = load_data('hospitalization', config_path=CONFIG_PATH, return_rel=True)
    assessments_rel = load_data('patient_assessments_raw_gcs', config_path=CONFIG_PATH, return_rel=True)
    cont_meds_rel = load_data('medication_admin_continuous', config_path=CONFIG_PATH, return_rel=True)

    try:
        intm_meds_rel = load_data(
            'medication_admin_intermittent', config_path=CONFIG_PATH, return_rel=True
        )
    except Exception:
        import duckdb
        intm_meds_rel = duckdb.sql("""
            SELECT
                NULL::VARCHAR AS hospitalization_id
                , NULL::TIMESTAMP AS admin_dttm
                , NULL::VARCHAR AS med_category
                , NULL::DOUBLE AS med_dose
                , NULL::VARCHAR AS mar_action_category
            WHERE 1 = 0
        """)
    return adt_rel, assessments_rel, hosp_rel


@app.cell
def _(adt_rel):
    cohort_df = mo.sql(
        f"""
        -- ICU admissions cohort
        FROM adt_rel
        SELECT hospitalization_id, in_dttm AS icu_start, out_dttm AS icu_end
        WHERE location_category = 'icu'
        LIMIT {COHORT_SIZE}
        """
    )
    return (cohort_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Sedation Episode Detection

    Continuous sedation drugs are processed through an episode detection pipeline:
    MAR dedup → binary `is_sedated` → LAG transition detection → cumulative SUM for episode IDs.

    Every individual MAR timestamp is preserved with its episode ID.
    """)
    return


@app.cell
def _():
    cont_sedation_df = mo.sql(
        f"""
        -- Continuous sedation events with episode detection
        -- Adapted from _detect_sedation_episodes() in _utils.py
        WITH raw_events AS (
            -- All continuous sedation drug admins within ICU stays
            FROM cont_meds_rel t
            JOIN cohort_df c ON
                t.hospitalization_id = c.hospitalization_id
                AND t.admin_dttm >= c.icu_start
                AND t.admin_dttm <= c.icu_end
            SELECT
                t.hospitalization_id
                , t.admin_dttm AS dttm
                , t.med_category AS sedation_category
                , t.med_dose AS sedation_dose
                , t.mar_action_category
            WHERE t.med_category IN {SEDATION_DRUGS_TUPLE}
        ),
        deduped AS (
            -- MAR deduplication (priority-based, from _utils.py)
            FROM raw_events
            SELECT *
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY hospitalization_id, dttm, sedation_category
                ORDER BY
                    CASE WHEN mar_action_category IS NULL THEN 10
                        WHEN mar_action_category IN ('verify', 'not_given') THEN 9
                        WHEN mar_action_category = 'stop' THEN 8
                        WHEN mar_action_category = 'going' THEN 7
                        ELSE 1 END,
                    CASE WHEN sedation_dose > 0 THEN 1 ELSE 2 END,
                    sedation_dose DESC
            ) = 1
        ),
        collapsed AS (
            -- Collapse across drug categories to binary is_sedated per timestamp
            FROM deduped
            SELECT
                hospitalization_id
                , dttm
                , is_sedated: MAX(CASE
                    WHEN sedation_dose > 0 AND mar_action_category != 'stop' THEN 1
                    ELSE 0 END)
            GROUP BY hospitalization_id, dttm
        ),
        with_lag AS (
            FROM collapsed
            SELECT
                *
                , prev_is_sedated: LAG(is_sedated) OVER (
                    PARTITION BY hospitalization_id ORDER BY dttm)
        ),
        episode_mapping AS (
            -- Assign episode IDs via off→on transitions
            FROM with_lag
            SELECT
                hospitalization_id
                , dttm
                , is_sedated
                , sedation_episode_id: SUM(
                    CASE WHEN is_sedated = 1 AND COALESCE(prev_is_sedated, 0) = 0
                    THEN 1 ELSE 0 END
                ) OVER (PARTITION BY hospitalization_id ORDER BY dttm)
        )
        -- Join episode_id back to individual deduped rows
        FROM deduped d
        LEFT JOIN episode_mapping e USING (hospitalization_id, dttm)
        SELECT
            d.hospitalization_id
            , d.dttm
            , d.sedation_category
            , d.sedation_dose
            , d.mar_action_category
            , e.sedation_episode_id
            , e.is_sedated
        ORDER BY d.hospitalization_id, d.dttm, d.sedation_category
        """
    )
    return


@app.cell
def _():
    intm_sedation_df = mo.sql(
        f"""
        -- Intermittent sedation drug admins (point events, no episode structure)
        FROM intm_meds_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.icu_start
            AND t.admin_dttm <= c.icu_end
        SELECT
            t.hospitalization_id
            , t.admin_dttm AS dttm
            , t.med_category AS sedation_category
            , t.med_dose AS sedation_dose
            , t.mar_action_category
            , NULL::BIGINT AS sedation_episode_id
            , 1 AS is_sedated
        WHERE t.med_category IN {SEDATION_DRUGS_TUPLE}
            AND t.med_dose > 0
            AND t.mar_action_category != 'not_given'
        ORDER BY t.hospitalization_id, t.admin_dttm
        """
    )
    return (intm_sedation_df,)


@app.cell
def _(intm_sedation_df):
    all_sedation_df = mo.sql(
        f"""
        -- Combined sedation events with source label
        FROM (
            FROM cont_sedation_df
            SELECT hospitalization_id, dttm, sedation_category, sedation_dose
                , mar_action_category, sedation_episode_id, is_sedated
                , source: 'continuous'
            UNION ALL
            FROM intm_sedation_df
            SELECT hospitalization_id, dttm, sedation_category, sedation_dose
                , mar_action_category, sedation_episode_id, is_sedated
                , source: 'intermittent'
        )
        SELECT *
        ORDER BY hospitalization_id, dttm
        """
    )
    return (all_sedation_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## GCS Measurements

    Extract `gcs_total` and `gcs_motor` from patient assessments, pivoted to one row per timestamp.
    """)
    return


@app.cell
def _(assessments_rel, cohort_df):
    gcs_df = mo.sql(
        f"""
        -- GCS measurements within ICU stays, pivoted
        FROM assessments_rel t
        JOIN cohort_df c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.icu_start
            AND t.recorded_dttm <= c.icu_end
        SELECT
            t.hospitalization_id
            , t.recorded_dttm AS dttm
            , gcs_total: MAX(t.numerical_value)
                FILTER (t.assessment_category = 'gcs_total')
            , gcs_motor: MAX(t.numerical_value)
                FILTER (t.assessment_category = 'gcs_motor')
        WHERE t.assessment_category IN ('gcs_total', 'gcs_motor')
        GROUP BY t.hospitalization_id, t.recorded_dttm
        ORDER BY t.hospitalization_id, t.recorded_dttm
        """
    )
    return (gcs_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Combined Timeline

    Each row is one event — either a sedation admin or a GCS measurement — ordered by time.
    """)
    return


@app.cell
def _(all_sedation_df, gcs_df, hosp_rel):
    timeline_df = mo.sql(
        f"""
        -- Combined timeline of sedation and GCS events
        WITH events AS (
            FROM all_sedation_df
            SELECT
                hospitalization_id
                , dttm
                , sedation_category
                , sedation_dose
                , mar_action_category
                , sedation_episode_id
                , NULL::DOUBLE AS gcs_total
                , NULL::DOUBLE AS gcs_motor
            UNION ALL
            FROM gcs_df
            SELECT
                hospitalization_id
                , dttm
                , NULL AS sedation_category
                , NULL::DOUBLE AS sedation_dose
                , NULL AS mar_action_category
                , NULL::BIGINT AS sedation_episode_id
                , gcs_total
                , gcs_motor
        )
        FROM events e
        LEFT JOIN hosp_rel h USING (hospitalization_id)
        SELECT
            h.patient_id
            , e.hospitalization_id
            , e.dttm
            , e.sedation_category
            , e.sedation_dose
            , e.mar_action_category
            , e.sedation_episode_id
            , e.gcs_total
            , e.gcs_motor
        ORDER BY e.hospitalization_id, e.dttm
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Visualization — Sedation Timeline with GCS

    Per-patient subplots showing:

    - **Left y-axis**: Sedation drug doses as step lines (color-coded by drug)

    - **Right y-axis**: GCS measurements as discrete markers
      (circles = `gcs_total`, triangles = `gcs_motor`)

    - **Gray shading**: Active sedation episodes
    """)
    return


@app.cell
def _(all_sedation_df):
    # Pick hospitalizations with sedation data for meaningful visualization
    _sedated_ids = all_sedation_df['hospitalization_id'].unique().tolist()
    select_hosp_ids = _sedated_ids[:VIZ_N]
    return (select_hosp_ids,)


@app.cell
def _(all_sedation_df, cohort_df, gcs_df, select_hosp_ids):
    import matplotlib.pyplot as plt

    # Filter to selected hospitalizations
    sed_plot = all_sedation_df[
        all_sedation_df['hospitalization_id'].isin(select_hosp_ids)
    ].copy()
    gcs_plot = gcs_df[
        gcs_df['hospitalization_id'].isin(select_hosp_ids)
    ].copy()
    cohort_plot = cohort_df[
        cohort_df['hospitalization_id'].isin(select_hosp_ids)
    ]

    # Merge ICU start time and compute hours since admission
    sed_plot = sed_plot.merge(
        cohort_plot[['hospitalization_id', 'icu_start']], on='hospitalization_id'
    )
    sed_plot['hrs'] = (
        sed_plot['dttm'] - sed_plot['icu_start']
    ).dt.total_seconds() / 3600

    gcs_plot = gcs_plot.merge(
        cohort_plot[['hospitalization_id', 'icu_start']], on='hospitalization_id'
    )
    gcs_plot['hrs'] = (
        gcs_plot['dttm'] - gcs_plot['icu_start']
    ).dt.total_seconds() / 3600

    # Color map for sedation drug categories
    all_cats = sed_plot['sedation_category'].dropna().unique()
    cmap_colors = plt.cm.tab10.colors
    color_map = {cat: cmap_colors[i % len(cmap_colors)] for i, cat in enumerate(all_cats)}

    fig, axes = plt.subplots(
        nrows=len(select_hosp_ids),
        ncols=1,
        figsize=(14, 4 * len(select_hosp_ids)),
        squeeze=False,
    )

    for row_idx, hosp_id in enumerate(select_hosp_ids):
        ax_left = axes[row_idx, 0]
        ax_right = ax_left.twinx()

        hosp_sed = sed_plot[sed_plot['hospitalization_id'] == hosp_id]

        # --- Left axis: Continuous sedation drugs (step lines + scatter dots) ---
        cont = hosp_sed[hosp_sed['source'] == 'continuous']
        for cat in cont['sedation_category'].dropna().unique():
            cat_df = cont[cont['sedation_category'] == cat].sort_values('hrs')
            color = color_map.get(cat, 'gray')

            # Step line (dose holds until next observation)
            ax_left.step(
                cat_df['hrs'], cat_df['sedation_dose'],
                where='post', color=color, alpha=0.7, linewidth=1.5, label=cat,
            )

            # Dots at active admin timestamps, X marks at stops
            active = cat_df[cat_df['mar_action_category'] != 'stop']
            stopped = cat_df[cat_df['mar_action_category'] == 'stop']
            ax_left.scatter(
                active['hrs'], active['sedation_dose'],
                c=[color], alpha=0.5, s=15, marker='o',
            )
            if len(stopped) > 0:
                ax_left.scatter(
                    stopped['hrs'], stopped['sedation_dose'],
                    c=[color], alpha=0.7, s=30, marker='x',
                )

        # --- Left axis: Intermittent sedation drugs (diamond markers) ---
        intm = hosp_sed[hosp_sed['source'] == 'intermittent']
        for cat in intm['sedation_category'].dropna().unique():
            cat_df = intm[intm['sedation_category'] == cat]
            color = color_map.get(cat, 'gray')
            ax_left.scatter(
                cat_df['hrs'], cat_df['sedation_dose'],
                c=[color], alpha=0.8, s=50, marker='D',
                label=f'{cat} (intm)', edgecolors='black', linewidth=0.5,
            )

        # --- Episode shading (continuous only, is_sedated=1) ---
        ep_data = cont[cont['is_sedated'] == 1]
        if len(ep_data) > 0:
            ep_bounds = ep_data.groupby('sedation_episode_id').agg(
                start=('hrs', 'min'), end=('hrs', 'max'),
            ).reset_index()
            for _, ep in ep_bounds.iterrows():
                ax_left.axvspan(ep['start'], ep['end'], alpha=0.08, color='gray')

        # --- Right axis: GCS scores ---
        hosp_gcs = gcs_plot[gcs_plot['hospitalization_id'] == hosp_id]

        # --- 24-hour day markers ---
        all_hrs = list(hosp_sed['hrs']) + list(hosp_gcs['hrs'])
        if all_hrs:
            max_hr = max(all_hrs)
            day_hr = 24
            while day_hr <= max_hr:
                ax_left.axvline(
                    x=day_hr, color='red', linestyle='--', alpha=0.4, linewidth=0.8,
                )
                day_hr += 24

        # gcs_total: letter G markers + vertical stems
        gt = hosp_gcs.dropna(subset=['gcs_total'])
        if len(gt) > 0:
            ax_right.vlines(
                gt['hrs'], ymin=0, ymax=gt['gcs_total'],
                colors='black', alpha=0.3, linewidth=1,
            )
            ax_right.scatter(
                gt['hrs'], gt['gcs_total'],
                c='black', marker='$G$', s=60, alpha=0.8, label='gcs_total', zorder=5,
            )

        # gcs_motor: letter g markers
        gm = hosp_gcs.dropna(subset=['gcs_motor'])
        if len(gm) > 0:
            ax_right.scatter(
                gm['hrs'], gm['gcs_motor'],
                c='black', marker='$g$', s=60, alpha=0.8, label='gcs_motor', zorder=5,
            )

        ax_left.set_xlabel('Hours since ICU admission')
        ax_left.set_ylabel('Sedation Dose')
        ax_right.set_ylabel('GCS Score', color='black')
        ax_right.set_ylim(0, 16)
        ax_left.set_title(f'Hosp ID: {hosp_id}')

        # Combined legend
        h_l, l_l = ax_left.get_legend_handles_labels()
        h_r, l_r = ax_right.get_legend_handles_labels()
        if h_l or h_r:
            ax_left.legend(
                h_l + h_r, l_l + l_r, loc='upper right', fontsize=7,
            )

    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Summary Statistics — Footnote c Impact
    """)
    return


@app.cell
def _(all_sedation_df, cohort_df, gcs_df):
    summary_stats_df = mo.sql(
        f"""
        -- Key metrics quantifying the footnote c concern
        WITH sedated_stays AS (
            FROM all_sedation_df
            SELECT DISTINCT hospitalization_id
        ),
        gcs_stays AS (
            FROM gcs_df
            SELECT DISTINCT hospitalization_id
        ),
        first_sedation AS (
            FROM all_sedation_df
            SELECT hospitalization_id
                , first_sedation_dttm: MIN(dttm)
            GROUP BY hospitalization_id
        ),
        gcs_before_sed AS (
            -- Hospitalizations with at least one GCS before first sedation
            FROM gcs_df g
            JOIN first_sedation s USING (hospitalization_id)
            SELECT DISTINCT g.hospitalization_id
            WHERE g.dttm < s.first_sedation_dttm
        ),
        gcs_gaps AS (
            -- Time between consecutive GCS measurements (hours)
            FROM gcs_df
            SELECT
                hospitalization_id
                , gap_hours: EXTRACT(EPOCH FROM (
                    dttm - LAG(dttm) OVER (
                        PARTITION BY hospitalization_id ORDER BY dttm)
                )) / 3600
        )
        SELECT
            n_total_stays: (SELECT COUNT(DISTINCT hospitalization_id) FROM cohort_df)
            , n_stays_with_sedation: (SELECT COUNT(*) FROM sedated_stays)
            , n_stays_with_gcs: (SELECT COUNT(*) FROM gcs_stays)
            , n_sedated_with_pre_sed_gcs: (SELECT COUNT(*) FROM gcs_before_sed)
            , n_sedated_no_pre_sed_gcs:
                (SELECT COUNT(*) FROM sedated_stays)
                - (SELECT COUNT(*) FROM gcs_before_sed)
            , pct_sedated_no_pre_sed_gcs: ROUND(
                ((SELECT COUNT(*) FROM sedated_stays)
                 - (SELECT COUNT(*) FROM gcs_before_sed))::DOUBLE
                / NULLIF((SELECT COUNT(*) FROM sedated_stays), 0) * 100, 1)
            , median_gcs_gap_hours: (
                SELECT ROUND(MEDIAN(gap_hours), 1)
                FROM gcs_gaps WHERE gap_hours IS NOT NULL)
            , p25_gcs_gap_hours: (
                SELECT ROUND(QUANTILE_CONT(gap_hours, 0.25), 1)
                FROM gcs_gaps WHERE gap_hours IS NOT NULL)
            , p75_gcs_gap_hours: (
                SELECT ROUND(QUANTILE_CONT(gap_hours, 0.75), 1)
                FROM gcs_gaps WHERE gap_hours IS NOT NULL)
        """
    )
    return


@app.cell
def _(all_sedation_df, gcs_df):
    gcs_sedation_overlap_df = mo.sql(
        f"""
        -- GCS during/after sedation: quantify overlap
        WITH episode_bounds AS (
            -- Episode bounds from continuous sedation events
            FROM all_sedation_df
            SELECT
                hospitalization_id
                , sedation_episode_id
                , episode_start: MIN(dttm)
                , episode_end: MAX(dttm)
            WHERE source = 'continuous'
                AND is_sedated = 1
                AND sedation_episode_id IS NOT NULL
            GROUP BY hospitalization_id, sedation_episode_id
        ),
        gcs_during_episode AS (
            -- GCS measurements that fall during a continuous sedation episode
            FROM gcs_df g
            JOIN episode_bounds e ON
                g.hospitalization_id = e.hospitalization_id
                AND g.dttm >= e.episode_start
                AND g.dttm <= e.episode_end
            SELECT g.hospitalization_id, g.dttm, e.sedation_episode_id
        ),
        intm_admins AS (
            FROM all_sedation_df
            SELECT hospitalization_id, dttm AS intm_dttm
            WHERE source = 'intermittent'
        ),
        gcs_after_intm AS (
            -- First GCS after each intermittent sedation admin
            FROM intm_admins i
            JOIN gcs_df g ON
                i.hospitalization_id = g.hospitalization_id
                AND g.dttm > i.intm_dttm
            SELECT
                i.hospitalization_id
                , i.intm_dttm
                , g.dttm AS gcs_dttm
                , gap_hours: EXTRACT(EPOCH FROM (g.dttm - i.intm_dttm)) / 3600
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY i.hospitalization_id, i.intm_dttm
                ORDER BY g.dttm
            ) = 1
        )
        SELECT
            n_gcs_during_cont_episode: (SELECT COUNT(*) FROM gcs_during_episode)
            , n_unique_episodes_with_gcs: (
                SELECT COUNT(DISTINCT sedation_episode_id) FROM gcs_during_episode)
            , n_total_cont_episodes: (SELECT COUNT(*) FROM episode_bounds)
            , n_intm_admins: (SELECT COUNT(*) FROM intm_admins)
            , n_intm_with_gcs_within_6h: (
                SELECT COUNT(*) FROM gcs_after_intm WHERE gap_hours <= 6)
            , median_intm_to_gcs_hours: (
                SELECT ROUND(MEDIAN(gap_hours), 1) FROM gcs_after_intm)
            , p25_intm_to_gcs_hours: (
                SELECT ROUND(QUANTILE_CONT(gap_hours, 0.25), 1) FROM gcs_after_intm)
            , p75_intm_to_gcs_hours: (
                SELECT ROUND(QUANTILE_CONT(gap_hours, 0.75), 1) FROM gcs_after_intm)
        """
    )
    return


if __name__ == "__main__":
    app.run()
