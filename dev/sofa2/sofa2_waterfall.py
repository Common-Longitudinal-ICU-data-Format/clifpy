import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium", sql_output="pandas")

with app.setup:
    import marimo as mo
    import duckdb

    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    PREFIX = ''
    CONFIG_PATH = PREFIX + 'config/mimic_config.yaml'


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # FiO2 Forward-Fill Sensitivity Analysis

    Compare respiratory subscore results **with vs. without** FiO2 forward-fill
    (`_forward_fill_fio2`) to evaluate its impact on P/F ratio matching and
    resp subscore accuracy.

    See `.dev/sofa2_waterfall.md` for the full waterfall comparison across implementations.
    """)
    return


@app.cell
def _():
    from clifpy import load_data
    from clifpy.utils.sofa2._utils import SOFA2Config
    from clifpy.utils.sofa2._core import _load_ecmo_optional

    labs_rel = load_data('labs', config_path=CONFIG_PATH, return_rel=True)
    vitals_rel = load_data('vitals', config_path=CONFIG_PATH, return_rel=True)
    resp_rel = load_data('respiratory_support', config_path=CONFIG_PATH, return_rel=True)
    ecmo_rel = _load_ecmo_optional(CONFIG_PATH)

    cfg = SOFA2Config()
    return cfg, ecmo_rel, labs_rel, load_data, resp_rel, vitals_rel


@app.cell
def _(load_data):
    adt_rel = load_data('adt', config_path=CONFIG_PATH, return_rel=True)
    return (adt_rel,)


@app.cell
def _(adt_rel):
    cohort_df = mo.sql(
        f"""
        -- Create cohort: ICU admissions with 24h windows
        FROM adt_rel a
        SELECT
            hospitalization_id
            , in_dttm AS start_dttm
            , in_dttm + INTERVAL '24 hours' AS end_dttm
        WHERE location_category = 'icu'
        """
    )
    return (cohort_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Baseline: Raw resp_rel (no forward-fill, no device heuristic)

    Run `_calculate_resp_subscore` with the **original** resp_rel before any preprocessing.
    This represents the previous SOFA-2 behavior.
    """)
    return


@app.cell
def _(cfg, cohort_df, ecmo_rel, labs_rel, resp_rel, vitals_rel):
    from clifpy.utils.sofa2._resp import _calculate_resp_subscore

    cohort_rel = duckdb.sql("FROM cohort_df SELECT *")

    resp_baseline, intm_baseline = _calculate_resp_subscore(
        cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg, dev=True
    )

    # Extract intermediate relations for downstream SQL cells
    concurrent_pf_baseline = intm_baseline['concurrent_pf']
    concurrent_sf_baseline = intm_baseline['concurrent_sf']
    return concurrent_pf_baseline, concurrent_sf_baseline, resp_baseline


@app.cell
def _(concurrent_pf_baseline):
    concurrent_pf_baseline
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## With preprocessing: Device heuristic + FiO2 forward-fill

    Apply Step 0 (IMV inference from mode_category) and Step 0b (`_forward_fill_fio2`)
    before running the resp subscore.
    """)
    return


@app.cell
def _(cfg, cohort_df, ecmo_rel, labs_rel, resp_rel, vitals_rel):
    from clifpy.utils.sofa2._resp import _forward_fill_fio2, _calculate_resp_subscore as _calc_resp

    cohort_rel_ff = duckdb.sql("FROM cohort_df SELECT *")

    # Step 0: Device heuristic
    resp_rel_enriched = duckdb.sql("""
        FROM resp_rel
        SELECT * REPLACE (
            CASE
                WHEN device_category IS NULL
                     AND mode_category IS NOT NULL
                     AND regexp_matches(
                         mode_category,
                         '(?:assist control-volume control|simv|pressure control)',
                         'i'
                     )
                THEN 'imv'
                ELSE device_category
            END AS device_category
        )
    """)

    # Step 0b: Forward-fill FiO2
    resp_rel_ffilled = _forward_fill_fio2(resp_rel_enriched)

    resp_ffilled, intm_ffilled = _calc_resp(
        cohort_rel_ff, resp_rel_ffilled, labs_rel, vitals_rel, ecmo_rel, cfg, dev=True
    )

    # Extract intermediate relations for downstream SQL cells
    concurrent_pf_ffilled = intm_ffilled['concurrent_pf']
    concurrent_sf_ffilled = intm_ffilled['concurrent_sf']
    return concurrent_pf_ffilled, concurrent_sf_ffilled, resp_ffilled


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Comparison: P/F match counts

    How many PaO2 measurements successfully matched with an FiO2 (within 4h tolerance)?
    """)
    return


@app.cell
def _(concurrent_pf_baseline, concurrent_pf_ffilled):
    pf_count_comparison = mo.sql(
        f"""
        -- Compare number of concurrent P/F matches
        WITH baseline AS (
            FROM concurrent_pf_baseline
            SELECT COUNT(*) AS n_pf_baseline
        ),
        ffilled AS (
            FROM concurrent_pf_ffilled
            SELECT COUNT(*) AS n_pf_ffilled
        )
        FROM baseline CROSS JOIN ffilled
        SELECT
            n_pf_baseline
            , n_pf_ffilled
            , n_pf_ffilled - n_pf_baseline AS pf_gained
            , ROUND(100.0 * (n_pf_ffilled - n_pf_baseline) / NULLIF(n_pf_baseline, 0), 2) AS pct_increase
        """
    )
    return


@app.cell
def _(concurrent_sf_baseline, concurrent_sf_ffilled):
    sf_count_comparison = mo.sql(
        f"""
        -- Compare number of concurrent S/F matches
        WITH baseline AS (
            FROM concurrent_sf_baseline
            SELECT COUNT(*) AS n_sf_baseline
        ),
        ffilled AS (
            FROM concurrent_sf_ffilled
            SELECT COUNT(*) AS n_sf_ffilled
        )
        FROM baseline CROSS JOIN ffilled
        SELECT
            n_sf_baseline
            , n_sf_ffilled
            , n_sf_ffilled - n_sf_baseline AS sf_gained
            , ROUND(100.0 * (n_sf_ffilled - n_sf_baseline) / NULLIF(n_sf_baseline, 0), 2) AS pct_increase
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Comparison: Resp subscore changes

    Join baseline and forward-filled results to see which windows changed score.
    """)
    return


@app.cell
def _(resp_baseline, resp_ffilled):
    score_comparison = mo.sql(
        f"""
        -- Compare resp subscores between baseline and forward-filled
        FROM resp_baseline b
        JOIN resp_ffilled f USING (hospitalization_id, start_dttm)
        SELECT
            b.hospitalization_id
            , b.start_dttm
            , b.resp AS resp_baseline
            , f.resp AS resp_ffilled
            , f.resp - b.resp AS score_diff
            , b.pf_ratio AS pf_baseline
            , f.pf_ratio AS pf_ffilled
            , b.sf_ratio AS sf_baseline
            , f.sf_ratio AS sf_ffilled
            , b.has_advanced_support AS adv_baseline
            , f.has_advanced_support AS adv_ffilled
        """
    )
    return (score_comparison,)


@app.cell
def _(score_comparison):
    score_change_summary = mo.sql(
        f"""
        -- Summary of score changes
        FROM score_comparison
        SELECT
            COUNT(*) AS total_windows
            , SUM(CASE WHEN score_diff != 0 THEN 1 ELSE 0 END) AS windows_changed
            , SUM(CASE WHEN score_diff > 0 THEN 1 ELSE 0 END) AS windows_scored_higher
            , SUM(CASE WHEN score_diff < 0 THEN 1 ELSE 0 END) AS windows_scored_lower
            , SUM(CASE WHEN resp_baseline IS NULL AND resp_ffilled IS NOT NULL THEN 1 ELSE 0 END) AS windows_gained_score
            , SUM(CASE WHEN pf_baseline IS NULL AND pf_ffilled IS NOT NULL THEN 1 ELSE 0 END) AS windows_gained_pf
            , SUM(CASE WHEN sf_baseline IS NULL AND sf_ffilled IS NOT NULL THEN 1 ELSE 0 END) AS windows_gained_sf
            , SUM(CASE WHEN adv_baseline = 0 AND adv_ffilled = 1 THEN 1 ELSE 0 END) AS windows_gained_advanced_support
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Distribution of P/F ratios: Baseline vs Forward-Filled
    """)
    return


@app.cell
def _(score_comparison):
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # P/F ratio comparison
    ax = axes[0]
    pf_baseline = score_comparison['pf_baseline'].dropna()
    pf_ffilled = score_comparison['pf_ffilled'].dropna()
    ax.hist(pf_baseline, bins=50, alpha=0.5, label=f'Baseline (n={len(pf_baseline)})', color='steelblue')
    ax.hist(pf_ffilled, bins=50, alpha=0.5, label=f'Forward-filled (n={len(pf_ffilled)})', color='coral')
    ax.set_xlabel('P/F Ratio')
    ax.set_ylabel('Count')
    ax.set_title('P/F Ratio Distribution')
    ax.legend()

    # Resp score comparison
    ax = axes[1]
    resp_b = score_comparison['resp_baseline'].dropna()
    resp_f = score_comparison['resp_ffilled'].dropna()
    bins = [-0.5, 0.5, 1.5, 2.5, 3.5, 4.5]
    ax.hist(resp_b, bins=bins, alpha=0.5, label=f'Baseline (n={len(resp_b)})', color='steelblue')
    ax.hist(resp_f, bins=bins, alpha=0.5, label=f'Forward-filled (n={len(resp_f)})', color='coral')
    ax.set_xlabel('Resp Subscore')
    ax.set_ylabel('Count')
    ax.set_title('Resp Subscore Distribution')
    ax.set_xticks([0, 1, 2, 3, 4])
    ax.legend()

    plt.tight_layout()
    fig
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Windows that gained a score (NULL → non-NULL)

    These are windows where the baseline had no P/F or S/F match (resp = NULL)
    but forward-fill provided enough FiO2 density for a match.
    """)
    return


@app.cell
def _(score_comparison):
    gained_scores = mo.sql(
        f"""
        -- Windows that went from NULL to a score
        FROM score_comparison
        WHERE resp_baseline IS NULL AND resp_ffilled IS NOT NULL
        ORDER BY hospitalization_id, start_dttm
        """
    )
    return


if __name__ == "__main__":
    app.run()
