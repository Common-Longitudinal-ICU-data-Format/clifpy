"""Step 2 -- when is a usable patient weight actually available?

For each drug under study, every hospitalization that received it is placed in
exactly one of nine scenarios. The first five are the ones that determine
whether a forced unweighted -> weighted conversion can succeed at all; the last
four describe weights that exist but should not be trusted blindly.

    1  weight charted BEFORE the first dose            -- as-of lookup succeeds
    2  none before, but at least one after (same hosp) -- only fallback_on_earliest recovers it
    3  none this hospitalization, one EARLIER          -- needs patient scope (unsupported today)
    4  none this or earlier, one LATER                 -- needs patient scope AND future-looking
    5  no weight in ANY hospitalization                -- irrecoverable
    6  weight before, but STALE                        -- succeeds silently on an old value
    7  weight before, but OUTLIER (<30 or >1100)       -- clifpy nulls it, looks like 5
    8  weight before, but ZERO or negative             -- divide-by-zero, no guard exists
    9  weight before, but HIGH SPREAD during the stay  -- which weight is the right one?

Scenarios are assigned in the order 8 -> 7 -> 6 -> 9 -> 1 -> 2 -> 3 -> 4 -> 5,
so a hospitalization with a broken weight is reported as broken rather than as
"available". They partition the cohort exactly, and that is asserted.

`vitals` carries no `patient_id`, so scenarios 3-5 require joining through
`hospitalization`. Where that table is unavailable the step degrades: 1, 2 and
6-9 are still reported, and 3-5 collapse into a single "no weight this
hospitalization, patient scope unavailable" bucket.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd

from _shared import (
    DRUG_FAMILY,
    Settings,
    logger,
    suppress_small_cells,
    write_output,
)

# clifpy's outlier_config bounds for vitals.weight_kg. Anything outside is
# nulled by apply_outlier_handling, so it behaves as missing downstream.
OUTLIER_MIN, OUTLIER_MAX = 30.0, 1100.0

# Ratio of max to min charted weight within one stay, above which the choice of
# weight materially changes the converted dose.
SPREAD_RATIO = 1.20

SCENARIO_LABELS = {
    1: 'weight before first dose',
    2: 'weight only after first dose (same hospitalization)',
    3: 'weight only from an earlier hospitalization',
    4: 'weight only from a later hospitalization',
    5: 'no weight in any hospitalization',
    6: 'weight before, but stale',
    7: 'weight before, but outlier',
    8: 'weight before, but zero or negative',
    9: 'weight before, but high spread during stay',
    0: 'no weight this hospitalization (patient scope unavailable)',
}


def _load(settings: Settings, table: str, columns: list[str]) -> pd.DataFrame | None:
    """Load one table, returning None rather than raising when absent."""
    from clifpy import load_data
    try:
        df = load_data(
            table,
            config_path=str(settings.config_path),
            columns=columns,
            return_format='pandas',
        )
        logger.info('loaded %s: %d rows', table, len(df))
        return df
    except Exception as exc:
        logger.warning('could not load %s (%s) -- dependent scenarios disabled',
                       table, exc)
        return None


def run(settings: Settings, results: dict[str, Any]) -> dict[str, Any]:
    drugs = list(settings.drugs)

    mac = _load(settings, 'medication_admin_continuous',
                ['hospitalization_id', 'admin_dttm', 'med_category'])
    if mac is None:
        raise RuntimeError('medication_admin_continuous is required')
    mac = mac[mac.med_category.isin(drugs)]
    if mac.empty:
        logger.warning('no study drugs present; nothing to classify')
        return {'scenarios': pd.DataFrame(), 'patient_scope': False}

    vitals = _load(settings, 'vitals',
                   ['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'])
    if vitals is None:
        raise RuntimeError('vitals is required')
    vitals = vitals[(vitals.vital_category == 'weight_kg') & vitals.vital_value.notna()]
    logger.info('weight_kg observations: %d', len(vitals))

    hosp = _load(settings, 'hospitalization',
                 ['hospitalization_id', 'patient_id', 'admission_dttm'])
    patient_scope = hosp is not None
    if not patient_scope:
        logger.warning(
            'hospitalization unavailable -- scenarios 3-5 collapse into a '
            'single "patient scope unavailable" bucket'
        )
        # A one-row-per-hospitalization stand-in keeps the SQL uniform.
        hosp = pd.DataFrame({
            'hospitalization_id': mac.hospitalization_id.unique(),
        })
        hosp['patient_id'] = hosp['hospitalization_id']
        hosp['admission_dttm'] = pd.NaT

    con = duckdb.connect()
    con.register('mac', mac)
    con.register('vitals', vitals)
    con.register('hosp', hosp)

    # First dose of each drug per hospitalization -- the reference instant.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE first_dose AS
        SELECT med_category, hospitalization_id, MIN(admin_dttm) AS first_admin_dttm
        FROM mac GROUP BY med_category, hospitalization_id
    """)

    # Weight facts for the hospitalization the dose was given in.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE own AS
        SELECT f.med_category
             , f.hospitalization_id
             , f.first_admin_dttm
             , COUNT(v.vital_value)                                    AS n_weights
             , COUNT(*) FILTER (v.recorded_dttm <= f.first_admin_dttm) AS n_before
             , COUNT(*) FILTER (v.recorded_dttm >  f.first_admin_dttm) AS n_after
             , MAX(v.recorded_dttm) FILTER (v.recorded_dttm <= f.first_admin_dttm)
                                                                       AS last_before_dttm
             , ARG_MAX(v.vital_value, v.recorded_dttm)
                   FILTER (v.recorded_dttm <= f.first_admin_dttm)      AS asof_weight
             , MIN(v.vital_value) AS min_weight
             , MAX(v.vital_value) AS max_weight
        FROM first_dose f
        LEFT JOIN vitals v ON v.hospitalization_id = f.hospitalization_id
        GROUP BY f.med_category, f.hospitalization_id, f.first_admin_dttm
    """)

    # Weights recorded during this patient's OTHER hospitalizations, split by
    # whether that stay began before or after the one holding the dose.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE other AS
        SELECT o.med_category
             , o.hospitalization_id
             , COUNT(*) FILTER (h2.admission_dttm <  h1.admission_dttm) AS n_earlier
             , COUNT(*) FILTER (h2.admission_dttm >= h1.admission_dttm) AS n_later
        FROM own o
        JOIN hosp h1 ON h1.hospitalization_id = o.hospitalization_id
        LEFT JOIN hosp h2
               ON h2.patient_id = h1.patient_id
              AND h2.hospitalization_id <> h1.hospitalization_id
        LEFT JOIN vitals v2 ON v2.hospitalization_id = h2.hospitalization_id
        WHERE v2.vital_value IS NOT NULL
        GROUP BY o.med_category, o.hospitalization_id
    """)

    stale_days = settings.weight_staleness_days
    scope_sql = 'TRUE' if patient_scope else 'FALSE'

    classified = con.execute(f"""
        WITH j AS (
            SELECT o.*
                 , COALESCE(x.n_earlier, 0) AS n_earlier
                 , COALESCE(x.n_later, 0)   AS n_later
                 , CASE WHEN o.min_weight > 0
                        THEN o.max_weight / o.min_weight END AS spread_ratio
                 , date_diff('day', o.last_before_dttm, o.first_admin_dttm)
                                                            AS weight_age_days
            FROM own o
            LEFT JOIN other x
                   ON x.med_category = o.med_category
                  AND x.hospitalization_id = o.hospitalization_id
        )
        SELECT med_category
             , asof_weight
             , weight_age_days
             , spread_ratio
             , CASE
                 -- broken-weight cases first: a hospitalization with an
                 -- unusable weight must not be counted as "available"
                 WHEN n_before > 0 AND asof_weight <= 0                      THEN 8
                 WHEN n_before > 0 AND (asof_weight < {OUTLIER_MIN}
                                        OR asof_weight > {OUTLIER_MAX})      THEN 7
                 WHEN n_before > 0 AND weight_age_days > {stale_days}        THEN 6
                 WHEN n_before > 0 AND spread_ratio > {SPREAD_RATIO}         THEN 9
                 WHEN n_before > 0                                           THEN 1
                 WHEN n_after  > 0                                           THEN 2
                 WHEN NOT {scope_sql}                                        THEN 0
                 WHEN n_earlier > 0                                          THEN 3
                 WHEN n_later   > 0                                          THEN 4
                 ELSE 5
               END AS scenario
        FROM j
    """).df()

    con.close()

    # ---- exhaustiveness: every hospitalization lands in exactly one bucket --
    n_cohort = len(classified)
    n_assigned = int(classified.scenario.notna().sum())
    if n_assigned != n_cohort:
        raise AssertionError(
            f'scenario classifier is not exhaustive: {n_cohort - n_assigned} '
            f'of {n_cohort} cohort rows unassigned'
        )
    logger.info('classified %s (drug, hospitalization) pairs into %d scenarios',
                f'{n_cohort:,}', classified.scenario.nunique())

    # ---- aggregate: one row per (drug, scenario), no identifiers -----------
    agg = (
        classified
        .groupby(['med_category', 'scenario'])
        .agg(n_hospitalizations=('scenario', 'size'),
             weight_median=('asof_weight', 'median'),
             weight_q1=('asof_weight', lambda s: s.quantile(.25)),
             weight_q3=('asof_weight', lambda s: s.quantile(.75)),
             weight_min=('asof_weight', 'min'),
             weight_max=('asof_weight', 'max'),
             age_days_median=('weight_age_days', 'median'),
             spread_ratio_median=('spread_ratio', 'median'))
        .reset_index()
    )
    totals = classified.groupby('med_category').size().rename('n_cohort')
    agg = agg.merge(totals, on='med_category')
    agg['pct_of_cohort'] = 100 * agg.n_hospitalizations / agg.n_cohort
    agg['scenario_label'] = agg.scenario.map(SCENARIO_LABELS)
    agg['family'] = agg.med_category.map(DRUG_FAMILY).fillna('other')
    agg['site_name'] = settings.site_name
    agg['patient_scope_available'] = patient_scope

    agg = agg[[
        'site_name', 'med_category', 'family', 'scenario', 'scenario_label',
        'n_hospitalizations', 'n_cohort', 'pct_of_cohort',
        'weight_median', 'weight_q1', 'weight_q3', 'weight_min', 'weight_max',
        'age_days_median', 'spread_ratio_median', 'patient_scope_available',
    ]].sort_values(['med_category', 'scenario'])

    write_output(
        suppress_small_cells(agg, 'n_hospitalizations', settings.min_cell_size,
                             keep_cols=['scenario', 'n_cohort']),
        settings, 'weight_scenarios.csv',
    )

    # Convertible = scenario 1 only. 6 and 9 convert but on a questionable
    # weight; 2 needs a non-default flag; 7 and 8 produce garbage.
    conv = (
        agg.assign(convertible=lambda d: d.scenario == 1)
        .groupby('med_category')
        .apply(lambda g: pd.Series({
            'n_cohort': int(g.n_cohort.iloc[0]),
            'pct_scenario_1': g.loc[g.scenario == 1, 'pct_of_cohort'].sum(),
            'pct_recoverable_flag': g.loc[g.scenario == 2, 'pct_of_cohort'].sum(),
            'pct_needs_patient_scope': g.loc[g.scenario.isin([3, 4]), 'pct_of_cohort'].sum(),
            'pct_irrecoverable': g.loc[g.scenario == 5, 'pct_of_cohort'].sum(),
            'pct_questionable': g.loc[g.scenario.isin([6, 7, 8, 9]), 'pct_of_cohort'].sum(),
        }), include_groups=False)
        .reset_index()
    )
    conv['site_name'] = settings.site_name
    write_output(
        suppress_small_cells(conv, 'n_cohort', settings.min_cell_size),
        settings, 'weight_convertibility_by_drug.csv',
    )

    return {
        'scenarios': agg,
        'convertibility': conv,
        'patient_scope': patient_scope,
        'n_cohort_pairs': n_cohort,
    }
