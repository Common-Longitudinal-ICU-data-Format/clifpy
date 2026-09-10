"""Step 3 -- what would forcing the mCIDE target actually cost?

Six metrics, computed per (site, drug), deliberately independent of any
downstream consumer. A metric defined through SOFA-2 or a norepinephrine
equivalent measures that consumer's implementation as much as it measures the
conversion, so none is used here.

    M1  loss, split by whether option B would recover it, and by granularity
    M2  loss decomposition -- which weight problem caused it
    M3  selection bias -- are the lost rows different from the kept ones?
    M4  conversion-introduced uncertainty -- the spread_ratio distribution
    M5  recovery ladder -- what each weight-lookup option would buy
    M6  does the failure fail loudly, or produce a plausible wrong number?

M3 and M6 are the ones that decide the recommendation. M1 volume alone never
does: a large but unbiased, loudly-failing loss is survivable, while a small
biased or silent one is not.
"""

from __future__ import annotations

from typing import Any

import duckdb
import numpy as np
import pandas as pd

from _shared import DRUG_FAMILY, Settings, logger, suppress_small_cells, write_output

# clifpy outlier_config ranges, keyed by (med_category, med_dose_unit). An
# unconvertible row keeps its RAW unit label, so it is range-checked against the
# wrong band -- which is what M6 measures.
OUTLIER_BANDS = {
    ('norepinephrine', 'mcg/kg/min'): (0.0, 3.0),
    ('norepinephrine', 'mcg/min'): (0.0, 200.0),
    ('epinephrine', 'mcg/kg/min'): (0.0, 3.0),
    ('epinephrine', 'mcg/min'): (0.0, 200.0),
    ('phenylephrine', 'mcg/kg/min'): (0.0, 10.0),
    ('phenylephrine', 'mcg/min'): (0.0, 1000.0),
}


def _weight_independent_note() -> str:
    return (
        'cumulative exposure, infusion duration, dose trajectory, ever-exposed'
    )


def run(settings: Settings, results: dict[str, Any]) -> dict[str, Any]:
    from clifpy import load_data
    from clifpy.utils.unit_converter import convert_dose_units_by_med_category

    drugs = settings.drugs

    mac = load_data(
        'medication_admin_continuous',
        config_path=str(settings.config_path),
        columns=['hospitalization_id', 'admin_dttm', 'med_category',
                 'med_dose', 'med_dose_unit'],
        return_format='pandas',
    )
    mac = mac[mac.med_category.isin(drugs)].copy()
    if mac.empty:
        logger.warning('no study drugs present; impact step has nothing to measure')
        return {}
    logger.info('study-drug administrations: %s', f'{len(mac):,}')

    vitals = load_data(
        'vitals',
        config_path=str(settings.config_path),
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'],
        return_format='pandas',
    )
    vitals = vitals[(vitals.vital_category == 'weight_kg') & vitals.vital_value.notna()]

    # ---- the forced conversion, exactly as a site would run it -------------
    logger.info('running forced conversion against the mCIDE targets...')
    converted, _counts = convert_dose_units_by_med_category(
        mac, vitals_df=vitals, preferred_units=drugs,
        override=True, show_intermediate=True,
    )
    converted['converted_ok'] = converted['_convert_status'] == 'success'
    logger.info('conversion complete: %.1f%% of rows succeeded',
                100 * converted.converted_ok.mean())

    con = duckdb.connect()
    con.register('c', converted)

    # ---- M1: loss by granularity ------------------------------------------
    m1 = con.execute("""
        SELECT med_category
             , COUNT(*)                                        AS n_rows
             , COUNT(*) FILTER (NOT converted_ok)              AS n_rows_lost
             , COUNT(DISTINCT hospitalization_id)              AS n_encounters
             , COUNT(DISTINCT hospitalization_id)
                   FILTER (NOT converted_ok)                   AS n_encounters_touched
        FROM c GROUP BY med_category
    """).df()

    # An encounter is *fully* lost only when none of its rows convert -- the
    # distinction that separates "a gap in this patient's record" from "this
    # patient disappears from a drug-defined cohort".
    fully = con.execute("""
        WITH per_enc AS (
            SELECT med_category, hospitalization_id
                 , BOOL_OR(converted_ok) AS any_ok
            FROM c GROUP BY med_category, hospitalization_id
        )
        SELECT med_category
             , COUNT(*) FILTER (NOT any_ok) AS n_encounters_fully_lost
        FROM per_enc GROUP BY med_category
    """).df()
    m1 = m1.merge(fully, on='med_category', how='left')
    m1['pct_rows_lost'] = 100 * m1.n_rows_lost / m1.n_rows
    m1['pct_encounters_fully_lost'] = 100 * m1.n_encounters_fully_lost / m1.n_encounters
    m1['pct_encounters_touched'] = 100 * m1.n_encounters_touched / m1.n_encounters

    # Recoverability: a lost row is still usable in its native unit for any
    # weight-independent analysis. That -- and only that -- is what option B
    # buys back.
    m1['recoverable_by_option_b'] = m1.n_rows_lost
    m1['recoverable_for'] = _weight_independent_note()
    m1['site_name'] = settings.site_name
    m1['family'] = m1.med_category.map(DRUG_FAMILY).fillna('other')

    # ---- M2: why did it fail? ---------------------------------------------
    m2 = con.execute("""
        SELECT med_category, _convert_status AS status
             , COUNT(*) AS n_rows
             , COUNT(DISTINCT hospitalization_id) AS n_encounters
        FROM c WHERE NOT converted_ok
        GROUP BY med_category, _convert_status
    """).df()
    if not m2.empty:
        tot = m2.groupby('med_category').n_rows.transform('sum')
        m2['pct_of_lost'] = 100 * m2.n_rows / tot
        m2['site_name'] = settings.site_name

    # ---- M3: selection bias -- the decisive metric -------------------------
    # Compare kept vs lost rows on their RAW, uncontroversial values. If they
    # are indistinguishable, option A loses volume but not validity.
    m3 = con.execute("""
        SELECT med_category
             , converted_ok
             , COUNT(*)                                  AS n_rows
             , COUNT(DISTINCT hospitalization_id)        AS n_encounters
             , median(med_dose)                          AS dose_median
             , quantile_cont(med_dose, 0.25)             AS dose_q1
             , quantile_cont(med_dose, 0.75)             AS dose_q3
        FROM c GROUP BY med_category, converted_ok
    """).df()

    # Rows per encounter is a proxy for infusion duration that needs no
    # additional table and no assumption about charting cadence.
    dur = con.execute("""
        WITH per_enc AS (
            SELECT med_category, hospitalization_id, converted_ok
                 , COUNT(*) AS n_admins
            FROM c GROUP BY med_category, hospitalization_id, converted_ok
        )
        SELECT med_category, converted_ok
             , median(n_admins) AS admins_per_encounter_median
        FROM per_enc GROUP BY med_category, converted_ok
    """).df()
    m3 = m3.merge(dur, on=['med_category', 'converted_ok'], how='left')
    m3['site_name'] = settings.site_name

    # Bias verdict: dose distributions differing by more than 25% at the median
    # between kept and lost rows.
    # A median ratio computed on a handful of lost rows is noise, not bias.
    # Without this gate a drug losing 4 rows out of 199,574 reads as "BIASED"
    # and drives a recommendation, which is worse than saying nothing.
    MIN_LOST_FOR_BIAS = max(30, settings.min_cell_size)

    bias_rows = []
    for drug, g in m3.groupby('med_category'):
        kept = g[g.converted_ok]
        lost = g[~g.converted_ok]
        n_kept = int(kept.n_rows.iloc[0]) if not kept.empty else 0
        n_lost = int(lost.n_rows.iloc[0]) if not lost.empty else 0

        if kept.empty or lost.empty:
            verdict, ratio = 'n/a (nothing lost)', np.nan
        elif n_lost < MIN_LOST_FOR_BIAS:
            k, l = kept.dose_median.iloc[0], lost.dose_median.iloc[0]
            ratio = (l / k) if k else np.nan
            verdict = f'insufficient (n_lost<{MIN_LOST_FOR_BIAS})'
        else:
            k, l = kept.dose_median.iloc[0], lost.dose_median.iloc[0]
            ratio = (l / k) if k else np.nan
            verdict = (
                'BIASED' if pd.notna(ratio) and (ratio > 1.25 or ratio < 0.8)
                else 'comparable'
            )
        bias_rows.append({
            'site_name': settings.site_name, 'med_category': drug,
            'lost_vs_kept_dose_ratio': ratio, 'bias_verdict': verdict,
            'n_kept': n_kept, 'n_lost': n_lost,
        })
    bias = pd.DataFrame(bias_rows)

    # ---- M4: conversion-introduced uncertainty ----------------------------
    # For rows that DID convert, how much would the dose change if a different
    # weight from the same stay had been chosen? Supersedes scenario 9's binary.
    con.register('v', vitals)
    m4 = con.execute("""
        WITH w AS (
            SELECT hospitalization_id
                 , MIN(vital_value) AS wmin
                 , MAX(vital_value) AS wmax
            FROM v GROUP BY hospitalization_id
        )
        SELECT c.med_category
             , COUNT(*)                                    AS n_rows
             , median(w.wmax / NULLIF(w.wmin, 0))          AS spread_median
             , quantile_cont(w.wmax / NULLIF(w.wmin, 0), 0.75) AS spread_q3
             , quantile_cont(w.wmax / NULLIF(w.wmin, 0), 0.95) AS spread_p95
             , AVG(CASE WHEN w.wmax / NULLIF(w.wmin, 0) > 1.20 THEN 1.0 ELSE 0 END) * 100
                                                           AS pct_spread_over_20
        FROM c JOIN w ON w.hospitalization_id = c.hospitalization_id
        WHERE c.converted_ok
        GROUP BY c.med_category
    """).df()
    m4['site_name'] = settings.site_name

    # ---- M5: recovery ladder ----------------------------------------------
    scen = results.get('weights', {}).get('scenarios')
    if scen is not None and not scen.empty:
        piv = (scen.pivot_table(index='med_category', columns='scenario',
                                values='pct_of_cohort', aggfunc='sum')
               .fillna(0))
        for s in range(0, 10):
            if s not in piv.columns:
                piv[s] = 0.0
        m5 = pd.DataFrame({
            'med_category': piv.index,
            'a_asof_today': piv[1] + piv[6] + piv[9],
            'b_plus_fallback_on_earliest': piv[1] + piv[6] + piv[9] + piv[2],
            'c_plus_patient_scope_earlier': piv[1] + piv[6] + piv[9] + piv[2] + piv[3],
            'd_plus_patient_scope_later': piv[1] + piv[6] + piv[9] + piv[2] + piv[3] + piv[4],
        }).reset_index(drop=True)
        m5['ceiling_irrecoverable'] = 100 - m5.d_plus_patient_scope_later
        m5['site_name'] = settings.site_name
    else:
        m5 = pd.DataFrame()
        logger.warning('weight scenarios unavailable -- M5 recovery ladder skipped')

    # ---- M6: does the failure fail loudly? --------------------------------
    # The converter does NOT mislabel: an unconvertible row keeps its raw value
    # and its raw (correct) unit label. What fails silently is the *column* --
    # it ends up mixed, mostly the target unit and partly the original, with no
    # error raised. Range checks key on (med_category, med_dose_unit), so each
    # row is correctly checked against its own unit and correctly passes; they
    # cannot see that the column is heterogeneous.
    #
    # So the risk is an unenforced promise, not corrupt data -- and the defence
    # is per-row discipline in every consumer, forever.
    mix = con.execute("""
        SELECT med_category
             , COUNT(DISTINCT med_dose_unit_converted) AS n_units_in_output
             , COUNT(*)                                AS n_rows
             , COUNT(*) FILTER (NOT converted_ok)       AS n_rows_off_target
        FROM c GROUP BY med_category
    """).df()
    mix['pct_off_target'] = 100 * mix.n_rows_off_target / mix.n_rows
    mix['output_is_mixed'] = mix.n_units_in_output > 1
    mix['site_name'] = settings.site_name
    n_mixed = int(mix.output_is_mixed.sum())
    if n_mixed:
        logger.info(
            '%d drug(s) produce a MIXED output unit column despite '
            'standardization', n_mixed,
        )

    lost = converted[~converted.converted_ok].copy()
    m6_rows = []
    for drug, g in lost.groupby('med_category'):
        target = drugs[drug]
        for unit, gg in g.groupby('med_dose_unit_converted'):
            raw_band = OUTLIER_BANDS.get((drug, str(unit)))
            tgt_band = OUTLIER_BANDS.get((drug, target))
            if raw_band is None or tgt_band is None:
                continue
            vals = gg['med_dose_converted'].dropna()
            if vals.empty:
                continue
            caught_raw = ((vals < raw_band[0]) | (vals > raw_band[1])).mean()
            caught_tgt = ((vals < tgt_band[0]) | (vals > tgt_band[1])).mean()
            m6_rows.append({
                'site_name': settings.site_name, 'med_category': drug,
                'surviving_unit': unit, 'target_unit': target,
                'n_rows': int(len(vals)),
                'pct_caught_by_raw_band': 100 * caught_raw,
                'pct_would_be_caught_by_target_band': 100 * caught_tgt,
                'pct_silent': 100 * (1 - caught_raw),
            })
    m6 = pd.DataFrame(m6_rows)

    # ---- decision table ----------------------------------------------------
    dec = m1[['site_name', 'med_category', 'family', 'n_rows', 'pct_rows_lost',
              'pct_encounters_fully_lost']].merge(
        bias[['med_category', 'bias_verdict', 'lost_vs_kept_dose_ratio']],
        on='med_category', how='left')
    if not m6.empty:
        silent = m6.groupby('med_category').pct_silent.max().rename('pct_silent')
        dec = dec.merge(silent, on='med_category', how='left')
    else:
        dec['pct_silent'] = np.nan

    # A drug losing a negligible share of rows needs no policy at all -- the
    # recommendation only has meaning above a floor.
    MATERIAL_ROW_LOSS_PCT = 0.5

    def _recommend(r) -> str:
        # Priority order, from the plan: bias, then silent failure, then
        # recoverable volume. Volume alone never justifies B -- but neither
        # does bias or silence on a loss too small to matter.
        if r['pct_rows_lost'] < MATERIAL_ROW_LOSS_PCT:
            return 'A (loss negligible)'
        if r.get('bias_verdict') == 'BIASED':
            return 'B (biased loss)'
        if pd.notna(r.get('pct_silent')) and r['pct_silent'] > 50:
            return 'B (fails silently)'
        if r['pct_encounters_fully_lost'] > 5:
            return 'B (material encounter loss)'
        return 'A'

    dec['recommendation'] = dec.apply(_recommend, axis=1)

    for name, df, cnt in [
        ('impact_m1_loss.csv', m1, 'n_encounters'),
        ('impact_m2_decomposition.csv', m2, 'n_encounters'),
        ('impact_m3_selection_bias.csv', m3, 'n_encounters'),
        ('impact_m3_bias_verdict.csv', bias, 'n_lost'),
        ('impact_m4_weight_uncertainty.csv', m4, 'n_rows'),
        ('impact_m5_recovery_ladder.csv', m5, None),
        ('impact_m6_silent_failure.csv', m6, 'n_rows'),
        ('impact_m6_mixed_output_unit.csv', mix, 'n_rows'),
        ('decision_table.csv', dec, 'n_rows'),
    ]:
        if df is None or df.empty:
            continue
        out = (suppress_small_cells(df, cnt, settings.min_cell_size)
               if cnt and cnt in df.columns else df)
        write_output(out, settings, name)

    con.close()
    logger.info('impact metrics complete')
    return {'m1': m1, 'm2': m2, 'm3': m3, 'bias': bias,
            'm4': m4, 'm5': m5, 'm6': m6, 'decision': dec}
