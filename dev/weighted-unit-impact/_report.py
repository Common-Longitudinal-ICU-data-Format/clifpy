"""Step 4 -- assemble a readable site summary.

Writes `site_summary.md` next to the CSVs. The CSVs are what gets pooled; this
file is so the person who ran it can see what they found without opening a
spreadsheet, and can sanity-check it before sending anything.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from _shared import REPO_ROOT, Settings, logger


def _fmt(v, dp: int = 1, suffix: str = '') -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '—'
    return f'{v:,.{dp}f}{suffix}'


def run(settings: Settings, results: dict[str, Any], manifest: dict[str, Any]) -> None:
    lines: list[str] = []
    add = lines.append

    add(f'# Weighted dose unit impact — {settings.site_name}')
    add('')
    add(f'- CLIF version: `{settings.clif_version}`')
    add(f'- Drugs analysed: {len(settings.drugs)}')
    add(f'- Small-cell threshold: {settings.min_cell_size}')
    add(f'- Staleness threshold: {settings.weight_staleness_days} days')
    add('')
    add('Every figure below is an aggregate. No identifier appears in any file '
        'in this folder — enforced at write time, not by convention.')
    add('')

    steps_ok = [k for k, v in manifest.get('steps', {}).items() if v == 'ok']
    steps_bad = [k for k, v in manifest.get('steps', {}).items() if v == 'failed']
    if steps_bad:
        add('## ⚠ Incomplete run')
        add('')
        add(f'Completed: {", ".join(steps_ok) or "none"}. '
            f'Failed: {", ".join(steps_bad)}.')
        add('')
        for k in steps_bad:
            add(f'- `{k}`: {manifest.get("skipped", {}).get(k, "unknown reason")}')
        add('')
        add('Partial results are still worth returning — the manifest records '
            'what is missing.')
        add('')

    # ---- inventory ---------------------------------------------------------
    inv = results.get('inventory', {})
    if inv:
        add('## What this site charts')
        add('')
        add(f'- `medication_admin_continuous` rows: {inv.get("n_mac_rows", 0):,}')
        add(f'- Observations that would need forcing to a weighted unit: '
            f'**{inv.get("total_forced_obs", 0):,}**')
        absent = inv.get('drugs_absent') or []
        if absent:
            add(f'- Study drugs not charted here ({len(absent)}): '
                f'{", ".join(absent)}')
        add('')

    # ---- weight availability ----------------------------------------------
    conv = results.get('weights', {}).get('convertibility')
    if conv is not None and not conv.empty:
        add('## Weight availability')
        add('')
        add('Percent of hospitalizations receiving each drug, by whether a '
            'usable weight exists. `questionable` means a weight was found but '
            'is stale, an outlier, non-positive, or ambiguous within the stay.')
        add('')
        add('| drug | cohort | clean | only after | other hosp | none ever | questionable |')
        add('|---|---:|---:|---:|---:|---:|---:|')
        for _, r in conv.sort_values('n_cohort', ascending=False).head(12).iterrows():
            add(f'| {r.med_category} | {_fmt(r.n_cohort, 0)} | '
                f'{_fmt(r.pct_scenario_1, 1, "%")} | '
                f'{_fmt(r.pct_recoverable_flag, 1, "%")} | '
                f'{_fmt(r.pct_needs_patient_scope, 1, "%")} | '
                f'{_fmt(r.pct_irrecoverable, 1, "%")} | '
                f'{_fmt(r.pct_questionable, 1, "%")} |')
        add('')

    # ---- the recommendation ------------------------------------------------
    dec = results.get('impact', {}).get('decision')
    if dec is not None and not dec.empty:
        b = dec[dec.recommendation.str.startswith('B')]
        add('## Recommendation')
        add('')
        if b.empty:
            add('**Option A (single weighted unit) is adequate for every drug '
                'at this site.** No drug shows biased loss, silent failure, or '
                'material encounter loss.')
        else:
            add(f'**{len(b)} drug(s) argue for option B** (keeping an '
                f'unweighted unit alongside the weighted one):')
            add('')
            add('| drug | rows lost | encounters fully lost | bias | silent | why |')
            add('|---|---:|---:|---|---:|---|')
            for _, r in b.sort_values('n_rows', ascending=False).iterrows():
                add(f'| {r.med_category} | {_fmt(r.pct_rows_lost, 2, "%")} | '
                    f'{_fmt(r.pct_encounters_fully_lost, 2, "%")} | '
                    f'{r.bias_verdict} | {_fmt(r.get("pct_silent"), 0, "%")} | '
                    f'{r.recommendation.split("(", 1)[-1].rstrip(")")} |')
            add('')
            add('The rule, in priority order: recommend B when the loss is '
                'biased, or fails silently, or removes more than 5% of '
                'encounters entirely — but only once the loss exceeds 0.5% of '
                'rows. Volume alone never justifies B; bias and silence do.')
        add('')

    # ---- silent failure ----------------------------------------------------
    m6 = results.get('impact', {}).get('m6')
    if m6 is not None and not m6.empty:
        add('### Why "silent" matters')
        add('')
        add('The converter does not mislabel anything — an unconvertible row '
            'correctly keeps its raw value **and** its raw unit label. The '
            'problem is that the standardized column ends up **silently '
            'mixed**: mostly the target unit, partly the original one, with no '
            'error.')
        add('')
        add('Range checks key on `(med_category, med_dose_unit)`, so each row '
            'is correctly checked against its own unit and correctly passes. '
            'They cannot detect that the column is heterogeneous. Had these '
            'rows carried the target label instead, the share below would have '
            'been rejected as outliers.')
        add('')
        add('| drug | surviving unit | rows | caught now | would be caught as target | silent |')
        add('|---|---|---:|---:|---:|---:|')
        for _, r in m6.sort_values('n_rows', ascending=False).iterrows():
            add(f'| {r.med_category} | `{r.surviving_unit}` | {_fmt(r.n_rows, 0)} | '
                f'{_fmt(r.pct_caught_by_raw_band, 1, "%")} | '
                f'{_fmt(r.pct_would_be_caught_by_target_band, 1, "%")} | '
                f'{_fmt(r.pct_silent, 1, "%")} |')
        add('')

    # ---- recovery ----------------------------------------------------------
    m5 = results.get('impact', {}).get('m5')
    if m5 is not None and not m5.empty:
        add('## What each remedy would buy')
        add('')
        add('Percent of the cohort convertible under progressively wider '
            'weight lookup.')
        add('')
        add('| drug | today | + fallback_on_earliest | + patient scope (earlier) | + later | ceiling |')
        add('|---|---:|---:|---:|---:|---:|')
        top = m5.sort_values('a_asof_today').head(10)
        for _, r in top.iterrows():
            add(f'| {r.med_category} | {_fmt(r.a_asof_today, 1, "%")} | '
                f'{_fmt(r.b_plus_fallback_on_earliest, 1, "%")} | '
                f'{_fmt(r.c_plus_patient_scope_earlier, 1, "%")} | '
                f'{_fmt(r.d_plus_patient_scope_later, 1, "%")} | '
                f'{_fmt(100 - r.ceiling_irrecoverable, 1, "%")} |')
        add('')
        add('`fallback_on_earliest` already exists in clifpy and is **off by '
            'default**. It is the cheapest available improvement.')
        add('')

    add('## Files in this folder')
    add('')
    for f in sorted(settings.output_dir.glob('*')):
        if f.name == 'site_summary.md':
            continue
        add(f'- `{f.name}`')
    add('')
    add('Send the whole folder back. Nothing in it is patient-level.')

    dest = settings.output_dir / 'site_summary.md'
    dest.write_text('\n'.join(lines) + '\n')
    logger.info('wrote %s', dest.relative_to(REPO_ROOT))
