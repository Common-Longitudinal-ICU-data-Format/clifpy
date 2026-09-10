"""Coordinating-centre pooling. Sites never run this.

Reads the folders sites returned and produces the cross-site comparison plus a
decision memo. Consumes them with no site-specific knowledge -- every file
carries its own `site_name` column, so adding a site means dropping its folder
in.

Usage:
    make weight-impact-pool INPUTS="output/weighted_unit_impact/*"

    uv run python dev/weighted-unit-impact/pool.py \
        --inputs output/weighted_unit_impact/RUSH output/weighted_unit_impact/NU
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / 'output' / 'weighted_unit_impact' / '_pooled'

# Files to concatenate across sites, and the key each is reported by.
POOLED = {
    'unit_inventory_by_drug.csv': ['site_name', 'med_category'],
    'weight_scenarios.csv': ['site_name', 'med_category', 'scenario'],
    'weight_convertibility_by_drug.csv': ['site_name', 'med_category'],
    'impact_m1_loss.csv': ['site_name', 'med_category'],
    'impact_m3_bias_verdict.csv': ['site_name', 'med_category'],
    'impact_m5_recovery_ladder.csv': ['site_name', 'med_category'],
    'impact_m6_silent_failure.csv': ['site_name', 'med_category'],
    'decision_table.csv': ['site_name', 'med_category'],
}


def _read(dirs: list[Path], name: str) -> pd.DataFrame:
    frames = []
    for d in dirs:
        f = d / name
        if not f.is_file():
            print(f'  {d.name}: missing {name} -- skipped')
            continue
        frames.append(pd.read_csv(f))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='weight-impact-pool')
    p.add_argument('--inputs', nargs='+', required=True,
                   help='site output directories (globs allowed)')
    args = p.parse_args(argv)

    dirs: list[Path] = []
    for pattern in args.inputs:
        for hit in sorted(glob.glob(pattern)):
            path = Path(hit)
            # Skip our own output directory and anything without a manifest.
            if path.is_dir() and (path / 'run_manifest.json').is_file():
                dirs.append(path)
    if not dirs:
        print('no site folders found (each needs a run_manifest.json)', file=sys.stderr)
        return 2

    print(f'pooling {len(dirs)} site(s): {", ".join(d.name for d in dirs)}')
    OUT.mkdir(parents=True, exist_ok=True)

    # ---- provenance: which clifpy/CLIF version produced each folder --------
    manifests = []
    for d in dirs:
        m = json.loads((d / 'run_manifest.json').read_text())
        manifests.append({
            'site_name': m.get('site_name', d.name),
            'clif_version': m.get('clif_version'),
            'clifpy_version': m.get('clifpy_version'),
            'analysis_commit': m.get('analysis_commit'),
            'steps_ok': ','.join(k for k, v in (m.get('steps') or {}).items() if v == 'ok'),
            'steps_failed': ','.join(k for k, v in (m.get('steps') or {}).items() if v == 'failed'),
        })
    prov = pd.DataFrame(manifests)
    prov.to_csv(OUT / 'provenance.csv', index=False)

    pooled: dict[str, pd.DataFrame] = {}
    for name, _key in POOLED.items():
        df = _read(dirs, name)
        if df.empty:
            continue
        df.to_csv(OUT / f'pooled_{name}', index=False)
        pooled[name] = df

    # ---- the memo ----------------------------------------------------------
    lines: list[str] = []
    add = lines.append
    add('# Weighted dose units — cross-site decision memo')
    add('')
    add(f'Sites pooled: {len(dirs)} ({", ".join(sorted(d.name for d in dirs))})')
    add('')

    versions = prov.clifpy_version.dropna().unique()
    if len(versions) > 1:
        add(f'⚠ Sites ran different clifpy versions ({", ".join(map(str, versions))}). '
            f'Check before comparing.')
        add('')

    dec = pooled.get('decision_table.csv')
    if dec is not None and not dec.empty:
        b = dec[dec.recommendation.str.startswith('B')]
        add('## Recommendation by drug')
        add('')
        add('A drug is listed once per site that flagged it. Agreement across '
            'sites is the signal worth acting on; a single-site flag may be '
            'local.')
        add('')
        if b.empty:
            add('No drug at any site meets the bar for option B.')
        else:
            counts = (b.groupby('med_category')
                      .agg(sites_flagging=('site_name', 'nunique'),
                           reasons=('recommendation', lambda s: '; '.join(sorted(set(s)))))
                      .reset_index()
                      .sort_values('sites_flagging', ascending=False))
            n_sites = dec.site_name.nunique()
            add(f'| drug | sites flagging (of {n_sites}) | reasons |')
            add('|---|---:|---|')
            for _, r in counts.iterrows():
                add(f'| {r.med_category} | {int(r.sites_flagging)} | {r.reasons} |')
        add('')

    m6 = pooled.get('impact_m6_silent_failure.csv')
    if m6 is not None and not m6.empty:
        add('## Silent failure')
        add('')
        add('Share of unconvertible rows that pass every downstream range '
            'check because they keep their raw unit label.')
        add('')
        piv = m6.pivot_table(index='med_category', columns='site_name',
                             values='pct_silent', aggfunc='max')
        add('| drug | ' + ' | '.join(piv.columns) + ' |')
        add('|---' * (len(piv.columns) + 1) + '|')
        for drug, row in piv.iterrows():
            cells = ' | '.join('—' if pd.isna(v) else f'{v:.0f}%' for v in row)
            add(f'| {drug} | {cells} |')
        add('')

    m5 = pooled.get('impact_m5_recovery_ladder.csv')
    if m5 is not None and not m5.empty:
        add('## What `fallback_on_earliest` would buy')
        add('')
        add('This flag already exists in clifpy and is off by default.')
        add('')
        gain = (m5.assign(gain=lambda d: d.b_plus_fallback_on_earliest - d.a_asof_today)
                .groupby('med_category')
                .agg(mean_gain_pct=('gain', 'mean'),
                     mean_today_pct=('a_asof_today', 'mean'))
                .reset_index()
                .sort_values('mean_gain_pct', ascending=False)
                .head(10))
        add('| drug | convertible today | with the flag | gain |')
        add('|---|---:|---:|---:|')
        for _, r in gain.iterrows():
            add(f'| {r.med_category} | {r.mean_today_pct:.1f}% | '
                f'{r.mean_today_pct + r.mean_gain_pct:.1f}% | '
                f'+{r.mean_gain_pct:.1f} pts |')
        add('')

    add('## Provenance')
    add('')
    add('| site | CLIF | clifpy | commit | steps ok | failed |')
    add('|---|---|---|---|---|---|')
    for _, r in prov.iterrows():
        add(f'| {r.site_name} | {r.clif_version} | {r.clifpy_version} | '
            f'`{r.analysis_commit}` | {r.steps_ok} | {r.steps_failed or "—"} |')
    add('')

    dest = OUT / 'decision_memo.md'
    dest.write_text('\n'.join(lines) + '\n')
    print(f'wrote {dest.relative_to(REPO_ROOT)}')
    print(f'wrote {len(pooled)} pooled CSV(s) to {OUT.relative_to(REPO_ROOT)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
