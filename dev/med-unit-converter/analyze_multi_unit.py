"""Which `med_category` rows deserve more than one permissible dose unit?

Reads the consortium DQA export of every `(med_category, med_dose_unit)` pair
observed across sites, works out each site's *primary* convention per drug, and
reports the cases where sites genuinely disagree in a way arithmetic cannot
reconcile.

Emits two CSVs next to this script:

- `target_vs_practice.csv`        -- every spreadsheet row vs what sites actually chart
- `multi_unit_recommendations.csv` -- the irreconcilable pairs, tiered

See `multi_permissible_units.md` for the interpretation. Aggregate input only;
no row-level or ID-linked data is read anywhere in this script.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pandas as pd

from clifpy.utils.unit_converter import (
    ALL_ACCEPTABLE_UNITS,
    CANONICAL_UNIT_SPELLING,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb,
)

HERE = Path(__file__).parent
INVENTORY = HERE / 'medication_admin_continuous_dose_by_category_and_unit.csv'
SPREADSHEET = HERE / 'meds unit standardization.xlsx - 09112026_med_admin_cont.csv'

# A site charting fewer than this many observations of a drug has no
# "convention" worth naming -- it is a handful of stray rows.
MIN_SITE_OBS = 100

# The alternative must be the primary convention at this many sites before it
# is a consortium-wide practice rather than one site's local habit.
MULTI_SITE_THRESHOLD = 3


# ---------------------------------------------------------------- unit axes

def axes(unit: str) -> tuple[str, str, str]:
    """Decompose a cleaned unit into (family, weight-base, time-base)."""
    weight = 'kg' if '/kg' in unit else ('lb' if '/lb' in unit else '')
    m = re.search(r'/(min|hr|day)', unit)
    time = m.group(1) if m else ''
    base = re.match(r'^([a-z\-]+)', unit).group(1)
    family = (
        'mass' if base in {'mcg', 'mg', 'ng', 'g'}
        else 'volume' if base in {'ml', 'l', 'mcl'}
        else 'unit' if base in {'u', 'mu', 'mnu'}
        else base
    )
    return family, weight, time


def irreconcilable(a: str, b: str) -> list[str]:
    """Axes on which `a` and `b` differ beyond what arithmetic can bridge.

    Prefix (mcg/mg), time base (/hr vs /min) and weight base (/kg vs /lb)
    differences are pure multiplication and are deliberately NOT reported --
    the converter already handles those losslessly.
    """
    af, aw, at = axes(a)
    bf, bw, bt = axes(b)
    diffs = []
    if (aw == '') != (bw == ''):
        diffs.append('weight')           # needs the patient's weight
    if (at == '') != (bt == ''):
        diffs.append('rate-vs-amount')   # needs the infusion duration
    if af != bf:
        diffs.append('family')           # needs concentration / potency
    return diffs


# ------------------------------------------------------------------ loading

def spell(unit: str) -> str:
    """Render a cleaned unit in the CLIF 3.0 output vocabulary.

    The converter works internally in short tokens (`u`, `mu`, `mnu`) but the
    standard spells the unit family out -- `units/hr`, `milli-units/min`,
    `million-units` -- while keeping mass and volume abbreviated. This is the
    Python counterpart of `_canonical_spelling_expr` in clifpy's `_grammar`,
    reusing the same map so the two cannot drift apart.

    Tokens are matched longest-first and anchored at the start, so `mnu` is
    never mistaken for `mu` or `u`.

    >>> spell('u/kg/hr'), spell('mu/min'), spell('mcg/kg/min')
    ('units/kg/hr', 'milli-units/min', 'mcg/kg/min')
    """
    if not isinstance(unit, str):
        return unit
    for token in sorted(CANONICAL_UNIT_SPELLING, key=lambda t: (-len(t), t)):
        if unit == token or unit.startswith(token + '/'):
            return CANONICAL_UNIT_SPELLING[token] + unit[len(token):]
    return unit


def _clean_map(units: pd.Series) -> dict[str, str]:
    """Map each distinct raw unit string to its clifpy-cleaned form.

    Returns a dict rather than a positionally-aligned Series on purpose. The
    duckdb relation carries the raw and cleaned values on the same row, so
    reading them out together is safe no matter what order duckdb returns; an
    aligned-by-position `.values` assignment would not be.
    """
    distinct = pd.DataFrame(
        {'med_dose_unit': sorted(units.dropna().astype(str).unique())})
    rel = _clean_dose_unit_names_duckdb(_clean_dose_unit_formats_duckdb(distinct))
    out = rel.to_df()
    return dict(zip(out['med_dose_unit'], out['_clean_unit']))


def load_usage() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per (med_category, cleaned unit, site) observation counts.

    Returns `(usage, unmapped)`. `unmapped` holds observations that carry a
    dose unit but no `med_category` at all -- they cannot inform a per-category
    recommendation, but they are a data-quality signal worth surfacing rather
    than dropping silently (a bare `groupby` would discard them without a word).
    """
    raw = pd.read_csv(INVENTORY)
    site_cols = [c for c in raw.columns
                 if c.startswith('total_obs__') and c != 'total_obs__ALL']

    usage = raw.melt(
        id_vars=['med_category', 'med_dose_unit'],
        value_vars=site_cols, var_name='site', value_name='obs',
    )
    usage['site'] = usage.site.str.removeprefix('total_obs__')
    usage['obs'] = pd.to_numeric(usage.obs, errors='coerce').fillna(0)
    usage = usage[usage.obs > 0].copy()

    # Map on the value, not by position: duckdb keeps the raw and cleaned
    # columns on the same row, so a value join is order-independent, whereas
    # assigning `.values` would rely on duckdb preserving input row order.
    usage['clean'] = usage.med_dose_unit.astype(str).map(_clean_map(usage.med_dose_unit))
    recognized = usage[usage.clean.isin(ALL_ACCEPTABLE_UNITS)].copy()

    unmapped = recognized[recognized.med_category.isna()].copy()
    recognized = recognized[recognized.med_category.notna()]

    # One row per (category, unit, site) -- distinct raw spellings can clean
    # to the same token (e.g. "mcg/kg/min" and "MCG/KG/MIN ").
    usage_out = (recognized.groupby(['med_category', 'clean', 'site'], as_index=False)
                           .obs.sum())
    return usage_out, unmapped


def dominant_units(usage: pd.DataFrame) -> pd.DataFrame:
    """Each site's primary unit per drug.

    Counting any site with >=1 row conflates a site's convention with stray
    rows -- it makes nitroglycerin look like a 10-site weighted drug when 10 of
    11 sites chart it unweighted. Taking the max-observation unit per
    (category, site) asks the right question.
    """
    site_totals = (usage.groupby(['med_category', 'site'], as_index=False)
                        .obs.sum().rename(columns={'obs': 'site_obs'}))
    merged = usage.merge(site_totals, on=['med_category', 'site'])
    merged = merged[merged.site_obs >= MIN_SITE_OBS]
    return (merged.sort_values(['obs', 'clean'])
                  .groupby(['med_category', 'site']).tail(1))


# ------------------------------------------------------------------ reports

def build_target_vs_practice(dom: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
    """Every spreadsheet row against the units sites actually use.

    The DQA export carries no `med_group`, so categories the spreadsheet splits
    by route (`epoprostenol`, `terbutaline`) receive the same observed split on
    both rows. Read those two pairs with that in mind.
    """
    rows = []
    for _, r in sheet.iterrows():
        target = r.target_clean
        sub = dom[dom.med_category == r.med_category]
        if sub.empty:
            rows.append({'med_category': r.med_category, 'med_group': r.med_group,
                         'target': spell(target), 'sites_on_target': 0, 'site_total': 0,
                         'split': '', 'flag': 'not charted'})
            continue
        by_unit = (sub.groupby('clean')
                      .agg(n=('site', 'nunique'), obs=('obs', 'sum'),
                           sites=('site', lambda s: ','.join(sorted(s))))
                      .sort_values('n', ascending=False))
        on_target = int(by_unit.loc[target, 'n']) if target in by_unit.index else 0
        rows.append({
            'med_category': r.med_category,
            'med_group': r.med_group,
            'target': spell(target),
            'sites_on_target': on_target,
            'site_total': int(by_unit.n.sum()),
            'split': ' | '.join(f'{spell(u)}:{v.n}({int(v.obs)})'
                                for u, v in by_unit.iterrows()),
            'flag': ('ZERO' if on_target == 0
                     else 'minority' if on_target < by_unit.n.max()
                     else ''),
        })
    return pd.DataFrame(rows)


def build_recommendations(dom: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
    """Irreconcilable (target, alternative) pairs, tiered for action.

    Tier A -- alternative is primary at >=3 sites; add it as a second unit.
    Tier B -- no site charts the target at all; fix the primary first.
    Tier D -- one or two sites; route back to the site, do not change schema.

    (Tier C -- target is a minority but every difference is lossless -- has no
    row here by construction, and is read off `target_vs_practice.csv`.)
    """
    rows = []
    for _, r in sheet.iterrows():
        target = r.target_clean
        sub = dom[dom.med_category == r.med_category]
        if sub.empty or not isinstance(target, str) or not target:
            continue
        by_unit = (sub.groupby('clean')
                      .agg(n=('site', 'nunique'), obs=('obs', 'sum'),
                           sites=('site', lambda s: ','.join(sorted(s))))
                      .sort_values('n', ascending=False))
        target_used = target in by_unit.index

        for unit, v in by_unit.iterrows():
            if unit == target:
                continue
            reasons = irreconcilable(target, unit)
            if not reasons:
                continue                      # lossless -- converter handles it
            rows.append({
                'med_category': r.med_category,
                'med_group': r.med_group,
                'current_unit': r.med_dose_unit_category,
                'add_unit': spell(unit),
                'reason': '+'.join(reasons),
                'n_sites_primary': int(v.n),
                'obs': int(v.obs),
                'sites': v.sites,
                'tier': ('B' if not target_used
                         else 'A' if v.n >= MULTI_SITE_THRESHOLD
                         else 'D'),
            })
    out = pd.DataFrame(rows)
    return out.sort_values(['tier', 'n_sites_primary', 'obs'],
                           ascending=[True, False, False])


def main() -> None:
    usage, unmapped = load_usage()
    dom = dominant_units(usage)

    sheet = pd.read_csv(SPREADSHEET)
    sheet['target_clean'] = sheet.med_dose_unit_category.astype(str).map(
        _clean_map(sheet.med_dose_unit_category))

    practice = build_target_vs_practice(dom, sheet)
    recs = build_recommendations(dom, sheet)

    practice.to_csv(HERE / 'target_vs_practice.csv', index=False)
    recs.to_csv(HERE / 'multi_unit_recommendations.csv', index=False)

    print(f'recognized observations : {int(usage.obs.sum()):,}')
    print(f'med_categories charted  : {usage.med_category.nunique()}')
    if not unmapped.empty:
        by_site = (unmapped.groupby('site').obs.sum()
                           .sort_values(ascending=False).astype(int))
        print(f'  ! excluded, no med_category: {int(unmapped.obs.sum()):,} obs  '
              + ', '.join(f'{s} {o:,}' for s, o in by_site.items()))
    print(f'  ... using >1 unit     : '
          f'{(usage.groupby("med_category").clean.nunique() > 1).sum()}')
    print(f'spreadsheet rows        : {len(practice)}')
    print(f'  flagged ZERO          : {(practice.flag == "ZERO").sum()}')
    print(f'  flagged minority      : {(practice.flag == "minority").sum()}')
    print(f'recommendation rows     : {len(recs)} '
          f'across {recs.med_category.nunique()} categories')
    print(recs.tier.value_counts().sort_index().to_string())
    print(f'\nwrote {HERE / "target_vs_practice.csv"}')
    print(f'wrote {HERE / "multi_unit_recommendations.csv"}')


if __name__ == '__main__':
    main()
