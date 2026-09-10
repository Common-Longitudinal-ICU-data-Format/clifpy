"""Step 1 -- what dose units does this site actually chart?

Computes the site's own `(med_category, med_dose_unit) -> observation count`
distribution directly from `medication_admin_continuous`, normalises each unit
through clifpy's own cleaning chain, and classifies it as weighted /
unweighted / other.

Deliberately computed from live data rather than read from the consortium DQA
export: a site running this has no such export, and regenerating means the
inventory is never stale.

Normalising through clifpy's chain matters -- `mcg/kg/minute` and `mcg/kg/min`
are the same unit, and counting them as disagreement would manufacture findings
that are pure spelling.
"""

from __future__ import annotations

import re
from typing import Any

import duckdb
import pandas as pd

from _shared import (
    DRUG_FAMILY,
    FORCED_CONVERSION_DRUGS,
    Settings,
    logger,
    suppress_small_cells,
    write_output,
)

WEIGHT_QUALIFIER = re.compile(r'/(kg|lb)(/|$)')


def _classify(clean_unit: str | None, acceptable: set[str]) -> str:
    """weighted | unweighted | unrecognized for a cleaned unit string."""
    if not isinstance(clean_unit, str) or clean_unit not in acceptable:
        return 'unrecognized'
    return 'weighted' if WEIGHT_QUALIFIER.search(clean_unit) else 'unweighted'


def run(settings: Settings, results: dict[str, Any]) -> dict[str, Any]:
    """Build the site's unit inventory.

    Returns the per-(category, unit) frame for later steps, which is kept in
    memory; only the aggregate is written.
    """
    from clifpy import load_data
    from clifpy.utils.unit_converter import (
        ALL_ACCEPTABLE_UNITS,
        _clean_dose_unit_formats_duckdb,
        _clean_dose_unit_names_duckdb,
    )

    # Only three columns are needed. hospitalization_id is used in memory to
    # count distinct encounters for small-cell suppression and never written.
    mac = load_data(
        'medication_admin_continuous',
        config_path=str(settings.config_path),
        columns=['hospitalization_id', 'med_category', 'med_dose_unit'],
        return_format='pandas',
    )
    logger.info('loaded medication_admin_continuous: %d rows', len(mac))

    raw = duckdb.sql("""
        SELECT med_category
             , med_dose_unit
             , COUNT(*)                        AS n_obs
             , COUNT(DISTINCT hospitalization_id) AS n_hospitalizations
        FROM mac
        GROUP BY med_category, med_dose_unit
    """)

    rel = _clean_dose_unit_formats_duckdb(raw, col='med_dose_unit', out_col='clean_unit')
    rel = _clean_dose_unit_names_duckdb(rel, col='clean_unit')
    inv = rel.to_df()

    acceptable = set(ALL_ACCEPTABLE_UNITS)
    inv['weight_axis'] = inv['clean_unit'].map(lambda u: _classify(u, acceptable))
    inv['mcide_target'] = inv['med_category'].map(FORCED_CONVERSION_DRUGS)
    inv['family'] = inv['med_category'].map(DRUG_FAMILY).fillna('other')
    # A row needs forcing when mCIDE targets a weighted unit and the site is
    # charting an unweighted one.
    inv['needs_forcing'] = (
        inv['mcide_target'].notna() & (inv['weight_axis'] == 'unweighted')
    )
    inv['site_name'] = settings.site_name

    cols = ['site_name', 'med_category', 'family', 'med_dose_unit', 'clean_unit',
            'weight_axis', 'mcide_target', 'needs_forcing',
            'n_obs', 'n_hospitalizations']
    inv = inv[cols].sort_values(['med_category', 'n_obs'], ascending=[True, False])

    written = suppress_small_cells(
        inv, count_col='n_hospitalizations', min_cell=settings.min_cell_size,
    )
    write_output(written, settings, 'unit_inventory.csv')

    # ---- per-drug rollup, restricted to the drugs under study --------------
    study = inv[inv['med_category'].isin(settings.drugs)].copy()
    if study.empty:
        logger.warning(
            'none of the %d study drugs appear at this site -- '
            'later steps will have nothing to measure', len(settings.drugs)
        )
    rollup = (
        study.assign(
            wt_obs=lambda d: d.n_obs.where(d.weight_axis == 'weighted', 0),
            unwt_obs=lambda d: d.n_obs.where(d.weight_axis == 'unweighted', 0),
            other_obs=lambda d: d.n_obs.where(d.weight_axis == 'unrecognized', 0),
        )
        .groupby(['med_category', 'family', 'mcide_target'], dropna=False)
        .agg(n_obs=('n_obs', 'sum'),
             n_hospitalizations=('n_hospitalizations', 'max'),
             weighted_obs=('wt_obs', 'sum'),
             unweighted_obs=('unwt_obs', 'sum'),
             unrecognized_obs=('other_obs', 'sum'),
             n_distinct_units=('clean_unit', 'nunique'))
        .reset_index()
    )
    if not rollup.empty:
        rollup['pct_weighted'] = 100 * rollup.weighted_obs / rollup.n_obs
        rollup['forced_obs'] = rollup.unweighted_obs
        rollup['pct_forced'] = 100 * rollup.forced_obs / rollup.n_obs
        rollup['site_name'] = settings.site_name
        rollup = rollup.sort_values('forced_obs', ascending=False)

    write_output(
        suppress_small_cells(
            rollup, count_col='n_hospitalizations', min_cell=settings.min_cell_size,
        ) if not rollup.empty else rollup,
        settings, 'unit_inventory_by_drug.csv',
    )

    total_forced = int(rollup.forced_obs.sum()) if not rollup.empty else 0
    drugs_forced = int((rollup.forced_obs > 0).sum()) if not rollup.empty else 0
    logger.info(
        'forced-conversion exposure: %s observations across %d drug(s)',
        f'{total_forced:,}', drugs_forced,
    )

    # Drugs the site does not chart at all are a finding, not an error.
    absent = sorted(set(settings.drugs) - set(study.med_category.unique()))
    if absent:
        logger.info('%d study drug(s) absent at this site: %s',
                    len(absent), ', '.join(absent[:8])
                    + (f', +{len(absent) - 8} more' if len(absent) > 8 else ''))

    return {
        'inventory': inv,
        'by_drug': rollup,
        'drugs_present': sorted(study.med_category.unique()) if not study.empty else [],
        'drugs_absent': absent,
        'total_forced_obs': total_forced,
        'n_mac_rows': len(mac),
    }
