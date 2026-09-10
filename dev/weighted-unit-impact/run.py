"""Weighted dose unit impact analysis -- single entry point.

Usage:
    make weight-impact CONFIG=my_site_config.yaml

    # or directly
    uv run python dev/weighted-unit-impact/run.py --config my_site_config.yaml

Runs three steps in order and writes aggregates to
`output/weighted_unit_impact/<site_name>/`:

    1. unit inventory      -- what units this site actually charts
    2. weight availability -- when is a usable weight present, and when not
    3. conversion impact   -- what forcing the mCIDE target would cost

Each step is independent enough to fail without taking the others down: a site
missing the `patient` table still gets steps 1 and 3, and `run_manifest.json`
records what was skipped and why. Partial results beat no results when the
person running this cannot be debugged interactively.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _shared import load_settings, setup_logging, write_manifest  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='weighted-unit-impact',
        description='Measure what forcing weight-indexed dose units would cost.',
    )
    p.add_argument(
        '--config', required=True,
        help='path to your site config yaml (copy config_template.yaml)',
    )
    p.add_argument(
        '--steps', default='inventory,weights,impact',
        help='comma-separated subset of steps to run (default: all)',
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        settings = load_settings(args.config)
    except (FileNotFoundError, ValueError) as exc:
        # Config problems are the most common failure at an unfamiliar site,
        # so report them plainly rather than as a traceback.
        print(f'\nconfiguration error:\n  {exc}\n', file=sys.stderr)
        return 2

    log = setup_logging(settings)
    log.info('site=%s  clif_version=%s  drugs=%d',
             settings.site_name, settings.clif_version, len(settings.drugs))
    log.info('output -> %s', settings.output_dir)

    requested = [s.strip() for s in args.steps.split(',') if s.strip()]
    manifest: dict = {'steps': {}, 'skipped': {}}

    # Imported lazily so a broken step cannot stop the others from loading.
    import _inventory
    import _weight_scenarios
    import _impact
    import _report

    steps = [
        ('inventory', _inventory.run),
        ('weights', _weight_scenarios.run),
        ('impact', _impact.run),
    ]

    results: dict = {}
    for name, fn in steps:
        if name not in requested:
            manifest['skipped'][name] = 'not requested'
            log.info('step %s: skipped (not requested)', name)
            continue
        log.info('--- step: %s ---', name)
        try:
            results[name] = fn(settings, results)
            manifest['steps'][name] = 'ok'
        except Exception as exc:
            # One step failing must not cost the others. The reason lands in
            # the manifest so a partial upload is still interpretable.
            manifest['steps'][name] = 'failed'
            manifest['skipped'][name] = f'{type(exc).__name__}: {exc}'
            log.error('step %s FAILED: %s', name, exc)
            log.debug(traceback.format_exc())

    try:
        _report.run(settings, results, manifest)
        manifest['steps']['report'] = 'ok'
    except Exception as exc:
        manifest['steps']['report'] = 'failed'
        manifest['skipped']['report'] = f'{type(exc).__name__}: {exc}'
        log.error('report FAILED: %s', exc)

    write_manifest(settings, manifest)

    failed = [k for k, v in manifest['steps'].items() if v == 'failed']
    if failed:
        log.warning('finished with %d failed step(s): %s', len(failed), failed)
        log.warning('partial results in %s are still worth sending back',
                    settings.output_dir)
        return 1

    log.info('done. send back: %s', settings.output_dir)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
