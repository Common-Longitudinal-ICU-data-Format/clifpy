#!/usr/bin/env python3
"""
Entry point for the CLIF 2.1 -> 3.0 crosswalk migration.

Thin wrapper around clifpy.utils.migrate.CrosswalkMigrationRunner -- mirrors the
run_tableone.py pattern: parse args, build the runner, run, exit with a code.

Usage:
    uv run run_crosswalk.py --config config/demo_data_config.yaml
    uv run run_crosswalk.py --data-dir <in_folder> --output-dir <out_folder>
    uv run run_crosswalk.py --config config/demo_data_config.yaml --dry-run
    uv run run_crosswalk.py --config config/demo_data_config.yaml --no-copy-through
"""
import sys
from pathlib import Path

from clifpy.utils.migrate_versions_2_1_to_3 import CrosswalkMigrationRunner

# Default config, resolved relative to THIS script (repo root) so it works no
# matter which directory you launch from. Override with --config for real sites.
DEFAULT_CONFIG = Path(__file__).resolve().parent / "config" / "demo_data_config.yaml"


def main():
    """Main entry point for the CLIF 2.1 -> 3.0 migration."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate a folder of CLIF 2.1 tables to 3.0 value conventions."
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="Path to a CLIF config YAML (supplies data_directory / output_directory). "
             "Default: %(default)s",
    )
    parser.add_argument(
        "--data-dir",
        help="Input folder of CLIF 2.1 files. Overrides the config's data_directory.",
    )
    parser.add_argument(
        "--output-dir",
        help="Destination folder for the 3.0 files. Overrides the config's output_directory.",
    )
    parser.add_argument(
        "--filetype",
        default="parquet",
        help="Data file type (default: parquet).",
    )
    parser.add_argument(
        "--log-dir",
        help="Folder for the run log (default: <output_dir>/logs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Audit and report what would happen, but write nothing.",
    )
    args = parser.parse_args()

    # When relying on a config file (i.e. explicit dirs not both given), make sure
    # it exists and explain clearly if it doesn't, instead of a deeper error.
    using_config = not (args.data_dir and args.output_dir)
    if using_config and args.config and not Path(args.config).exists():
        print(f"\nFatal error: config file not found: {args.config}\n"
              f"Pass --config <path>, or --data-dir <in> --output-dir <out>.")
        sys.exit(1)

    try:
        runner = CrosswalkMigrationRunner(
            config_path=args.config,
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            filetype=args.filetype,
            log_dir=args.log_dir,
        )
        success = runner.run(dry_run=args.dry_run)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()