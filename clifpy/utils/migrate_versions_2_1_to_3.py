#!/usr/bin/env python3
"""
CLIF 2.1 -> 3.0 directory migration runner.

Wraps :func:`clifpy.utils.crosswalk.crosswalk_file_2_1_to_3_0` to migrate an
entire site data folder in one pass:

  * audits every file in the folder (beta / other-CLIF / excluded / missing),
  * crosswalks the beta tables (standardized VALUES only; nothing else changes),
  * logs every non-beta file but does NOT process or write it (so the PHI
    variant and scratch files are never written -- automatically),
  * verifies each conversion preserved row count, column set, and distinct ID
    counts (schema + metadata only -- no full data load),
  * reports timezone changes (the DuckDB backend relabels tz-aware timestamps to
    UTC, instants preserved -- so that relabel is NOT treated as a failure),
  * wraps each table in try/except so one bad table never aborts the run,
  * logs through clifpy's logger (``clifpy.migrate``) so output lands in clifpy's
    configured logs/ folder rather than the data directory.

Used by the thin entry point ``run_crosswalk.py``. Can also be called directly::

    from clifpy.utils.migrate import CrosswalkMigrationRunner
    ok = CrosswalkMigrationRunner(config_path="config/demo_data_config.yaml").run()

Assumes parquet input (the verification reads parquet metadata).
"""

import logging
import traceback
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Relative imports keep this decoupled from clifpy/__init__ (avoids circular import).
from .crosswalk import crosswalk_file_2_1_to_3_0, BETA_TABLES

log = logging.getLogger("clifpy.migrate")


# --------------------------------------------------------------------------- #
# Schema / metadata helpers (no full data load)
# --------------------------------------------------------------------------- #
def tz_map(path):
    """{column: tz} for timestamp columns. tz=None means timezone-naive."""
    return {f.name: f.type.tz
            for f in pq.read_schema(path)
            if pa.types.is_timestamp(f.type)}


def zones(tzmap):
    """Compact set of timezones across timestamp columns ('naive' = no tz)."""
    return ",".join(sorted({(v or "naive") for v in tzmap.values()})) or "-"


def summary(path, id_cols):
    """rows, column set, distinct ID counts, and per-column tz -- no data load."""
    schema = pq.read_schema(path)
    cols = schema.names
    s = {
        "rows": pq.read_metadata(path).num_rows,
        "cols": set(cols),
        "tz": {f.name: f.type.tz for f in schema if pa.types.is_timestamp(f.type)},
    }
    for c in id_cols:
        if c in cols:
            s[c] = duckdb.sql(
                f"SELECT COUNT(DISTINCT {c}) FROM read_parquet('{path.as_posix()}')"
            ).fetchone()[0]
    return s


def tz_status(src_tz, dst_tz):
    """Return (description, is_concern).

    A plain relabel of a tz-aware column to UTC is what the DuckDB backend does
    and is instant-preserving, so it is NOT a concern. A real zone shift, or a
    tz-aware column becoming naive, IS a concern worth investigating.
    """
    if src_tz == dst_tz:
        return "match", False
    diffs = {c: (src_tz.get(c), dst_tz.get(c))
             for c in set(src_tz) | set(dst_tz)
             if src_tz.get(c) != dst_tz.get(c)}
    relabel_only = all(new == "UTC" and old is not None for old, new in diffs.values())
    if relabel_only:
        return "relabel->UTC (instants preserved)", False
    return f"CHANGED {diffs}", True


# Aligned table layout for the per-table result lines (header + rows share it).
ROW_FMT = "%-30s  %-9s  %-8s  %16s  %-9s  %s"


def _pair(a, b):
    """'1,234' when a == b, else '1,234->5,678' (thousands separators)."""
    return f"{a:,}" if a == b else f"{a:,}->{b:,}"


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
class CrosswalkMigrationRunner:
    """Migrate a directory of CLIF 2.1 tables to 3.0 value conventions."""

    ID_COLS = ["patient_id", "hospitalization_id"]

    def __init__(self, config_path=None, data_dir=None, output_dir=None,
                 filetype="parquet", log_dir=None):
        self.config_path = config_path
        self.data_dir = Path(data_dir) if data_dir else None
        self.output_dir = Path(output_dir) if output_dir else None
        self.filetype = filetype
        self.log_dir = log_dir
        self._resolve_paths()
        self._setup_logging()

    def _resolve_paths(self):
        """Resolve data/output dirs from a config (via the orchestrator) or from
        explicit paths, and make sure clifpy logging is configured either way."""
        if self.config_path and not (self.data_dir and self.output_dir):
            # The orchestrator reads the config AND calls clifpy's setup_logging,
            # so logs route to clifpy's configured logs/ folder.
            from clifpy import ClifOrchestrator
            co = ClifOrchestrator(config_path=self.config_path)
            self.data_dir = self.data_dir or Path(co.data_directory)
            self.output_dir = self.output_dir or Path(co.output_directory)
            self.filetype = co.filetype or self.filetype
        elif not (self.data_dir and self.output_dir):
            raise ValueError(
                "Provide config_path, or both data_dir and output_dir."
            )
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self):
        """Send this runner's output to the console AND to a dedicated, timestamped
        log file in its own logs folder -- separate from the data output.

        Defaults to <output_dir>/logs; pass log_dir to use an existing folder.
        propagate=False isolates this from clifpy's / the root logger so the
        dedicated file gets exactly the migration output, with no duplicates.
        """
        self.log_dir = Path(self.log_dir) if self.log_dir else (self.output_dir / "logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_path = self.log_dir / f"crosswalk_2.1_to_3.0_{ts}.log"

        log.setLevel(logging.INFO)
        log.propagate = False
        log.handlers.clear()           # avoid stacking handlers if constructed again
        fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s")
        for handler in (logging.FileHandler(self.log_path, encoding="utf-8"),
                        logging.StreamHandler()):
            handler.setFormatter(fmt)
            log.addHandler(handler)

    # ----- audit ----------------------------------------------------------- #
    def audit(self):
        """Bucket every file: beta tables (crosswalked), everything else
        (logged but not processed), and beta tables with no file present."""
        all_files = {p.stem.removeprefix("clif_"): p
                     for p in sorted(self.data_dir.glob(f"*.{self.filetype}"))}
        beta     = [t for t in all_files if t in BETA_TABLES]
        non_beta = [t for t in all_files if t not in BETA_TABLES]
        missing  = [t for t in BETA_TABLES if t not in all_files]
        return all_files, beta, non_beta, missing

    # ----- main entry ------------------------------------------------------ #
    def run(self, dry_run=False) -> bool:
        """Run the migration. Returns True on success (no failures, no integrity
        mismatches). is_complete=False is NOT a failure -- it means some values
        need manual mapping and are reported, not lost."""
        all_files, beta, non_beta, missing = self.audit()

        log.info("CLIF 2.1 -> 3.0 migration starting")
        log.info("Data dir : %s", self.data_dir.resolve())
        log.info("Output   : %s", self.output_dir.resolve())
        log.info("Crosswalk (beta tables present)       : %s", beta)
        log.info("Not beta tables (skipped, NOT written): %s", non_beta)
        log.info("Beta tables MISSING from this folder  : %s", missing)
        log.info("-" * 90)

        if not all_files:
            log.warning("No '*.%s' files found in %s -- nothing to do.",
                        self.filetype, self.data_dir)
            return True
        if dry_run:
            log.info("dry-run: audit only, no files written.")
            return True

        counts = dict(converted=0, mismatch=0, skipped=0, failed=0, incomplete=0)
        results = {}

        log.info(ROW_FMT, "table", "check", "values", "rows", "tz", "ids")
        log.info(ROW_FMT, "-" * 30, "-" * 9, "-" * 8, "-" * 16, "-" * 9, "-" * 20)
        for tb in beta:
            self._crosswalk_one(tb, all_files[tb], results, counts)

        self._report_incomplete(results)
        self._summary(counts)
        return counts["failed"] == 0 and counts["mismatch"] == 0

    # ----- per-table work -------------------------------------------------- #
    def _crosswalk_one(self, tb, in_path, results, counts):
        out_path = self.output_dir / in_path.name
        if out_path.exists():
            log.info("%-32s SKIP (output already exists)", tb)
            counts["skipped"] += 1
            return
        try:
            report = crosswalk_file_2_1_to_3_0(str(in_path), str(out_path), tb)

            src = summary(in_path, self.ID_COLS)
            dst = summary(out_path, self.ID_COLS)
            checks = {"rows": src["rows"] == dst["rows"],
                      "cols": src["cols"] == dst["cols"]}
            for c in self.ID_COLS:
                if c in src:
                    checks[c] = src[c] == dst[c]
            integrity_ok = all(checks.values())

            tz_desc, tz_concern = tz_status(src["tz"], dst["tz"])
            if not integrity_ok:
                check = "MISMATCH"
            elif tz_concern:
                check = "TZ-WARN"
            else:
                check = "OK"
            values = "complete" if report["is_complete"] else "REVIEW"

            results[tb] = {"is_complete": report["is_complete"], "report": report}

            src_z, dst_z = zones(src["tz"]), zones(dst["tz"])
            tz_disp = src_z if src_z == dst_z else f"{src_z}->{dst_z}"
            ids = "  ".join(
                f"{label}={_pair(src[c], dst[c])}"
                for c, label in (("patient_id", "pt"), ("hospitalization_id", "hosp"))
                if c in src
            )
            log.info(ROW_FMT, tb, check, values,
                     _pair(src["rows"], dst["rows"]), tz_disp, ids)

            counts["converted"] += 1
            if not integrity_ok:
                counts["mismatch"] += 1
                log.error("   %s INTEGRITY FAILED -> %s",
                          tb, [k for k, v in checks.items() if not v])
            if tz_concern:
                log.warning("   %s timezone change: %s", tb, tz_desc)
            if not report["is_complete"]:
                counts["incomplete"] += 1

        except Exception:
            counts["failed"] += 1
            log.error("%-32s FAILED to convert\n%s", tb, traceback.format_exc())
            # Drop any partial output so a rerun retries this table cleanly.
            if out_path.exists():
                try:
                    out_path.unlink()
                except OSError:
                    log.error("   could not remove partial output %s", out_path)

    # ----- reporting ------------------------------------------------------- #
    def _report_incomplete(self, results):
        incomplete = {tb: r for tb, r in results.items() if not r["is_complete"]}
        if not incomplete:
            return
        log.info("-" * 90)
        log.info("VALUES NEEDING MANUAL MAPPING (is_complete=False) -- left as-is in output:")
        for tb, r in incomplete.items():
            for col, info in r["report"].get("columns", {}).items():
                flagged = (info.get("ambiguous") or []) + (info.get("unresolved") or [])
                if flagged:
                    log.info("   %-28s %-24s %s",
                             tb, col, [f.get("original") for f in flagged])

    def _summary(self, c):
        log.info("=" * 90)
        log.info("DONE.  converted=%(converted)d  skipped=%(skipped)d  "
                 "failed=%(failed)d  mismatch=%(mismatch)d  needs-review=%(incomplete)d", c)
        log.info("CLIF 3.0 output written to: %s", self.output_dir.resolve())
        log.info("Run log saved to:           %s", self.log_path.resolve())