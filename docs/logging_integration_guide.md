# clifpy Logging — Integration Guide

> Internal contributor / AI-agent reference. Not registered in `mkdocs.yml`.
> Canonical implementation lives in `clifpy/utils/sofa2/`.

This document is the **integration playbook** for downstream projects built on
`clifpy`. It tells you the minimum-viable correct way to wire up the
centralized logger, the per-module naming convention, and the patterns to copy
from sofa2 — written in prescriptive **CLAUDE.md style**: terse imperatives
with one paragraph of explanation, a small snippet, and a `file:line` cite
into the canonical implementation.

`docs/logging.md` is the **reference manual** (what the logger does, how
formatters work, troubleshooting). This doc is the **how-to-use-it-correctly**
companion. If you only read one section, read **TL;DR**.

---

## TL;DR — the seven rules

1. **Call `setup_logging()` exactly once, at the entry point.** Never inside library code or per-cell in a notebook.

2. **Get a per-module logger with `get_logger("<area>.<module>")`** as a module-level singleton at import time.

3. **Use a hardcoded short name, not `__name__`.** Mirrors `utils.sofa2.resp`. Survives renames; renders cleanly in the console formatter.

4. **Pick the right level.** INFO for pipeline milestones + config echo, DEBUG for row counts and other expensive payloads, WARNING for missing-but-optional inputs. Raise on errors — sofa2 doesn't use `logger.error`.

5. **Don't embed emojis in messages.** The `EmojiFormatter` injects them per-level automatically and falls back to ASCII on Windows.

6. **Gate expensive log payloads on `logger.isEnabledFor(logging.DEBUG)`.** Avoids paying for `count(*)` queries when DEBUG is off.

7. **Never call `print()` or configure handlers from a library module.** Only the entry point sets up handlers.

---

## 1. Set up logging once, at the entry point

`setup_logging()` is **idempotent but not free** — it tears down and rebuilds
all handlers each call (`clifpy/utils/logging_config.py:113-114`). Calling it
from inside a library module would clobber whatever configuration the caller
installed (different output directory, different level, custom handlers).

```python
# main.py — your project's entry point
from clifpy import setup_logging

setup_logging(output_directory="./output")
# → ./output/logs/clifpy_all.log
# → ./output/logs/clifpy_errors.log (warnings + errors only)
# → console mirror with emoji + short logger names
```

If your project uses `ClifOrchestrator`, it already calls `setup_logging()`
from its `__init__`. Don't call it again.

**Cite (negative — the canonical reference is a pure consumer):**
`clifpy/utils/sofa2/` contains zero `setup_logging()` calls. Every sofa2
module assumes the caller has already configured logging.

---

## 2. Get a logger per module — module-level singleton

Each sofa2 submodule declares one logger at import time, top-of-file, right
after the imports:

```python
from clifpy.utils.logging_config import get_logger

logger = get_logger("utils.sofa2.resp")
```

`get_logger()` auto-prefixes `clifpy.` if missing
(`clifpy/utils/logging_config.py:181-182`), so the resolved logger name is
`clifpy.utils.sofa2.resp`. Module-level (not per-call) means the logger
registry is hit once per import rather than once per function call.

**Cite:** `_core.py:35`, `_resp.py:32`, `_cv.py:28`, `_brain.py:31`,
`_liver.py:22`, `_kidney.py:35`, `_hemo.py:22` — every sofa2 module follows
this exact pattern.

---

## 3. Naming convention: hardcoded short name, not `__name__`

sofa2 deliberately uses **hardcoded short names** in `get_logger()` calls:

| Module | Logger name passed | Resolved full name |
|---|---|---|
| `_core.py` | `"utils.sofa2.core"` | `clifpy.utils.sofa2.core` |
| `_resp.py` | `"utils.sofa2.resp"` | `clifpy.utils.sofa2.resp` |
| `_cv.py` | `"utils.sofa2.cv"` | `clifpy.utils.sofa2.cv` |
| `_brain.py` | `"utils.sofa2.brain"` | `clifpy.utils.sofa2.brain` |
| `_liver.py` | `"utils.sofa2.liver"` | `clifpy.utils.sofa2.liver` |
| `_kidney.py` | `"utils.sofa2.kidney"` | `clifpy.utils.sofa2.kidney` |
| `_hemo.py` | `"utils.sofa2.hemo"` | `clifpy.utils.sofa2.hemo` |

Two reasons this is preferred over `get_logger(__name__)`:

- **Cleaner display.** The `EmojiFormatter` strips the `clifpy.` prefix for console output (`clifpy/utils/logging_config.py:48-49`), so the shortname is what users see. `__name__` would render as `utils.sofa2._resp` with a leading underscore — visually noisy and inconsistent with the other modules.

- **Rename-safe filtering.** External tools that grep `output/logs/clifpy_all.log` for `utils.sofa2.cv` keep working when the file moves or is renamed; `__name__` couples log filters to the file path.

For a downstream project, mirror the pattern with your own top-level prefix:

```python
# myproject/cohort/builder.py
logger = get_logger("myproject.cohort.builder")
# → resolves to clifpy.myproject.cohort.builder
# → renders as "myproject.cohort.builder" in console
```

**Cite:** `_resp.py:32` (canonical declaration);
`clifpy/utils/logging_config.py:48-49` (the prefix-stripping that makes the
short name worth caring about).

---

## 4. Choose the right level

| Level | Use for | sofa2 example |
|---|---|---|
| INFO | Pipeline milestones, configuration echo, cohort metrics | `_resp.py:163` `"Calculating respiratory subscore..."` + `:167` config echo |
| DEBUG | Row counts, intermediate shapes, expensive diagnostics | `_cv.py:208` (gated count of `pressor_events_raw`) |
| WARNING | Optional input missing, fallback path taken | `_core.py:68` `"ECMO/MCS table not available... will be skipped."` |
| ERROR / EXCEPTION | sofa2 doesn't use these — failures raise | (intentionally absent) |

The recurring sofa2 INFO pattern: announce-step, echo-config, then per-step
sub-headers. This makes `clifpy_all.log` readable as a narrative of what the
pipeline did and which knobs were active.

```python
logger.info("Calculating respiratory subscore...")
logger.info(f"resp_lookback_hours={lookback_hours}, pf_sf_tolerance_minutes={tolerance_minutes}")
logger.info("Applying device heuristic: inferring IMV from mode_category...")
```

**Cite:** `_resp.py:163-172` (announce + config echo + step header);
`_cv.py:89-96` (same shape for the cardiovascular subscore).

For optional inputs that may or may not be present at the site, the canonical
WARNING form names the table, the exception, and the consequence:

```python
try:
    rel = load_data('ecmo_mcs', config_path=clif_config_path, return_rel=True)
except Exception as e:
    logger.warning(f"ECMO/MCS table not available ({e}). ECMO scoring will be skipped.")
    return EMPTY_ECMO
```

**Cite:** `_core.py:68, 108, 145, 181, 218` — same template across five
optional tables.

---

## 5. Don't embed emojis in messages

The `EmojiFormatter` reads `record.levelname` and prefixes the appropriate
emoji from `EMOJI_MAP` automatically (`clifpy/utils/logging_config.py:21-50`).
On Windows, it falls back to ASCII tags (`[I]`, `[!]`, `[X]`, etc.) because
the cp1252 console encoder cannot render `📢` or `⚠️` — embedded Unicode
emojis would crash on Windows.

```python
# Good — formatter adds the emoji per-level
logger.info("Loading respiratory_support table...")

# Bad — duplicates the formatter's emoji and blows up on Windows
logger.info("📢 Loading respiratory_support table...")
```

`docs/logging.md` shows emoji-prefixed lines in some example **output** blocks;
those are illustrating the rendered console line, not the source string. The
source strings in sofa2 contain zero emojis.

**Cite:** `clifpy/utils/logging_config.py:23-38` (Windows fallback); grep
`clifpy/utils/sofa2/` for emoji characters in `logger.*` calls — there are
none.

---

## 6. Gate expensive log payloads on `isEnabledFor(DEBUG)`

DuckDB row-count queries, Pandas `.shape` calls on freshly materialized
relations, and `.describe()` summaries all force execution. If you log them
unconditionally inside an `f"..."`, the cost is paid even when the log line
will be discarded. Wrap them so the work only happens when DEBUG is enabled:

```python
import logging

if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"pressor_events_raw materialized: {pressor_events_raw.count('*').fetchone()[0]} rows")
else:
    logger.info("pressor_events_raw materialized")
```

The `else` branch keeps an INFO-level breadcrumb so users running at the
default verbosity still see that the step happened — they just don't pay for
the row count. This is the pattern sofa2 uses at every materialization
boundary in the cardiovascular subscore.

**Cite:** `_cv.py:207-210, 257-260, 282-285, 357-360` — four mirrored
instances around `pressor_events_raw`, dedup output, agg output, and per-class
splits.

---

## 7. Never call `print()` or configure handlers from a library module

sofa2 has zero `print()` calls and zero direct handler manipulation across the
entire package. All output flows through `logger.*`. This buys three things:

- **Test capture works.** `pytest`'s `caplog` fixture sees every message; nothing escapes to stdout via `print()`.

- **Quiet mode actually quiets.** `setup_logging(level=logging.WARNING)` silences INFO; if `print()` calls existed they'd ignore the level and keep printing.

- **Console / file split stays consistent.** Every line goes through the same `EmojiFormatter`, so log files and console show the same content modulo level filtering.

If you find yourself reaching for `print()` to "just see what's happening",
that's a `logger.debug()` or `logger.info()` instead — same convenience, plays
nicely with the rest of the system.

**Cite (negative):** grep `clifpy/utils/sofa2/` for `print(` — zero
occurrences.

---

## 8. Anti-patterns to avoid

- **`setup_logging()` inside a library function or per notebook cell.** Tears down handlers the caller installed; in a notebook you re-pay setup cost on every run.

- **`logger = get_logger(__name__)`.** Works, but yields names like `utils.sofa2._resp` (leading underscore from the private-module convention) that render inconsistently in the console. Use a hardcoded short name instead.

- **Embedding emojis in messages.** Crashes on Windows; duplicates formatter output everywhere else.

- **`logger.info(f"... {expensive_count_query()}")`.** Forces the query even when INFO is silenced. Gate on `isEnabledFor(DEBUG)` or move to plain `logger.debug` (which still evaluates the f-string but at least communicates intent).

- **`logger.error(...)` followed by `raise`.** Pick one. sofa2's convention is to raise — the entry-point handler logs the traceback once, instead of producing two log entries (the manual error log + the traceback) for a single failure.

- **`logging.getLogger("my_module")` without the `clifpy.` prefix.** Bypasses the centralized handlers entirely; messages disappear into the root Python logger with no formatter and no file output. See `docs/logging.md` "Why the prefix matters".

- **Reconfiguring logging per-cell in a Jupyter / marimo notebook.** Call `setup_logging()` once in the first cell; every later cell just calls `get_logger(...)`.

---

## 9. Quickstart for a new clifpy-based project

The minimum-viable two-file setup:

```python
# main.py — entry point
import logging
from clifpy import setup_logging, get_logger, ClifOrchestrator

setup_logging(output_directory="./output")
# Or, for verbose debug runs:
# setup_logging(output_directory="./output", level=logging.DEBUG)

logger = get_logger("myproject.main")
logger.info("Starting cohort build")

clif = ClifOrchestrator(
    data_directory="./data",
    filetype="parquet",
    timezone="US/Central",
    output_directory="./output",
)
clif.load_table("labs")

logger.info("Cohort build complete")
```

```python
# myproject/cohort.py — library module
import logging
from clifpy.utils.logging_config import get_logger

logger = get_logger("myproject.cohort")  # → clifpy.myproject.cohort

def build_cohort(input_df):
    logger.info(f"Building cohort with {len(input_df)} candidates")

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Input columns: {list(input_df.columns)}")

    if 'optional_table' not in input_df.columns:
        logger.warning("optional_table column missing — falling back to defaults")

    # ... build cohort ...
    return result
```

That's the whole integration. Three calls — `setup_logging` once at the top,
`get_logger("<your-namespace>")` per module, and standard `logger.{info,
debug, warning}` calls inside functions.

---

## Appendix A — Public API reference

Both functions are re-exported at the package root
(`clifpy/__init__.py:34, 90-91`), so `from clifpy import setup_logging, get_logger`
works directly.

| Function | Signature | Purpose |
|---|---|---|
| `setup_logging` | `(output_directory=None, level=logging.INFO, console_output=True, separate_error_log=True) -> Logger` | Configure the `clifpy` root logger and its handlers. Idempotent. Call once at entry point. (`clifpy/utils/logging_config.py:53-155`) |
| `get_logger` | `(name: str) -> Logger` | Return a logger named `clifpy.<name>` (auto-prefixes if missing). (`clifpy/utils/logging_config.py:158-183`) |

Defaults of note for `setup_logging`:

- `output_directory=None` → uses `os.getcwd() + "/output"`. Logs land in `./output/logs/`.

- `level=logging.INFO` → captures INFO and above. Pass `logging.DEBUG` for row-count diagnostics.

- `console_output=True` → mirrors everything to stdout with the short-name formatter.

- `separate_error_log=True` → also writes warnings + errors to `clifpy_errors.log` for quick triage.

---

## Appendix B — sofa2 cite map

One-line index from rule to canonical example. Use this when this doc says
"follow the sofa2 pattern" and you want to read live code.

| Rule | File | Line(s) | What's there |
|---|---|---|---|
| Module-level `get_logger` declaration | `_core.py` | 35 | `logger = get_logger("utils.sofa2.core")` |
| Module-level `get_logger` declaration | `_resp.py` | 32 | `logger = get_logger("utils.sofa2.resp")` |
| Module-level `get_logger` declaration | `_cv.py` | 28 | `logger = get_logger("utils.sofa2.cv")` |
| Module-level `get_logger` declaration | `_brain.py` | 31 | `logger = get_logger("utils.sofa2.brain")` |
| Module-level `get_logger` declaration | `_liver.py` | 22 | `logger = get_logger("utils.sofa2.liver")` |
| Module-level `get_logger` declaration | `_kidney.py` | 35 | `logger = get_logger("utils.sofa2.kidney")` |
| Module-level `get_logger` declaration | `_hemo.py` | 22 | `logger = get_logger("utils.sofa2.hemo")` |
| Subscore-entry INFO + config echo | `_resp.py` | 163, 167, 172 | "Calculating ..." + config + step header |
| Subscore-entry INFO + config echo | `_cv.py` | 89–96 | Mirrored shape for CV |
| WARNING for missing optional table | `_core.py` | 68, 108, 145, 181, 218 | ECMO, intermittent meds, output, input, CRRT |
| Conditional DEBUG row count | `_cv.py` | 207–210, 257–260, 282–285, 357–360 | `isEnabledFor(DEBUG)` gating around `count('*')` |
| Cohort row-count INFO metric | `_core.py` | 347, 354 | Batching summary lines |
| Multi-line config dump (Windows path) | `_core.py` | 316–317 | Loop emitting `cfg.summary()` lines as INFO |
| Top-level re-exports | `clifpy/__init__.py` | 34, 90–91 | `setup_logging`, `get_logger` available from `clifpy` |
| `EmojiFormatter` + Windows fallback | `clifpy/utils/logging_config.py` | 21–50 | EMOJI_MAP, format() override, shortname stripping |
| Idempotent handler reset | `clifpy/utils/logging_config.py` | 113–114 | `root_logger.handlers = []` on re-entry |
| Auto-prefix `clifpy.` | `clifpy/utils/logging_config.py` | 181–182 | `if not name.startswith('clifpy.')` |

---

## Further reading

- `docs/logging.md` — full reference manual (levels, namespaces, file layout, troubleshooting).

- `clifpy/utils/logging_config.py` — implementation, ~180 lines, readable end-to-end.

- `docs/duckdb_perf_guide.md` — sister contributor reference for the DuckDB patterns used by sofa2.

- Python `logging` HOWTO: <https://docs.python.org/3/howto/logging.html> — only if you're new to stdlib `logging`.
