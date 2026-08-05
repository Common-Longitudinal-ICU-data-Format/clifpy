# DuckDB + Python Performance Guide

> Internal contributor / AI-agent reference. Not registered in `mkdocs.yml`.
> Canonical implementation lives in `clifpy/utils/sofa2/`.

This document codifies the patterns used by the SOFA-2 pipeline so other modules
and downstream CLIF projects can reuse them. The rules below are written in
prescriptive **CLAUDE.md style** — terse imperatives with one paragraph of
explanation, a small snippet, and a `file:line` cite into the canonical
implementation.

If you only read one section, read **TL;DR**.

---

## TL;DR — the eight rules

1. **Stay lazy by default.** A `DuckDBPyRelation` is a query plan, not data. Chain `duckdb.sql(...)` / `.filter` / `.project` until you actually need rows.

2. **Materialize at three places only.** (a) at source-engine boundaries (pandas/Polars → DuckDB), (b) when a relation is referenced more than once, and (c) at small subscore-shaped boundaries.

3. **Replacement scans, not `.register()`.** DuckDB resolves Python-scope DataFrame variables automatically. `.register()` is only for variables not visible in local scope.

4. **Push predicates and column lists into the source.** Use `load_data(..., return_rel=True, columns=[...], filters={...})` — this propagates into the parquet scan.

5. **Reuse one connection.** Reconnecting drops the metadata cache and re-pays parse/plan overhead.

6. **Bound resources with a context manager.** `memory_limit`, `threads`, `temp_directory` should be SET on entry and restored on exit so library code doesn't leak settings.

7. **Use SQL idioms over hand-rolled Python:** `ASOF JOIN`, `QUALIFY ROW_NUMBER()`, `LAG → cumulative SUM` for episode runs, `SEMI JOIN` / `ANTI JOIN`, `UNION ALL` (not `UNION`).

8. **Track every temp table in a registry; drop them after the terminal `.df()`.**

---

## 1. The relational API is lazy — exploit it

A `DuckDBPyRelation` is a symbolic query plan. Nothing executes until you call
an **output method** (`.df()`, `.pl()`, `.arrow()`, `.fetchall()`, `.to_table()`,
`.to_view()`, `.create_view()`, etc.).

This means you can build deeply nested queries without paying for intermediate
rows. DuckDB's optimizer combines predicates, prunes columns, and reorders
joins across the whole DAG before a single tuple flows.

```python
import duckdb

# All lazy — no execution yet.
rel = duckdb.sql("FROM 'labs.parquet' SELECT hospitalization_id, lab_value_numeric")
filtered = duckdb.sql("FROM rel WHERE lab_value_numeric > 0")
agg = duckdb.sql("FROM filtered SELECT hospitalization_id, AVG(lab_value_numeric) GROUP BY 1")

# Single execution here. The optimizer fuses the three steps.
df = agg.df()
```

**Cite (canonical chain):**

- `clifpy/utils/sofa2/_resp.py:174-221` chains predicate → join → window functions
  in a single `duckdb.sql()` block without materializing.

**Output methods reference (from DuckDB docs):**

| Method | Returns | Side effect |
|---|---|---|
| `.df()` / `.fetchdf()` / `.to_df()` | pandas DataFrame | execute |
| `.pl()` | Polars DataFrame | execute |
| `.arrow()` / `.to_arrow_table()` | Arrow Table | execute |
| `.fetchall()` | list of tuples | execute |
| `.to_table("name")` / `.create("name")` | None | persist as table |
| `.to_view("name")` / `.create_view("name")` | None | persist as view |
| `.to_parquet("path")` / `.to_csv("path")` | None | write file |

---

## 2. When to materialize — and how

The rule sofa2 follows: **materialize only at the three boundary types below.**
Everything else stays lazy.

### 2a. At source-engine boundaries (pandas / Polars → DuckDB)

If the input is a pandas or Polars DataFrame, **copy it into a DuckDB temp
table before joining**. DuckDB's scan path for foreign DataFrames lacks
cardinality and min/max statistics, so the optimizer falls back to nested-loop
joins on temporal windows.

```python
# cohort_df is a pd.DataFrame or pl.DataFrame
duckdb.execute("CREATE OR REPLACE TEMP TABLE _sofa2_cohort AS SELECT * FROM cohort_df")
cohort_rel = duckdb.table("_sofa2_cohort")
```

**Cite:** `clifpy/utils/sofa2/_core.py:389-395` — comment explicitly notes the
Polars-statistics issue.

### 2b. When a relation is referenced more than once

If a lazy relation is read N times, the underlying parquet file is scanned N
times. Materialize once, after filtering to the cohort, then reference the
temp table everywhere else.

```python
duckdb.execute(f"""
    CREATE OR REPLACE TEMP TABLE _clif_labs AS
    FROM labs_rel t
    SEMI JOIN cohort_rel c ON t.{id_name} = c.{id_name}
    SELECT t.*
""")
labs_rel = duckdb.table("_clif_labs")
```

`SEMI JOIN` is preferred over `WHERE x IN (SELECT ...)` because it cannot
duplicate rows on the left side, even if the cohort key is non-unique.

**Cite:** `clifpy/utils/sofa2/_core.py:475-494` — labs_rel is referenced ~8
times across 4 subscores; materializing once after `SEMI JOIN` cuts downstream
table size by ~99% for small cohorts.

### 2c. At small "subscore" boundaries

Once a subscore relation has been computed (one row per scoring window —
small), materialize it so the assembly query joins six small temp tables
instead of re-executing six deep lazy DAGs.

```python
def _materialize_subscore(name, rel):
    table_name = f"_sofa2_{name}"
    duckdb.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT * FROM rel")
    _register_temp_table(table_name)
    return duckdb.table(table_name)
```

**Cite:** `clifpy/utils/sofa2/_perf.py:53-75`.

### How — preferred forms

In order of preference:

- `CREATE OR REPLACE TEMP TABLE name AS FROM rel SELECT *`, then `duckdb.table("name")`. This is what sofa2 uses because the table name is reusable across SQL strings via replacement scan.

- `rel.to_table("name")` — equivalent PyRelation method.

- `rel.create_view("name")` — when the result will only be referenced once but you want a stable name (rare in sofa2).

### What NOT to materialize

- Every intermediate. Lazy chaining is the default.

- Flag relations from `_flag_*` functions — they are kept lazy and `LEFT JOIN`'d in the assembly. `COALESCE(flag, 0)` provides the default when the flag is absent.

- Pre-window / in-window slices that immediately feed a single `UNION ALL`.

- Don't materialize purely to rename columns — push the desired names into the upstream column-builder where possible (see §11.2).

- Don't materialize at clifpy boundaries by default — vendor the DuckDB equivalent if it's a hot path (see §11.1).

---

## 3. Replacement scans beat `.register()`

DuckDB automatically resolves Python variable names that refer to pandas
DataFrames, Polars DataFrames/LazyFrames, Arrow tables, NumPy arrays, or other
`DuckDBPyRelation`s — **as long as the variable is visible in the calling
scope** at the point where `duckdb.sql()` / `duckdb.execute()` is invoked.

```python
import duckdb, pandas as pd

cohort_df = pd.DataFrame({"hospitalization_id": ["A", "B"], "start_dttm": [...]})
# No .register() needed — replacement scan picks up cohort_df by name.
duckdb.sql("FROM cohort_df SELECT * LIMIT 5").show()
```

**When to use `duckdb.register("name", df)` instead:**

- The DataFrame is stored as a dict value, class attribute, or otherwise not a local variable at the call site.

- You need a stable view name across multiple connections.

- You want to reference the same data under a different name than the variable.

**To disable replacement scans (rarely needed):**

```sql
SET python_enable_replacements = false;
```

**Cite:** sofa2 contains zero `.register()` calls. Every `duckdb.sql(f"FROM
cohort_rel ...")` in `_utils.py` works via replacement scan.

---

## 4. Push predicates and projections to the source

DuckDB performs **filter pushdown** and **projection pushdown** automatically
for parquet readers — only the required columns are read, and parquet zonemaps
are used to skip row groups whose min/max bounds fall outside the WHERE
predicate.

clifpy's `load_data` wrapper exposes this directly:

```python
labs_rel = load_data(
    'labs',
    config_path=clif_config_path,
    return_rel=True,
    columns=['hospitalization_id', 'lab_category', 'lab_collect_dttm', 'lab_value_numeric'],
    filters={'lab_category': [
        'platelet_count', 'bilirubin_total', 'creatinine',
        'potassium', 'ph_arterial', 'ph_venous',
        'bicarbonate', 'po2_arterial',
    ]},
)
```

`columns=` is projection pushdown; `filters=` becomes a `WHERE lab_category IN
(...)` that the parquet reader can use to skip row groups.

**Other pushdown rules:**

- Apply `WHERE` before `GROUP BY`. The optimizer often does this for you, but writing it that way is clearer and removes ambiguity.

- Colocate temporal predicates **on the JOIN clause** when the join is range-based, not as a post-join `WHERE`.

```sql
-- Good: predicate is part of the join, optimizer can use it for sort-merge.
JOIN cohort_rel c
  ON  t.hospitalization_id = c.hospitalization_id
  AND t.lab_collect_dttm >= c.start_dttm
  AND t.lab_collect_dttm <= c.end_dttm

-- Worse: optimizer may materialize the cross-product first.
JOIN cohort_rel c ON t.hospitalization_id = c.hospitalization_id
WHERE t.lab_collect_dttm BETWEEN c.start_dttm AND c.end_dttm
```

**Cite:** `clifpy/utils/sofa2/_core.py:415-436` (load_data pushdown);
`_utils.py:259-262` (temporal predicate on JOIN).

---

## 5. Connection management

**Reuse one connection.** DuckDB caches schema metadata, prepared plans, and
buffer-pool pages. Disconnecting drops all of that and adds parse/plan overhead
to the next query.

sofa2 uses the implicit global default connection (`duckdb.sql(...)`,
`duckdb.execute(...)`). For library code that may be called inside a
caller-supplied connection, accept it as a parameter and fall back:

```python
def my_pipeline(cohort_df, con: duckdb.DuckDBPyConnection | None = None):
    con = con or duckdb.default_connection()
    con.execute("CREATE OR REPLACE TEMP TABLE _cohort AS SELECT * FROM cohort_df")
    ...
```

**When multiple connections help:** only when DuckDB is bottlenecked on
something other than CPU (e.g., remote-object-store latency). Within a single
query DuckDB already parallelizes across all configured threads, so concurrent
connections rarely speed up CPU-bound work.

---

## 6. Resource limits — set them with a context manager

The four configuration knobs that matter for a memory-bounded library
pipeline:

| Setting | Purpose |
|---|---|
| `memory_limit` | RAM cap for the buffer manager. Spills to disk above this. |
| `threads` | Parallel execution thread count. |
| `temp_directory` | Where spill files go. **Always set this** if your workload may exceed `memory_limit`. |
| `max_temp_directory_size` | Hard cap on spill-file disk usage. |

Set with `SET key = 'val'`, save the prior value via `current_setting('key')`,
restore on exit. This matters for library code: `SET memory_limit` would
otherwise leak into the caller's connection.

```python
from contextlib import contextmanager

@contextmanager
def with_duckdb_config(**settings):
    saved = {k: duckdb.sql(f"SELECT current_setting('{k}')").fetchone()[0]
             for k in settings}
    for k, v in settings.items():
        duckdb.execute(f"SET {k} = '{v}'")
    try:
        yield
    finally:
        for k, v in saved.items():
            duckdb.execute(f"SET {k} = '{v}'")
```

**Heuristics from the DuckDB docs:**

- **Aggregation-heavy** workloads: 1–2 GB memory per thread.

- **Join-heavy** workloads: 3–4 GB memory per thread.

- **Minimum:** 125 MB per thread (rule of thumb). 8 threads → 1 GB minimum.

- **Larger-than-memory:** out-of-core support exists for grouping, joining, sorting, windowing. Spills to `temp_directory` — if unset, you OOM instead of spilling.

**Cite:** `clifpy/utils/sofa2/_perf.py:83-134` (`_with_duckdb_config`).

---

## 7. SQL idioms that beat hand-rolled Python

Each of these is dense, optimizer-friendly, and replaces multiple lines of
Python that would force materialization.

### 7a. `ASOF LEFT JOIN` for "most recent prior measurement"

Use when you need the latest value at or before a reference time (e.g.,
forward-fill from before the scoring window).

```sql
FROM cohort_rel c
ASOF LEFT JOIN resp_rel t
  ON  c.hospitalization_id = t.hospitalization_id
  AND c.start_dttm > t.recorded_dttm
SELECT c.*, t.fio2_set, c.start_dttm - t.recorded_dttm AS time_gap
WHERE time_gap <= INTERVAL '6 hours'
```

**Cite:** `_resp.py:224-239` (pre-window FiO2 lookback); `_cv.py:122-147`
(pre-window pressor episode forward-fill).

### 7b. `QUALIFY ROW_NUMBER() OVER (...) = 1` for "best record per key"

Eliminates the CTE-then-WHERE pattern.

```sql
FROM mar_events
SELECT *
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY hospitalization_id, start_dttm, admin_dttm, med_category
    ORDER BY priority_score DESC
) = 1
```

**Cite:** `_utils.py:906-916` (MAR dedup).

### 7c. `LAG → CASE → cumulative SUM` for episode/run detection

The canonical "find runs of consecutive same-value rows" pattern over an
ordered partition.

```sql
WITH ordered AS (
    FROM resp_rel
    SELECT *,
        CASE WHEN device_category IS DISTINCT FROM
                  LAG(device_category) OVER (PARTITION BY hospitalization_id ORDER BY recorded_dttm)
             THEN 1 ELSE 0 END AS _device_change
)
FROM ordered
SELECT *,
    SUM(_device_change) OVER (
        PARTITION BY hospitalization_id ORDER BY recorded_dttm
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS _device_episode_id
```

**Cite:** `_resp.py:65-110` (device-episode detection); `_kidney.py:175-194`
(UO-rate forward-fill).

### 7d. `FIRST_VALUE` / `LAST_VALUE` for episode bounds

Once you have an episode id, capture its bounds in one pass:

```sql
SELECT *,
    FIRST_VALUE(admin_dttm) OVER episode_w AS episode_start,
    LAST_VALUE(admin_dttm)  OVER episode_w AS episode_end
FROM ...
WINDOW episode_w AS (PARTITION BY episode_id ORDER BY admin_dttm
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

**Cite:** `_utils.py:948-954`.

### 7e. `SEMI JOIN` / `ANTI JOIN` instead of `WHERE x IN (SELECT ...)`

`SEMI JOIN` cannot duplicate left-side rows even when the right side has
duplicate keys. `ANTI JOIN` is the negation.

**Cite:** `_core.py:480` (`SEMI JOIN cohort` for table scoping); `_resp.py:249-251`
(`ANTI JOIN windows_with_data` for fallback rows).

### 7f. `UNION ALL` (not `UNION`) when duplicates are impossible

`UNION` performs an implicit sort + dedup. `UNION ALL` does not. If you know
the inputs are disjoint (e.g., pre-window vs in-window slices), use
`UNION ALL`.

**Cite:** `_utils.py:511-525` (three disjoint slices unioned for delirium-drug
flag).

---

## 8. Temp-table lifecycle

Track every `CREATE TEMP TABLE` in a module-level registry; drop the registry
in a `finally` after the pipeline's terminal `.df()`. Without this, temp tables
linger on the connection until process exit and can collide with names used
by subsequent runs.

```python
_TEMP_TABLE_REGISTRY: list[str] = []

def _register_temp_table(name: str):
    if name not in _TEMP_TABLE_REGISTRY:
        _TEMP_TABLE_REGISTRY.append(name)

def _cleanup_temp_tables():
    while _TEMP_TABLE_REGISTRY:
        name = _TEMP_TABLE_REGISTRY.pop()
        try:
            duckdb.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass
```

Temp tables are session-scoped, live in the `temp.main` schema, and will spill
to `temp_directory` when memory is constrained. Their names can shadow regular
tables — sofa2 prefixes everything with `_sofa2_` or `_clif_` to avoid this.

**Cite:** `clifpy/utils/sofa2/_perf.py:22-50`.

---

## 9. Anti-patterns to avoid

- **Calling `.df()` mid-pipeline.** Breaks the lazy DAG. The next `duckdb.sql("FROM df ...")` re-imports via replacement scan with no statistics, and the optimizer can't see the predicates upstream.

- **Re-reading a parquet source N times.** If you reference it more than once, materialize once after `SEMI JOIN` to the cohort.

- **Using `UNION` when you mean `UNION ALL`.** Pays for sort+dedup needlessly.

- **Disconnecting and reconnecting per query.** Drops the metadata cache.

- **Forgetting to `SET temp_directory`.** Larger-than-memory workloads will OOM instead of spilling.

- **`duckdb.register()` for a variable already in local scope.** Replacement scans handle it; the registration is dead code.

- **Leaving `SET memory_limit` set after a function returns.** Always restore via `_with_duckdb_config`.

- **Building a pandas merge chain on top of DuckDB output.** Stay in SQL until the assembly is complete, then `.df()` once.

---

## 10. Diagnosing a slow workload

```python
# Print the physical plan. Note: do NOT print(duckdb.sql("EXPLAIN ...")) — it
# escapes \n into the output. Use .explain() instead.
print(rel.explain())
```

Other diagnostic moves:

- Watch `htop` (or your platform's equivalent) — confirm parallelism is engaging. If only one core is hot, your query is serial.

- `SET threads = 4` — bound thread count if you're memory-limited and the buffer manager is thrashing.

- For parquet sources: target row groups of 100k–1M rows and file sizes of 100 MB – 10 GB. Smaller row groups defeat zonemap-based skipping; larger ones inflate the in-memory decompression footprint.

- **Persistent (compressed) DB can be ~8× faster than uncompressed in-memory.** From the DuckDB docs (TPC-H Q1, SF30): in-memory uncompressed = 4.22 s, in-memory compressed = 0.55 s, persistent compressed = 0.56 s. To get a compressed in-memory DB:

  ```sql
  ATTACH ':memory:' AS db (COMPRESS);
  USE db;
  ```

- The slow-workload checklist from the DuckDB docs: enough memory? fast disk? no unnecessary indexes? correct types (`TIMESTAMP` not `VARCHAR`)? sane parquet row groups? `EXPLAIN` looks reasonable? parallelism engaging? thread count not too high?

---

## 11. Working with library boundaries that can't go fully lazy

The §1–10 patterns assume you control the full chain. In practice you'll hit
library boundaries — clifpy table objects, marimo cells, parquet writers — that
push back on "stay lazy by default." This section catalogs the recurring
patterns and when each workaround is the right call. Distilled from the
`CLIF-epi-of-sedation/code/02_exposure.py` refactor (April 2026).

### 11.1 Vendoring vs accepting the boundary

`clifpy.utils.outlier_handler.apply_outlier_handling(table_obj, ...)` requires
a `clifpy.Table` instance (with `.df` and `.table_name`) and round-trips
Polars↔pandas internally. This conflicts with the lazy-DAG rule when the
outlier step is mid-pipeline: you'd have to materialize at `from_file`, hand
the Table off, then re-relation downstream.

Two options:

- **Accept the materialization.** Use `from_file()` to get a Table,
  materialize at the boundary, document with a load-bearing comment naming
  the constraint. Cheaper to maintain (no schema-tracking burden) but pandas
  in the chain.

- **Vendor a DuckDB-native version locally.** Read the same YAML config,
  build `CASE WHEN` SQL, apply via `SELECT * REPLACE (case_expr AS col) FROM
  rel`. ~100 LOC for a YAML→SQL translator covering category-dependent,
  medication, and simple-range column shapes. Cite:
  `CLIF-epi-of-sedation/code/_outlier_handler.py` (full implementation
  including DEBUG-gated drop-count diagnostics).

Vendoring policy: schema-track upstream (mirror config-key changes when
clifpy adds new (table, column) pairs). Long-term, prefer upstreaming the
DuckDB version into clifpy so the whole consortium can drop their pandas
pinch.

### 11.2 Dynamic PIVOT_WIDER columns block SQL aliasing

When downstream column names depend on cohort-presence (e.g.,
`PIVOT_WIDER ON med_category_unit` produces a column per drug+unit combo),
post-pivot SQL aliases will error if a particular drug has no records — the
column simply doesn't exist. Solutions in priority order:

1. **Embed the post-aggregation unit in the pivot column-name builder.**
   Construct `med_category_unit` so the pivot directly emits the final
   names. No rename needed because the column-builder runs per row and
   adapts to whatever drugs are present:
   ```sql
   , med_category_unit:
       med_category || '_'
       || REPLACE(REPLACE(med_dose_unit, '/min', ''), '/', '_')
       || '_hr_cont'
   -- propofol + 'mcg/kg/min' → 'propofol_mcg_kg_hr_cont' directly
   ```
   Cite: `CLIF-epi-of-sedation/code/02_exposure.py` (cont_sed_w + intm_sed_w
   pivot cells).

2. **`pandas.rename()` after `.df()`.** Silent on missing keys, but forces
   a materialize-rename pass. Use only when (1) isn't tractable.

3. **Pre-compute the expected column list and conditionally alias.** More
   SQL, more code; rarely worth it.

### 11.3 `AT TIME ZONE site_tz` over session tz

`extract('hour' FROM tstz AT TIME ZONE 'America/Chicago')` returns local
hour invariant under DuckDB session tz. The session tz can be reset under
your feet by `clifpy.utils.io.load_parquet_with_tz` (which calls
`SET timezone = 'UTC';` on every parquet load), so any code relying on
session tz for correctness has a non-local failure mode. Use the explicit
form everywhere shift/clock semantics matter. Cite:
`CLIF-epi-of-sedation/code/_utils.py:add_day_shift_id` (pure-SQL implementation
with `AT TIME ZONE '{site_tz}'` interpolation).

### 11.4 Site-local-tz at the parquet-write boundary

pyarrow round-trips tz tags through parquet metadata, so writing a
tz-tagged column preserves the tag on disk for downstream readers. But:

- **pandas `dt.tz_convert`** is metadata-only and preserves the tag through
  `df.to_parquet()` via pyarrow.

- **Polars `dt.convert_time_zone`** is also metadata-only and preserves the
  tag through `df.write_parquet()` via Arrow.

- **DuckDB's parquet writer** (`rel.to_parquet(path)`) normalizes
  TIMESTAMPTZ columns to a UTC tag — losing any site-local tag you might
  want preserved on disk.

So **for tz-tagged outputs you need pandas or Polars at the write boundary**,
not DuckDB native. For tz-free outputs, `rel.to_parquet(path)` is fine.
Cite: `CLIF-epi-of-sedation/code/02_exposure.py` save cell — `sed_dose_daily`
and `sed_dose_agg` (no `*_dttm`) write via DuckDB native, while
`sed_dose_by_hr` (has `event_dttm`) routes through Polars
`pl.col('event_dttm').dt.convert_time_zone(SITE_TZ)` + `write_parquet(...)`
to preserve the tag.

### 11.5 Library tz-handling tradeoffs (when to use which)

| Need | Use | Why |
|---|---|---|
| Compute (extract local hour from UTC instant) | DuckDB `AT TIME ZONE` | ICU-backed, session-tz-independent, already in your SQL |
| Storage tag preservation at parquet write | pandas `dt.tz_convert` *or* Polars `dt.convert_time_zone` | Both metadata-only and tag-preserving via Arrow; DuckDB's parquet writer does NOT preserve tags |
| Site-local tz reflag of a UTC tz-aware column | `df[c].dt.tz_convert(site_tz)` (pandas) or `pl.col(c).dt.convert_time_zone(site_tz)` (polars) | Same UTC instants, different display tag — pure metadata op |

**Avoid**: relying on DuckDB session tz for correctness. clifpy's
`load_parquet_with_tz` resets it to UTC on every parquet load, and any
isolated-connection helper sees its own session-tz state.

### 11.6 Scalar diagnostics without breaking the lazy DAG

When you need a count or summary mid-pipeline, don't materialize the
relation. Run a separate `duckdb.sql("FROM rel SELECT COUNT(*) ...").fetchone()`
that touches the relation independently — `rel` continues downstream
untouched. Pair with `logger.isEnabledFor(logging.DEBUG)` gating per the
logging guide so the count query only runs when you'll actually consume the
output. Cite: `CLIF-epi-of-sedation/code/02_exposure.py` weight-attach cell
— a separate `_diag = duckdb.sql("FROM cont_sed_with_weight SELECT
COUNT(*) ...").fetchone()` query surfaces per-source counts while the
lazy `cont_sed_with_weight` relation feeds straight into the next cell.

### 11.7 `mo.sql` vs `duckdb.sql` in marimo — front-end rendering matters

Both yield the same value (DuckDBPyRelation in `sql_output="native"` mode,
pandas DataFrame in default mode), but they render differently in the
marimo notebook UI:

- **`mo.sql(f"""...""")`** — rendered as a first-class SQL cell with
  syntax highlighting. Use whenever the cell's value IS the SQL output
  (idiomatic + reactive + readable).

- **`duckdb.sql(...)`** — rendered as an opaque Python string. Use only
  for in-cell mutations (`obj.attr = duckdb.sql(...).df()`) or when you
  need the relation's lazy-chaining methods directly (`.filter`, `.project`,
  etc.).

Cite: `CLIF-epi-of-sedation/code/02_exposure.py` — every cell whose value
is the SQL output uses `mo.sql`; the few `duckdb.sql` calls are for
diagnostic scalar fetchall(s) that don't expose a relation as the cell's
value.

---

## Appendix A — Output methods, full reference

Lifted from the DuckDB Python API docs and trimmed to the methods sofa2
actually uses.

| Method | Returns | Notes |
|---|---|---|
| `arrow()` / `to_arrow_table()` / `fetch_arrow_table()` | `pyarrow.Table` | Zero-copy where possible. |
| `df()` / `to_df()` / `fetchdf()` | `pandas.DataFrame` | Most common terminal. |
| `pl()` | `polars.DataFrame` | Use when downstream code is in Polars. |
| `fetchall()` | `list[tuple]` | Small result sets only. |
| `fetchone()` / `fetchmany(n)` | tuple / list | Streaming-style consumers. |
| `to_table("name")` / `create("name")` | None | Persist as a (regular) table. |
| `to_view("name")` / `create_view("name")` | None | Persist as a view. |
| `to_parquet(path)` / `to_csv(path)` | None | Write directly to file. |
| `record_batch(batch_size)` / `fetch_record_batch()` | `pyarrow.RecordBatchReader` | Stream Arrow batches. |
| `explain()` | str | Plan inspection — print this, not `duckdb.sql("EXPLAIN ...")`. |

---

## Appendix B — sofa2 cite map

One-line index from rule to canonical example. Use this as a starting point
when the doc says "follow the sofa2 pattern" and you need to read live code.

| Rule | File | Lines | What's there |
|---|---|---|---|
| Lazy chaining | `_resp.py` | 174–221 | Predicate → join → window in one `duckdb.sql()`. |
| Lazy + LAG cumsum episode detection | `_resp.py` | 65–110 | Device-episode id construction. |
| Lazy + ASOF lookback | `_resp.py` | 224–239 | Pre-window FiO2 lookup. |
| Lazy + ANTI JOIN fallback | `_resp.py` | 249–251 | Pre-window only when in-window absent. |
| Cohort materialization (boundary 2a) | `_core.py` | 389–395 | `CREATE TEMP TABLE _sofa2_cohort`. |
| Predicate + projection pushdown | `_core.py` | 415–436 | `load_data(columns=, filters=)`. |
| SEMI JOIN reuse materialization (boundary 2b) | `_core.py` | 475–494 | `_clif_*` per-source tables. |
| Subscore materialization (boundary 2c) | `_perf.py` | 53–75 | `_materialize_subscore`. |
| Temp-table registry | `_perf.py` | 22–50 | `_register_temp_table` / `_cleanup_temp_tables`. |
| Resource-limit context manager | `_perf.py` | 83–134 | `_with_duckdb_config`. |
| ASOF for episode forward-fill | `_cv.py` | 122–147 | Pre-window pressor lookup. |
| UNION ALL → CREATE TEMP TABLE | `_cv.py` | 194–200 | Raw pressor-event union. |
| LAG forward-fill chain | `_kidney.py` | 175–194 | UO-rate per-patient forward-fill. |
| Flag pattern (`_flag_*`) | `_utils.py` | 369–583 | DISTINCT key + value=1 lazy rels. |
| QUALIFY ROW_NUMBER dedup | `_utils.py` | 906–916 | MAR record dedup. |
| FIRST_VALUE / LAST_VALUE episode bounds | `_utils.py` | 948–954 | Episode start/end capture. |
| UNION ALL of disjoint slices | `_utils.py` | 511–525 | Pre/in/intm-window flag union. |
| Temporal predicate on JOIN | `_utils.py` | 259–262 | Range JOIN for in-window labs. |

---

## Further reading

- DuckDB Python relational API: `https://duckdb.org/docs/lts/clients/python/relational_api`

- DuckDB performance guide: `https://duckdb.org/docs/lts/guides/performance/my_workload_is_slow`

- DuckDB workload tuning: `https://duckdb.org/docs/lts/guides/performance/how_to_tune_workloads`

- AsOf joins blog: `https://duckdb.org/2023/09/15/asof-joins-fuzzy-temporal-lookups`

- Querying Parquet with precision: `https://duckdb.org/2021/06/25/querying-parquet`
