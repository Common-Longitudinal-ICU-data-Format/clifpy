# Timezone Diagnosis: issue #144 (`admin_dttm` mislabeled after dose conversion)

> Diagnosis reference for the `clifpy` timezone bug tracked as issue #144. All line numbers are on
> the **dev branch** (`merge-sofa2-medconverter-misc-fixes-jul2026`) and were verified against the
> tree at the time of writing — re-check if the files have since moved. This document diagnoses the
> bug and traces its data flow; the fix is summarized in the last section.

## 1. TL;DR

`ClifOrchestrator(timezone=X)` → dose-unit conversion returns `df_converted['admin_dttm']` in the
**machine / DuckDB default-connection zone** instead of the configured `X`.

- **Root cause (one sentence):** the load path and the converter use *different DuckDB connections
  with different `TimeZone` settings* — the load normalizes timestamps to the target zone (on an
  isolated connection it then closes, or in pandas), but the converter re-renders the `TIMESTAMPTZ`
  through the **unpinned default connection**, whose `TimeZone` silently defaults to the machine's OS
  zone.

- **Label-only:** the absolute (UTC) instant is *always* preserved. Only the timezone **label** is
  wrong.

- **Conditional visibility:** the bug is visible **iff** the target zone ≠ the default-connection
  zone. On a developer's own machine, with `timezone` set to that machine's zone, it silently
  *disappears* — which is why it evaded notice.

## 2. The DuckDB mechanism (the crux)

Two DuckDB facts fully explain the bug:

- **Fact A — rendering is connection-relative.** A `TIMESTAMP WITH TIME ZONE` value is stored as a UTC
  instant. When DuckDB materializes it to pandas (`.to_df()` / `.fetchdf()`), it attaches the tz
  **label** of the *rendering connection's* `TimeZone` setting. Same instant, different connection
  zone → different pandas `.dt.tz` label.

- **Fact B — `timezone()` strips the zone.** `timezone('X', ts_tz)` applied to a `TIMESTAMPTZ` returns
  a **naive** `TIMESTAMP` (the wall-clock in zone X, no tzinfo). This is the operator the dev eager
  path uses, which is why dev eager loads come back tz-*naive*.

The demo confirms the storage side — every `*_dttm` in the packaged demo parquet is stored as UTC
`TIMESTAMPTZ`:

```
column_name              column_type
 admin_dttm  TIMESTAMP WITH TIME ZONE
```

## 3. The two code paths, side by side (`clifpy/utils/io.py`)

### 3a. Load — correct, uses isolated-connection discipline

The **lazy** load reads on a fresh isolated connection pinned to UTC, then applies the site timezone
in pandas — never touching the process-wide default connection:

```python
# load_parquet_with_tz, lazy branch — io.py:336-338
if lazy:
    con = duckdb.connect()
    con.execute("SET timezone = 'UTC';")          # read & return in UTC
```

```python
# fetch_lazy_result — io.py:161-162
if site_tz:
    df = convert_datetime_columns_to_site_tz(df, site_tz, verbose)
```

`convert_datetime_columns_to_site_tz` is the tz-**aware** pandas relabeler — `tz_convert` for already
aware input, `tz_localize` for naive input:

```python
# convert_datetime_columns_to_site_tz — io.py:699 (aware) / io.py:706 (naive)
df[col] = df[col].dt.tz_convert(site_tz)
...
df[col] = df[col].dt.tz_localize(site_tz, ambiguous=True, nonexistent='shift_forward')
```

This is the behavior `main` exposes for *all* loads and is the contract existing users depend on
(`df['x_dttm'].dt.tz_convert(...)` only works on an aware column).

### 3b. Load — dev eager regression: tz-naive via `timezone()`

The dev **eager** path instead builds a SELECT clause that wraps each dttm column in `timezone(...)`,
producing **naive** wall-clock, and runs it on the **default** connection:

```python
# _build_tz_converted_select — io.py:214-216
if 'dttm' in col.lower() and site_tz:
    # Convert UTC to site timezone for datetime columns
    select_parts.append(f"timezone('{site_tz}', {col}) AS {col}")   # -> NAIVE
```

```python
# load_parquet_with_tz, eager/return_rel branch — io.py:364-391
duckdb.execute("SET timezone = 'UTC';")          # read & return in UTC  (DEFAULT connection)
...
if site_tz:
    sel = _build_tz_converted_select(duckdb.default_connection(), file_path, columns, site_tz, source_type="parquet")
...
df = duckdb.sql(query).df()              # pandas DataFrame  (io.py:391)
```

The CSV eager path is the same shape: `duckdb.execute("SET timezone = 'UTC';")` (io.py:599),
`_build_tz_converted_select(..., source_type="csv")` (io.py:604), `duckdb.sql(query).df()`
(io.py:626).

Net: dev eager `load_data(site_tz=X)` returns **tz-naive** columns, while the lazy path (§3a) stays
**tz-aware** — an internal eager/lazy inconsistency, and a regression against `main`'s aware contract.

### 3c. Convert — the bug site (`clifpy/utils/unit_converter.py`)

The converter registers the input DataFrame into a temp table **on the default connection**, does all
its work there, and renders back with `.to_df()`:

```python
# convert_dose_units_by_med_category — unit_converter.py:1345
duckdb.execute("CREATE OR REPLACE TEMP TABLE _med_unit_input AS SELECT * FROM med_df")
```

```python
# final renders — unit_converter.py:1517 and :1548
return med_df_converted.to_df(), convert_counts_df.to_df()      # show_intermediate=True
...
return result_rel.to_df(), convert_counts_df.to_df()           # default
```

An **aware** `admin_dttm` registers as `TIMESTAMPTZ` (UTC instant preserved), but the `.to_df()` label
is whatever the **default connection's** `TimeZone` happens to be — which nothing pinned to the target
zone. That is the entire bug (Fact A). For `return_rel=True` the render is *deferred* to the caller's
later `.df()` on the returned relation (unit_converter.py:1516, 1547), so the mislabel would surface
then instead.

## 4. End-to-end #144 flow (orchestrator)

Setup: `ClifOrchestrator(timezone='UTC')` on a non-UTC machine (or with a hostile default-connection
zone), then a continuous-med dose conversion.

1. `__init__` stores the configured zone — `self.timezone = config['timezone']` (clif_orchestrator.py:176).

2. `convert_dose_units_for_continuous_meds` (clif_orchestrator.py:1038) loads the med table via
   `load_data(site_tz=self.timezone)`. On `main` this yields **aware** `admin_dttm` in `self.timezone`;
   `self.medication_admin_continuous.df['admin_dttm']` is correct here.

3. It calls the converter (call site clif_orchestrator.py:1111) passing that DataFrame.

4. The converter round-trips through the default connection and `.to_df()`s (§3c) → `admin_dttm` is
   re-labeled to the **default-connection zone**, not `self.timezone`.

5. The result is stored as `self.medication_admin_continuous.df_converted = converted_df`
   (clif_orchestrator.py:1126).

Outcome: `df['admin_dttm']` (aware, target zone) ≠ `df_converted['admin_dttm']` (aware, wrong zone) —
same instants, mismatched labels. That mismatch is issue #144.

## 5. Why the dev guard tests currently pass (and why the fixes are coupled)

The dev regression-guard tests
(`tests/utils/med_unit_converter/test_unit_converter.py::test_converter_preserves_admin_dttm_tz` and
`tests/core/test_clif_orchestrator.py`) currently pass on dev — but only *accidentally*:

- dev eager load is **naive** (§3b). A naive pandas datetime registers into DuckDB as a plain
  `TIMESTAMP` (no zone), so `.to_df()` renders it back **unchanged** — there is no zone label to get
  wrong (Fact A doesn't fire on a naive column).

- The moment aware eager is restored on the materialized load path (§3a behavior), the converter's
  input becomes `TIMESTAMPTZ` again and the mislabel re-appears.

**Therefore the two fixes are coupled:** restoring aware eager *without* fixing the converter would
re-expose #144. Both must land together.

## 6. Scenario matrix (machine × target × storage)

Three independent axes: **storage zone** (parquet encoding), **target zone**
(`ClifOrchestrator(timezone=)`), **machine zone** (OS / default-connection render zone). Universal
expectation: every `*_dttm` output (loaded *and* converted) is in the **target** zone with the correct
absolute instant, regardless of machine and storage.

### Matrix A — CLIF-compliant storage = UTC `TIMESTAMPTZ` (the norm)

Load reads the UTC instant and converts to target correctly; the pre-fix converter mis-renders to the
machine zone. Instant always preserved; label wrong iff **target ≠ machine**:

| # | Target | Machine | Loaded `df` | Converter (pre-fix) | Correct? | #144 visible |
|---|--------|---------|-------------|---------------------|----------|--------------|
| A1 | UTC | UTC | aware UTC | aware UTC | aware UTC | hidden (match) |
| A2 | UTC | CST | aware UTC | aware CST | aware UTC | **yes — 5–6h (the report)** |
| A3 | US/Eastern | CST | aware EST | aware CST | aware EST | **yes — 1h (subtle)** |
| A4 | US/Central | CST | aware CST | aware CST | aware CST | hidden (match) |
| A5 | US/Central | UTC | aware CST | aware UTC | aware CST | **yes** |

The subtle A3 case (a 1-hour EST-vs-CST error) is why regression tests must force target ≠ machine and
parametrize multiple target zones, rather than trusting a single developer machine.

### Matrix B — non-compliant storage = naive local wall-clock (a minority of sites)

Deeper problem: the *load itself* can mislabel the instant, because
`convert_datetime_columns_to_site_tz` `tz_localize`s a naive column **to the target** (io.py:706),
assuming naive == target. clifpy has no separate storage-zone input:

| # | Storage (actual) | Target | Loaded `df` | Correct? |
|---|------------------|--------|-------------|----------|
| B1 | naive EST | US/Eastern | localize → aware EST | ✅ (target == storage) |
| B2 | naive EST | UTC | localize → aware UTC | ❌ instant wrong (off 5h) |
| B3 | naive EST | US/Central | localize → aware CST | ❌ instant wrong (off 1h) |

For naive storage the converter mis-render (Matrix A) stacks on top of B. Mitigation: non-compliant
sites must set `timezone` = their actual storage zone; there is no storage-zone override. Matrix B is a
**load-path** concern, distinct from the converter bug this document primarily traces.

## 7. Prevention rules

- **Never** materialize a `TIMESTAMPTZ` to pandas (`.to_df()` / `.fetchdf()`) on the **default**
  connection without first pinning that connection's `TimeZone` to the intended zone — or carry the
  data tz-naive/UTC and attach the zone in pandas. Do not rely on the ambient default-connection zone.

- Prefer **explicit connection discipline** (a connection with a known `SET timezone`) over bare
  `duckdb.sql()` / `duckdb.execute()` for any query that returns timestamps. The load path already
  does this (§3a); the converter did not (§3c) — that is the gap.

- When writing tests for tz behavior, force **target ≠ machine** (e.g. pin the default connection to a
  hostile zone) and parametrize several target zones, so the subtle 1-hour case (A3) can't hide behind
  a matching developer-machine zone.

## 8. Provenance (why the seam exists)

`git blame` on the dev branch traces the two sides of the seam to *different, chronologically
separated* commits (all by the repo owner, so this is a layer/convention gap, not a cross-author
miscommunication):

- **Load-path tz discipline** — the isolated-connection + `SET timezone='UTC'` + pandas-relabel
  handling landed in the Jan 2026 `feat(tz)` commits (`0ce7b346`, `fbe6dcc4`, "extend/enhance
  timezone handling in data loading"), with the isolated-con UTC read at io.py:337-338 later reconciled
  via a merge from `origin/main` (`83f9bfc9`, 2026-04-27).

- **Converter default-connection render** — the temp-table registration + `.to_df()` render
  (unit_converter.py:1345 ff.) landed later, in the Apr 2026 converter upgrade (`ef1ed9fa`, "upgrade
  med unit converter to handle missing patient weight_kg…").

The converter's render point postdates and is disconnected from the load path's connection discipline:
the convention "pin the connection zone before rendering timestamps" existed in the loader but was not
carried into the newer converter code. The fix is to extend that discipline into the converter.

## 9. The fix + tests

**Chosen direction:** restore aware output on the **materialized** load path (match `main`, zero
regression for existing users) and fix the converter render. This ships as **Batch A now**; full
`return_rel`/SOFA2 aware-ness is deferred to **Batch B** (§10 gives the exact aware-vs-naive scope and
why the split is forward-compatible).

**Batch A — what actually changes:**

- **Restore aware materialized eager** (`io.py`): in `load_parquet_with_tz` (io.py:363-393) and the CSV
  branch of `load_data` (io.py:596-626), the materialized (`return_rel=False`) branch selects raw
  columns, `.df()`s (→ tz-aware UTC, default connection already at UTC), then relabels with
  `convert_datetime_columns_to_site_tz` (io.py:648) → tz-aware `site_tz`, mirroring the lazy path (§3a)
  and `main`. The `sel` guard becomes `if return_rel and site_tz:` so the naive `_build_tz_converted_
  select` is used *only* for `return_rel`. No isolated connection and no default-connection zone change
  are needed — the relabel is pure pandas. `site_tz=None` → tz-aware UTC. The `return_rel=True` branch
  is left **byte-identical** (naive) — see §10.

- **Fix the converter render** (`unit_converter.py`): capture the input's tz-aware column zones before
  the temp-table rebind (unit_converter.py:1345), and on the materialized path relabel the output
  columns back to those exact zones after `.to_df()` (unit_converter.py:1517/1548) — pure pandas,
  instant-preserving, zero global state. (`return_rel=True` callers own their connection's zone; the
  deferred-render pin only matters once return_rel loads go aware in Batch B.)

- **Keep `_build_tz_converted_select`** (io.py:170) — still used by the `return_rel` path and covered by
  `test_tz.py::TestBuildTzConvertedSelect`.

**Regression guards (already landed):**

- `tests/utils/med_unit_converter/test_unit_converter.py::test_converter_preserves_admin_dttm_tz` —
  bare-converter, parametrized over target zones, under a hostile default-connection zone.

- `tests/core/test_clif_orchestrator.py::test_orchestrator_{continuous,intermittent}_conversion_respects_timezone`
  — end-to-end #144 repro through `ClifOrchestrator`.

- A `main`-anchored load contract suite (`tests/utils/test_tz_contract.py`, in the `pyCLIF-tztest`
  worktree) distinguishes aware (`main`) from naive (dev eager) and is the oracle for the aware-eager
  restoration.

## 10. Fix scope: aware vs naive per path, and why the tests all pass under Batch A

The user's sharp question: *are we making it aware or naive, and if Batch A is only a partial fix, why
do **all** timezone tests pass?* This section answers both.

### What each load path returns after Batch A

| Load path | Returns | After Batch A | Changed by A? |
|---|---|---|---|
| Eager materialized `load_data(site_tz=X)` | `DataFrame` | **tz-AWARE** in X | ✅ restored (matches `main`) |
| `load_data(..., return_rel=True)` | `DuckDBPyRelation` | **tz-NAIVE** wall-clock in X | ❌ unchanged |
| `load_data(..., lazy=True)` + `fetch_lazy_result(site_tz=X)` | `DataFrame` | **tz-AWARE** in X | ❌ already aware |

So the answer is: **aware on the DataFrame (materialized) path; naive on the DuckDB-relation
(`return_rel`) path.** This is a deliberate, documented split. `return_rel` feeds SOFA2, whose fallback
`EMPTY_*` sentinels declare dttm columns as naive `NULL::TIMESTAMP` (`_core.py:59/98/134/170/210`,
`_kidney.py:514`) and whose `INTERVAL`/comparison arithmetic (`_kidney.py`) is built on naive
timestamps. Flipping `return_rel` to aware would mismatch those sentinels and change DST-boundary
interval semantics → it is deferred to **Batch B** together with the SOFA2 updates + clinical
re-validation. The #144 flow is entirely on the materialized path, so Batch A closes #144 without
touching `return_rel`.

### Why every timezone test passes anyway (the non-obvious part)

It looks paradoxical that a *partial* aware-restoration turns **all** tz tests green. The reason:
**no existing test asserts the one property Batch A leaves unchanged** (`return_rel` = naive). Path by
path:

- **The aware-guard (`test_tz_contract.py`) only probes the path we ARE fixing.** Its docstring states
  the `return_rel` path "is intentionally not used"; it tests only materialized loads. Batch A makes
  those aware → its 6 previously-failing eager assertions (`dt.tz is not None`, zone == X, `tz_convert`
  round-trips) flip fail→pass. It never inspects `return_rel`, so the still-naive relation path is
  invisible to it. **These 6 flips are the actual proof the fix landed.**

- **The converter/orchestrator guards run entirely through the materialized path.** Both the
  bare-converter and orchestrator tests load the med table as a *materialized* DataFrame (aware after
  A); the converter relabel (§9) preserves the zone. `return_rel` never enters these tests.

- **`test_tz.py` never asserts the naive/aware distinction for materialized `site_tz=X`.** Its only
  tz-awareness assertions (`.dt.tz is not None`, lines 70/185/233) are on the `site_tz=None` UTC case,
  which is aware on dev *already*. Its `site_tz=X` materialized tests check only hour-offsets and column
  presence — satisfied by both naive and aware. Its `return_rel` tests check only hour-offsets —
  satisfied by naive (which A preserves). So all 19 pass under A **and** would have passed before A;
  `test_tz.py` is simply blind to the property #144 is about.

**Honest takeaway:** "all tz tests pass" means *no test contradicts Batch A's partial scope* — **not**
that everything is now aware. The only genuine fail→pass flips are the 6 materialized-eager assertions
in `test_tz_contract.py`. Batch B is precisely the batch that would *add* tests asserting `return_rel`
is aware (rewriting `test_tz.py`'s return_rel cases) — assertions that would fail under A today, which
is exactly why B is scoped as a separate batch.

## 11. Far-future dates & the pytz DST freeze — why pandas is the *correct* decoder

A subtle correctness issue surfaced during Batch A verification. It affects timezone conversion of
CLIF-MIMIC (and any de-identified far-future) data, and it is *why the materialized load path must keep
using pandas/pytz* rather than "upgrading" to a rule-based engine.

### The pytz 2037 cliff

- pandas resolves string zones like `'US/Eastern'` via **pytz**, which carries a **finite** precomputed
  DST-transition table that ends at **2037**. Past the last transition it applies the final offset
  (**EST, −5**) *permanently* — it does not project the recurring rule.

- `zoneinfo` (Python) and DuckDB's **ICU** are **rule-based**: they project the recurring rule
  ("EDT 2nd-Sun-March → 1st-Sun-Nov") indefinitely.

- So for a **summer date past 2037**, pandas/pytz returns EST(−5) while zoneinfo/ICU return EDT(−4) — a
  1-hour disagreement. Verified for `2180-07-15 17:00 UTC → US/Eastern`: pandas `12:00 EST`, zoneinfo
  `13:00 EDT`, DuckDB `13:00`. (For `2030` dates all three agree — the split is strictly post-2037.)

### Why this is NOT a bug to "fix"

- CLIF-MIMIC de-identifies by shifting dates ~150 years into the future (2100s–2200s), so **every** real
  timestamp lands past the pytz cliff — this is not demo-only.

- For synthetic/de-identified dates there is **no civil-correct answer** ("what a New York wall clock
  would read" is meaningless for a fabricated date). The only meaningful quantity is the **intended
  clinical local time-of-day**, which you recover by *inverting the ETL's own encoding*.

- CLIF-MIMIC's ETL encoded `naive-Eastern → UTC` using **pandas/pytz**, which bakes the frozen-EST
  offset into the stored UTC. To recover the intended local time-of-day you must **decode with the same
  engine (pytz)** — the two frozen-EST applications cancel:

```
intended 13:00 ET  --[pytz encode]-->  18:00 UTC  --[pytz decode]-->   13:00  ✅ recovered
                                                   --[zoneinfo decode]--> 14:00  ❌ 1h off
```

- Therefore **pandas `convert_datetime_columns_to_site_tz` (§3a, §9) is the *correct* decoder** — it is
  the inverse of the encoder. Switching to zoneinfo/ICU (superficially "more correct") would silently
  shift every DST-season MIMIC timestamp by 1 hour, **corrupting** time-of-day analyses. Two consistent
  "wrongs" make a right.

### Robustness across populations

- **Present-day, non-MIMIC data:** pytz and zoneinfo agree (pre-2037) → pandas is exactly correct.

- **Far-future data:** only arises from MIMIC-style de-id, which is pytz-encoded → pandas round-trips
  correctly. clifpy cannot know a per-site encoder anyway; pandas/pytz is correct for both populations
  and matches `main`.

### What is / isn't affected (and the testing rule it implies)

- The **instant** is always preserved — pandas returns the correct UTC instant with an EST *label*;
  `.dt.tz_convert('UTC')` round-trips exactly.

- **Durations, rates, SOFA windows, dose-rate math** — unaffected (offsets cancel).

- **Absolute time-of-day** (`.dt.hour`, circadian/day-vs-night) — recovered correctly *only* by decoding
  with the encoder (pytz); this is the concrete reason we keep pandas.

- **Testing rule:** tz tests must assert **instant-preservation** + tz-awareness, **not** hardcoded
  wall-clock hours (which are engine- and date-dependent). `test_tz.py::test_loads_with_timezone_
  conversion` was re-anchored to this contract (its old hardcoded-hour assertion happened to encode the
  zoneinfo/ICU value and so mis-fired against the correct pytz result).
