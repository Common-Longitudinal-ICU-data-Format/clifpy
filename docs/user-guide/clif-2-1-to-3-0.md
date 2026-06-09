# CLIF 2.1 → 3.0 Migration

`clifpy` ships schemas for both **CLIF 2.1** (the current default) and **CLIF 3.0**. The
most pervasive change in 3.0 is that the standardized column values — everything in
`*_category`, `*_group`, and `*_type` columns — were **lowercased and `snake_cased`**,
and a handful were **renamed**. This guide shows how to migrate a site's CLIF 2.1 data to
the 3.0 value conventions.

## What changed

The change applies to **every** value in **every** standardized column
(`*_category` / `*_group` / `*_type`) across the 16 beta tables — that's hundreds of values
(e.g. dozens of `lab_category` values, 500+ microbiology organisms, the medication
vocabularies). The table below is just **a few representative examples** to show the kinds of
transformations, not the full list:

| CLIF 2.1 value | CLIF 3.0 value | kind |
|---|---|---|
| `IMV` | `imv` | lowercase |
| `Non-Hispanic` | `non_hispanic` | lowercase + `snake_case` |
| `l&d` | `l_and_d` | `&` → `_and_` |
| `DNR/DNI` | `dnr_or_dni` | `/` → `_or_` |
| `High Flow NC` | `hfnc` | curated rename (abbreviation) |
| `Assist Control-Volume Control` | `acvc` | curated rename (abbreviation) |
| `Long Term Care Hospital (LTACH)` | `ltach` | curated rename |
| `Psychiatric Hospital` | `mental_health_hosp` | curated rename (semantic) |

Two mechanisms cover everything, and both run automatically — you don't configure either:

1. **An automatic rule for most values** — lowercase and `snake_case` (with `&` → `_and_` and
   `/` → `_or_`). For example `IMV` → `imv`, `Non-Hispanic` → `non_hispanic`, `l&d` → `l_and_d`.
2. **Built-in renames for the rest** — a small set of values were shortened to abbreviations or
   renamed outright (e.g. `High Flow NC` → `hfnc`, `Psychiatric Hospital` → `mental_health_hosp`),
   so no rule could guess them. clifpy ships the official mapping for these and applies it for you.

!!! info "You don't need to enumerate the changes yourself"
    The crosswalk applies both mechanisms to **every** value automatically, and **flags
    anything it can't resolve** in the report. To see the authoritative, complete 3.0 value
    list for any column, load its 3.0 schema (see [Inspecting a schema version](#inspecting-validating-against-a-schema-version))
    — the permissible values there are the source of truth.

!!! note "Scope of the migration helpers"
    The crosswalk covers the 16 CLIF **beta tables** (`clifpy.BETA_TABLES`) and transforms
    **values only** — column header names are left unchanged. It is **non-mutating**: it
    returns a converted *copy* plus a change report, and never edits your input in place.

## Migrate a table in memory

Use `crosswalk_table_2_1_to_3_0(df, table_name)`. It accepts a **pandas** *or* an eager
**polars** `DataFrame` and returns `(converted_df, report)` — where `converted_df` is the
**same type you passed in** (pandas in → pandas out, polars in → polars out).

```python
from clifpy.data import load_demo_clif
from clifpy import crosswalk_table_2_1_to_3_0

# The bundled demo data is in CLIF 2.1 format.
co = load_demo_clif(tables=["respiratory_support"])

converted, report = crosswalk_table_2_1_to_3_0(
    co.respiratory_support.df, "respiratory_support"
)

# e.g. 'IMV' -> 'imv', 'High Flow NC' -> 'hfnc'
print(sorted(converted["device_category"].dropna().unique()))
print("is_complete:", report["is_complete"])
```

The function expects a **DataFrame** (pandas or polars), not a table object — if you have a
loaded table, pass its `.df` (as above). Already have a plain DataFrame? Pass it directly
(a polars `DataFrame` works the same way and comes back as polars; for a polars `LazyFrame`
or larger-than-memory data, use [`crosswalk_file_2_1_to_3_0`](#large-out-of-core-files)):

```python
import pandas as pd

df = pd.DataFrame({"device_category": ["IMV", "High Flow NC", "Nasal Cannula"]})
converted, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
print(list(converted["device_category"]))   # ['imv', 'hfnc', 'nasal_cannula']
```

## Read the change report

Every call returns a structured report so you can audit exactly what happened:

```python
df = pd.DataFrame({"discharge_category": [
    "Home", "Long Term Care Hospital (LTACH)", "Psychiatric Hospital",
]})
converted, report = crosswalk_table_2_1_to_3_0(df, "hospitalization")

print(list(converted["discharge_category"]))   # ['home', 'ltach', 'mental_health_hosp']

col = report["columns"]["discharge_category"]
print("values converted:", col["n_values_converted"])
print("ambiguous:", col["ambiguous"])     # left-unchanged 1->many values
print("unresolved:", col["unresolved"])   # produced a value not in the 3.0 list
print("overall complete:", report["is_complete"])
```

Report shape:

- `columns[<col>].n_values_converted` — count of values that changed.
- `columns[<col>].ambiguous` — values **left unchanged** because they can't be auto-mapped
  (see below), with the candidate 3.0 values and the reason.
- `columns[<col>].unresolved` — values whose produced result isn't a valid 3.0 value.
- `is_complete` — `True` only when nothing was flagged anywhere.

## Values that aren't auto-converted

The crosswalk never guesses. Two kinds of values are **left unchanged** and surfaced in the
report instead:

1. **1→many splits** — e.g. CLIF 2.1 `albumin` maps to either `albumin_5` or `albumin_25`
   in 3.0 depending on concentration, which can't be inferred from the category alone.
2. **Values removed in 3.0** — e.g. the `csf` lab specimen has no 3.0 equivalent.

```python
med = pd.DataFrame({"med_category": ["albumin", "norepinephrine"]})
converted, report = crosswalk_table_2_1_to_3_0(med, "medication_admin_continuous")

print(list(converted["med_category"]))   # 'albumin' is left unchanged
print(report["columns"]["med_category"]["ambiguous"])
```

Review these entries and resolve them with your own domain knowledge (for `albumin`, choose
`albumin_5`/`albumin_25` based on the product concentration).

## Large / out-of-core files

For tables too large to hold in memory, use `crosswalk_file_2_1_to_3_0`, which reads a file,
converts it, and writes the result — without loading the whole table into Python.

<!-- skip: next -->
```python
from clifpy import crosswalk_file_2_1_to_3_0

# DuckDB backend (default): streams parquet/CSV -> parquet/CSV, fully out-of-core.
report = crosswalk_file_2_1_to_3_0(
    "respiratory_support.parquet",        # CLIF 2.1 input
    "respiratory_support_3_0.parquet",    # converted output
    "respiratory_support",
)

# Or the chunked pandas backend (bounded memory, no DuckDB):
report = crosswalk_file_2_1_to_3_0(
    "respiratory_support.csv", "respiratory_support_3_0.csv",
    "respiratory_support", backend="pandas", chunk_size=1_000_000,
)
```

| `backend` | How it works | Memory | Best for |
|---|---|---|---|
| `"duckdb"` (default) | Streams the transform in SQL (parquet/CSV → parquet/CSV) | minimal (runs in DuckDB's engine) | very large / **bigger-than-RAM** files |
| `"pandas"` | Reads the file in `chunk_size` row batches | bounded by `chunk_size` | environments without DuckDB |

!!! tip "Which backend?"
    The DuckDB backend is recommended for large data — it's genuinely out-of-core (a 20M-row
    table converts in well under a second) and avoids dependencies that can be fragile on some
    platforms. It reuses the DuckDB dependency clifpy already installs. Both backends produce
    identical output and the same report.

## Convert a single value

`normalize_category_value` is the deterministic transform the crosswalk applies to any value
that isn't a curated rename. It's handy for one-off normalization or for understanding the rules:

```python
from clifpy import normalize_category_value

for v in ["IMV", "Non-Hispanic", "l&d", "DNR/DNI", "Mobility/Activity"]:
    print(f"{v!r:22} -> {normalize_category_value(v)!r}")
```

which prints:

```text
'IMV'                  -> 'imv'
'Non-Hispanic'         -> 'non_hispanic'
'l&d'                  -> 'l_and_d'
'DNR/DNI'              -> 'dnr_or_dni'
'Mobility/Activity'    -> 'mobility_or_activity'
```

## Inspecting / validating against a schema version

clifpy can load either version's schema. To see the 3.0 permissible values for a column:

```python
from clifpy.schemas import load_schema

schema = load_schema("respiratory_support", "3.0")
device = next(c for c in schema["columns"] if c["name"] == "device_category")
print(device["permissible_values"])
```

### Looking up permissible values without code

Prefer to browse the controlled vocabularies directly instead of loading a schema?

- **CLIF 3.0 mCIDE** (authoritative for permissible values) — one CSV per category column, on
  the repository's `3.0` branch. clifpy's 3.0 schemas are built directly from these, so they
  match `load_schema(..., "3.0")`:
  [github.com/.../CLIF/tree/3.0/mCIDE](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF/tree/3.0/mCIDE).
- **CLIF 3.0 data dictionary** — human-readable table and column definitions for 3.0 (marked
  "Concept"): [clif-icu.com/data-dictionary/data-dictionary-3.0.0](https://clif-icu.com/data-dictionary/data-dictionary-3.0.0).

### Validating your converted data against CLIF 3.0

`.validate()` is a method on a **table object**, not on a DataFrame — so to validate the
DataFrame the crosswalk returns, wrap it in a table object created at `clif_version="3.0"`:

```python
from clifpy import crosswalk_table_2_1_to_3_0, RespiratorySupport
from clifpy.data import load_demo_clif

rs_2_1 = load_demo_clif(tables=["respiratory_support"]).respiratory_support.df
rs_3_0, _ = crosswalk_table_2_1_to_3_0(rs_2_1, "respiratory_support")

# Wrap the converted DataFrame in a 3.0 table object, then validate
rs = RespiratorySupport(data=rs_3_0, timezone="US/Eastern", clif_version="3.0")
rs.validate()
print("valid:", rs.isvalid(), "| issues:", len(rs.errors))
```

`validate()` checks the data against the **3.0** schema — required columns, data types, and that
categorical values fall within the 3.0 permissible lists, plus completeness/plausibility — stores
findings in `rs.errors`, and returns `None` (read `rs.errors` / `rs.isvalid()`). Because it
validates against 3.0, raw 2.1 values (e.g. `device_category = "IMV"`) would be flagged — which
is the point of converting first.

Prefer not to build a table object? Validate the DataFrame directly against a version's schema:

```python
from clifpy.utils import validator
from clifpy.schemas import load_schema

errors = validator.validate_dataframe(rs_3_0, load_schema("respiratory_support", "3.0"))
print(len(errors), "issue(s)")
```

And if your data is already on disk in 3.0 form, load it at 3.0 directly with
`<TableClass>.from_file(..., clif_version="3.0")` and call `.validate()`.

A typical migration is therefore: **crosswalk your 2.1 data to 3.0 → wrap/load it at
`clif_version="3.0"` → `validate()`.**
