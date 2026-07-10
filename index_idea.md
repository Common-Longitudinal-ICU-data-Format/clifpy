# The mCIDE Index — Idea & Rationale

> A proposal for a single, version-aware **data dictionary** of every permissible mCIDE
> concept in CLIF — replacing the permissible-value lists scattered across per-table,
> per-version schema YAMLs, and adding a Polars **scanner** that reports how many
> hospitalizations actually use each concept.
>
> Modeled on the [AmsterdamUMCdb](https://github.com/AmsterdamUMC/AmsterdamUMCdb)
> `dictionary.csv` / `get_dictionary()`, adapted to CLIF.

---

## 1. The problem

Today, permissible category values live inside each table's schema YAML, duplicated once
per CLIF version:

```
clifpy/schemas/2.1/patient_schema.yaml   # sex_category: [Male, Female, Unknown]
clifpy/schemas/3.0/patient_schema.yaml   # sex_category: [male, female, unknown]
```

Five things break as a result:

| Problem | Today (YAML) | Consequence |
|---|---|---|
| **Not scalable** | ~2,400 values spread across 60+ schema files | No way to see the whole vocabulary; no single list to review |
| **Updates are undetectable** | A value added, removed, or renamed is a buried diff in one of many files | "Did mCIDE change between releases?" is unanswerable at a glance |
| **No stable identity** | A value is just a string; renaming `glucose_fingerstick` → `glucose` looks like a delete plus an add | A concept cannot be tracked across versions |
| **No ontology hooks** | Nowhere to record RxNorm, SNOMED, LOINC, or OMOP `concept_id` | Every site maintains a private crosswalk; no federation path |
| **No usage signal** | The schema states what is *allowed*, never what is *present* | Cannot answer "how many hospitalizations actually have `hfnc`?" |

AmsterdamUMCdb solved the same shape with one flat dictionary keyed by a stable id,
carrying concept mappings and a usage-count column. We adopt that shape — but make the
usage count **live**, computed from data, rather than baked into the file.

---

## 2. What it looks like

One CSV per CLIF table, git-tracked, canonical:

```
clifpy/mcide/
  patient.csv
  vitals.csv
  labs.csv
  respiratory_support.csv
  microbiology_culture.csv
  medication_admin_continuous.csv
  ...                              # 30 tables in total
```

Current contents: **2,383 concepts** across **30 table CSVs** and **75 distinct
`table.category_column` pairs**.

### The columns

| Column | Example | Meaning |
|---|---|---|
| `id` | `pat012` | **Stable** id: table prefix + zero-padded sequence. Assigned in schema order, then frozen forever. Not semantic — never re-read meaning into the number. |
| `table` | `patient` | CLIF table |
| `category_column` | `sex_category` | Which category column the value belongs to |
| `mcide` | `male` | Canonical concept token (the normalized, 3.0-style form) |
| `label` | `Male` | Source display form — the surface string as written in the schema it came from |
| `versions` | `2.1;3.0` | CLIF versions in which this concept is permissible |
| `version_tokens` | `{"2.1":"Male"}` | Per-version surface token, recorded **only when it differs** from `mcide`. Blank otherwise. |
| `status` | `active` | `active` or `deprecated` |
| `ontology` | *(blank)* | Reserved — RxNorm / SNOMED / LOINC / OMOP |
| `concept_id` | *(blank)* | Reserved — external standard concept id |
| `concept_code` | *(blank)* | Reserved — source code in that vocabulary |
| `ontology_level` | *(blank)* | Reserved — granularity (e.g. ingredient vs. product) |
| `mapping_status` | `UNMAPPED` | Reserved — `UNMAPPED` until the ontology columns are populated |
| `notes` | *(blank)* | Free text: rename or deprecation rationale |

> **On `label`:** it holds the *source* display form, not a prettified version of `mcide`.
> For a concept that was renamed, `label` is the **old** token — see `lab057` below, where
> `mcide=glucose` but `label=glucose_fingerstick`. This is a known wrinkle of the
> bootstrap, not a bug to hide.

> **On the ontology columns:** this is a **registry first**. The five ontology columns
> exist so the file is the future single home and nothing has to be re-shaped later, but
> they ship blank / `UNMAPPED`. They are filled incrementally (see §8).

---

## 3. Worked examples

Every concept's life story is one of six shapes: it exists, it is added, it is renamed, it
is merged away, it is split apart, or it is dropped. The index has a representation for
each — and in all six, the id survives. All rows below are copied verbatim from
`clifpy/mcide/*.csv`.

### A plain concept

`pat012 = male`, `pat013 = female` — a stable handle per concept, exactly the mnemonic
this design started from:

```csv
pat012,patient,sex_category,male,Male,2.1;3.0,"{""2.1"":""Male""}",active,,,,,UNMAPPED,
pat013,patient,sex_category,female,Female,2.1;3.0,"{""2.1"":""Female""}",active,,,,,UNMAPPED,
```

Both versions permit the concept, but the surface token differs (`Male` in 2.1, `male` in
3.0). One row carries both.

### An addition (3.0 only)

`versions` names only 3.0; `version_tokens` is empty because there is no 2.1 form:

```csv
pat006,patient,race_category,middle_eastern_or_north_african,Middle Eastern Or North African,3.0,,active,,,,,UNMAPPED,
```

### A rename

One stable id spans both versions. The rename is a single visible field, not a delete plus
an add. **This is what makes "detect updates" trivial.**

```csv
lab057,labs,lab_category,glucose,glucose_fingerstick,2.1;3.0,"{""2.1"":""glucose_fingerstick""}",active,,,,,UNMAPPED,renamed 2.1 'glucose_fingerstick' -> 3.0 'glucose'
rsp004,respiratory_support,device_category,hfnc,High Flow NC,2.1;3.0,"{""2.1"":""High Flow NC""}",active,,,,,UNMAPPED,renamed 2.1 'High Flow NC' -> 3.0 'hfnc'
```

13 concepts are renames of this kind. 175 concepts in total carry a `version_tokens` entry
(that is, their 2.1 surface form differs from the canonical token).

### A merge (many → one)

CLIF 2.1 listed both the misspelled and corrected forms as distinct permissible values;
3.0 keeps only one. The surviving spelling folds into the canonical concept; the loser
becomes a deprecated standalone row that **keeps its own id** and records where it went:

```csv
mcl194,microbiology_culture,organism_category,clostridioides_difficile,clostridioides_difficile,2.1;3.0,,active,,,,,UNMAPPED,
mcl589,microbiology_culture,organism_category,clostridium_difficile,clostridium_difficile,2.1,,deprecated,,,,,UNMAPPED,merged into 3.0 'clostridioides_difficile'
```

Two concepts merged this way (the other is
`staphyloccocus_coagneg` → `staphylococcus_coagneg`).

### A deprecation

Removed in 3.0. The id is retained forever so historical 2.1 data still resolves:

```csv
mcs010,ecmo_mcs,device_category,impella_2.5,Impella_2.5,2.1,"{""2.1"":""Impella_2.5""}",deprecated,,,,,UNMAPPED,removed in 3.0
```

30 concepts are currently deprecated, all of them 2.1-only: 25 `ecmo_mcs` device values
(the Impella / TandemHeart / CentriMag family), 2 `labs` specimen categories (`csf`,
`other`), the 2 `microbiology_culture` merges above, and 1 `medication_admin_continuous`
split (next).

### A split (one → many)

The inverse of a merge, and the case that has no clean automatic answer. 2.1's single
`albumin` concept became two in 3.0. The old id is deprecated and its `notes` name both
successors, so the ambiguity is *recorded* rather than silently resolved:

```csv
mac077,medication_admin_continuous,med_category,albumin,albumin,2.1,,deprecated,,,,,UNMAPPED,"split in 3.0 -> albumin_5, albumin_25"
```

A site migrating 2.1 → 3.0 must decide, per row, which successor applies. The index cannot
make that call — but it makes the decision *visible* instead of letting `albumin` quietly
vanish from the vocabulary.

---

## 4. Compared to AmsterdamUMCdb

Their real `dictionary.csv` header:

```
concept_id, concept_name, domain_id, concept_class_id, vocabulary_id,
concept_code, source_vocabulary_id, source_code, source_code_description,
value_of_concept_id, value_of_source_code,
source_frequency, source_frequency_validated, mapping_status, equivalence
```

Two of their rows — note that `8507` and `8532` are OMOP standard `concept_id`s, that
`source_code` holds the Dutch surface form, and that `source_frequency` is a usage count:

```
8532,FEMALE,Gender,...,AUMC Gender,Vrouw,...,7875,7875,APPROVED,EQUAL
8507,MALE,Gender,...,AUMC Gender,Man,...,14735,14735,APPROVED,EQUAL
```

How the two line up:

| AmsterdamUMCdb | CLIF mCIDE index | Note |
|---|---|---|
| `concept_id` / `concept_name` | reserved `concept_id`, plus our `mcide` / `label` | CLIF uses readable tokens as the key today; the OMOP id is added in Phase 2 |
| `source_code` (Dutch `Man`) | `version_tokens` (`"Male"` in 2.1) | Same idea: a version- or source-specific surface form for one concept |
| `mapping_status`, `equivalence` | `mapping_status` | Same lifecycle field |
| `source_frequency` — **static**, baked into the CSV | **the scanner** — **live**, computed from data | The one place we deliberately diverge |
| `get_dictionary()` → DataFrame | `load_mcide()` → DataFrame | Same access ergonomics |

A frequency column committed to a file goes stale the moment anyone loads new data, and it
describes *their* cohort, not yours. So we leave it out of the file and compute it on
demand.

---

## 5. The scanner

`scan_mcide` reads a CLIF dataset and counts, per concept, how many **distinct
hospitalizations** (and how many rows) use it.

```python
from clifpy.mcide import scan_mcide

usage = scan_mcide(
    data_directory="/data/site_x",
    filetype="parquet",
    clif_version="3.0",
    tables=["respiratory_support"],   # default: every table present on disk
)
```

Returns a Polars DataFrame — the index, joined to live counts (**illustrative numbers**):

```
id      table                 category_column   mcide          n_hospitalizations   n_rows   present  status
rsp001  respiratory_support   device_category   imv                          2210   301884   true     active
rsp004  respiratory_support   device_category   hfnc                         1290    45110   true     active
rsp008  respiratory_support   device_category   nasal_cannula                   0        0   false    active
—       respiratory_support   device_category   HFNC_HUMIDIFIED                 44     1902   true     unmapped
```

Reading the output:

- `present=false` with zero counts → a permitted concept that this site never uses.
- A value found in the data but **absent from the registry** surfaces as
  `status='unmapped'` with a null `id` — a data-quality signal that feeds straight back
  into validation.
- `n_hospitalizations` is null for tables with no `hospitalization_id` (e.g. `patient`);
  use `n_rows` there.

Mechanism: `pl.scan_parquet` (lazy, so the file is never fully materialized) →
`group_by(category_column)` → `agg(n_unique(hospitalization_id), len())` → join to the
index on the version-resolved token.

---

## 6. How it becomes the source of truth

The validator already reads `schema["columns"][i]["permissible_values"]`. Rather than
touch the validator, we **invert ownership one level up**, in the schema loader:

`clifpy/schemas/__init__.py :: load_schema(table_name, clif_version)`

1. Load the table YAML — it keeps dtypes, required flags, and keys (the *structure*).
2. **Overlay** `permissible_values` for each category column from
   `clifpy/mcide/<table>.csv`, filtered to rows where `clif_version ∈ versions`, using
   `version_tokens[clif_version]` when present and `mcide` otherwise.
3. Return the merged schema dict.

`clifpy/utils/validator.py` is **unchanged** — it simply reads the overlaid values. The
YAML `permissible_values:` blocks become vestigial. One canonical list; both versions
derive from it.

---

## 7. Detecting updates

This was the original complaint, and it is the payoff. Because the registry is a handful
of flat, id-keyed, git-tracked CSVs:

```python
from clifpy.mcide import diff_mcide, validate_index

diff_mcide("v0.5.0", "HEAD")   # -> added / removed / renamed / deprecated / reactivated
validate_index()               # -> integrity errors, or [] if clean
```

`validate_index()` is the CI guard: ids unique and never reused, `(table,
category_column, mcide)` unique, every `versions` entry known, no `version_tokens` key
outside `versions`, `status` in `{active, deprecated}`.

And the human answer to "did mCIDE change between releases?" is now just:

```bash
git diff v0.5.0..HEAD -- clifpy/mcide/
```

---

## 8. Phasing

**Phase 1 — the registry.** The CSVs, `load_mcide`, `scan_mcide`, `diff_mcide`,
`validate_index`, the loader overlay, docs, and a parity test proving the overlay
reproduces every existing permissible set exactly, for both 2.1 and 3.0. No ontology
values; those columns ship as `UNMAPPED`.

**Phase 2 — the ontology.** Fill `ontology`, `concept_id`, `concept_code`, and
`ontology_level` incrementally. Start with OMOP demographics (`male` = `8507`, `female` =
`8532` — the same ids Amsterdam uses), then RxNorm for medications, LOINC for labs, and
SNOMED for organisms. The columns already exist, so Phase 2 only fills them — no reshape,
no migration, no id churn.

---

**Sources:** [amsterdamumcdb on PyPI](https://pypi.org/project/amsterdamumcdb/) ·
[AmsterdamUMCdb repository](https://github.com/AmsterdamUMC/AmsterdamUMCdb) ·
dictionary at `amsterdamumcdb/dictionary/dictionary.csv`
