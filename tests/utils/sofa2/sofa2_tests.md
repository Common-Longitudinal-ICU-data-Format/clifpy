# SOFA-2 Test Reference

Canonical reference for designing, reviewing, and extending SOFA-2 subscore test data. Covers test architecture, infrastructure helpers, design patterns, and per-subscore case outlines.

## Architecture: Hybrid Approach

| Approach | Pros | Cons |
|----------|------|------|
| **All-in-One** | Single cohort, tests orchestration | Hard to isolate failures, combinatorial explosion |
| **Individual** | Focused, easy to debug | Doesn't test integration |
| **Hybrid** | Best of both | Slightly more files |

Each subscore has its **own cohort + data** for test isolation. A small integration test covers `_core.py` orchestration.

---

## Directory Structure

```
tests/utils/sofa2/
├── sofa2_tests.md          # This file
├── resp/
│   ├── clif_cohort.csv
│   ├── clif_respiratory_support.csv
│   ├── clif_vitals.csv
│   ├── clif_labs.csv
│   ├── clif_ecmo_mcs.csv
│   ├── resp_expected.csv
│   └── test_sofa2_resp.py
├── brain/
│   ├── __init__.py
│   ├── clif_cohort.csv
│   ├── clif_patient_assessments.csv
│   ├── clif_medication_admin_continuous.csv
│   ├── clif_medication_admin_intermittent.csv
│   ├── brain_expected.csv
│   └── test_sofa2_brain.py
├── cv/
│   ├── clif_cohort.csv
│   ├── clif_vitals.csv
│   ├── clif_medication_admin_continuous.csv
│   ├── cv_expected.csv
│   └── test_sofa2_cv.py
├── liver/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── liver_expected.csv
│   └── test_sofa2_liver.py
├── kidney/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── clif_crrt_therapy.csv
│   ├── kidney_expected.csv
│   └── test_sofa2_kidney.py
├── hemo/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── hemo_expected.csv
│   └── test_sofa2_hemo.py
└── integration/
    ├── clif_cohort.csv
    ├── (all CLIF tables)
    ├── sofa2_expected.csv
    └── test_sofa2_integration.py
```

---

## Data Model Reference

### Input Tables (CLIF Format)

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `respiratory_support` | hospitalization_id, recorded_dttm, device_category, fio2_set, lpm_set, mode_category | |
| `labs` | hospitalization_id, **lab_collect_dttm**, lab_category, lab_value_numeric | `lab_collect_dttm` not `lab_result_dttm` |
| `vitals` | hospitalization_id, recorded_dttm, vital_category, vital_value | |
| `ecmo_mcs` | hospitalization_id, recorded_dttm, ecmo_configuration_category, mcs_group | |
| `patient_assessments` | hospitalization_id, recorded_dttm, assessment_category, assessment_value | |
| `medication_admin_continuous` | hospitalization_id, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category | |
| `crrt_therapy` | hospitalization_id, recorded_dttm, crrt_mode_category | |

### Subscore Input Requirements

| Subscore | CLIF Tables | Lab Categories | Vital Categories | Med Categories | Other |
|----------|-------------|----------------|------------------|----------------|-------|
| **Resp** | respiratory_support, labs, vitals, ecmo_mcs | po2_arterial | spo2 | - | device_category |
| **Brain** | patient_assessments, medication_admin_continuous | - | - | dexmedetomidine | gcs_total |
| **CV** | medication_admin_continuous, vitals | - | map | norepinephrine, epinephrine, dopamine, dobutamine, vasopressin, phenylephrine, milrinone, angiotensin_ii, isoproterenol | |
| **Liver** | labs | bilirubin_total | - | - | |
| **Kidney** | labs, crrt_therapy | creatinine, potassium, ph_arterial, ph_venous, bicarbonate | - | - | crrt_mode_category |
| **Hemo** | labs | platelet_count | - | - | |

### Resp Output Columns

```
hospitalization_id, start_dttm, sofa2_resp,
pf_ratio, sf_ratio, has_advanced_support, device_category,
pao2_at_worst, pao2_dttm_offset, spo2_at_worst, spo2_dttm_offset,
fio2_at_worst, fio2_dttm_offset, pf_sf_dttm_offset,
has_ecmo, ecmo_dttm_offset
```

Note: `pf_sf_dttm_offset` is available at the subscore level (`_resp.py`) but excluded from the `_core.py` assembly output. It is tested only in subscore-level tests.

---

## Test Infrastructure

Shared helpers live in `tests/utils/sofa2/conftest.py`. Each subscore test file imports what it needs.

### Fixture Loading

Two approaches depending on CSV cleanliness:

| Approach | When to use | Used by |
|----------|-------------|---------|
| `duckdb.read_csv()` | CSVs have clean ISO timestamps | hemo, liver, kidney, brain |
| `load_csv_fixture(path, datetime_cols)` | CSVs may have Excel-mangled dates | resp |

`load_csv_fixture` reads through pandas with `pd.to_datetime(format='mixed')` first, then converts to a DuckDB relation. This handles Excel reformatting ISO timestamps (`2024-01-01 10:00:00`) to short format (`1/1/24 10:00`). Use this when fixture CSVs will be edited in Excel.

### Expected CSV Conventions

Every `*_expected.csv` follows these conventions:

- **`case` column**: `default` for standard config, descriptive names for custom configs (`short_lookback`, `long_tolerance`, `short_post_sed`)

- **`status` column**: manually set to `validated` by a human reviewer after verifying the expected values

- **`notes` column**: documents what this row tests / proves

- **Sort order**: `hospitalization_id, start_dttm`

`load_expected(fixtures_dir, filename, case)` filters to a single case and returns a sorted pandas DataFrame.

### Column Assertion

Each test file defines a `*_COLUMNS` list of `(column_name, comparison_type)` tuples — the contract between the subscore function output and the expected CSV:

```python
HEMO_COLUMNS = [
    ('sofa2_hemo', 'Int64'),
    ('platelet_count', 'Float64'),
    ('platelet_dttm_offset', 'offset'),
]
```

Supported comparison types:

| Type | Behavior |
|------|----------|
| `Int64` | Exact match, NaN-safe |
| `Float64` | `np.isclose(rtol=1e-9, equal_nan=True)` for computed ratios |
| `str` | Exact match, NaN → empty string |
| `offset` | Converts both sides to total seconds (handles DuckDB INTERVAL ↔ string `HH:MM:SS` mismatch) |

`assert_columns_match(result_df, expected_df, column_specs)` checks all columns, collects all failures, and prints a single assertion error with `hospitalization_id`, `start_dttm`, `notes` context for each mismatch.

### Test Function Patterns

Each subscore test file follows the same structure:

| Function | Purpose |
|----------|---------|
| `test_*_default` | Runs all `case='default'` rows against the full cohort with `SOFA2Config()` |
| `test_*_intermediates` | Verifies `dev=True` returns the expected set of intermediate DuckDB relations |
| `test_*_custom_*` | Parametrized over config variants; filters result to relevant `hosp_ids` from the expected CSV |

Custom config tests reuse the same fixture data as default — only `SOFA2Config` changes.

---

## Distractor Observations

Most test cases include a **distractor observation** — a second in-window measurement that is "better" than the scoring value (higher platelet/GCS for MIN aggregation, lower bilirubin for MAX aggregation). This verifies that the MIN/MAX aggregation and `ARG_MIN`/`ARG_MAX` timestamp selection correctly pick the worst value from multiple observations. Cases where distractors are not viable (no data, pre-window only, GCS=15/motor=6 ties, all-invalidated sedation cases) are skipped.

## Pre-Window Fallback Preference Testing

For test cases that verify "in-window preferred over pre-window" (e.g., hemo 508, liver 308), the **pre-window value must be worse** than the in-window scoring value. "Worse" means the direction that the aggregation function would select:

- **MIN aggregation** (hemo/platelet): pre-window value must be **lower** than in-window MIN

- **MAX aggregation** (liver/bilirubin): pre-window value must be **higher** than in-window MAX

If the pre-window value is already "better" than the in-window value, the aggregation function (MIN or MAX) would select the correct in-window value regardless of whether the fallback logic correctly excludes pre-window data — making the test **moot**. A properly constructed test should fail if the code incorrectly unions pre-window + in-window before aggregating.

| Subscore | Agg | Case | Pre-window | In-window worst | Correct? |
|----------|-----|------|------------|-----------------|----------|
| Hemo | MIN | 508 | 10 | 80 | pre < in-window (leaking pre would change MIN) |
| Liver | MAX | 308 | 8.0 | 4.0 | pre > in-window (leaking pre would change MAX) |
| Resp | MIN(P/F) | 424-d1 | n/a | 200 | distractor@10:00 (P/F=400) vs scoring@04:00 (P/F=200) |

Note: Resp's pre-window fallback is tested via the 3-day pattern (424) and custom lookback (421/422) rather than a dedicated in-window-preferred case, because the ANTI JOIN excludes pre-window at the `(hosp_id, start_dttm)` level.

## Test Design Patterns

These patterns should be applied consistently across all subscores when building or updating test data.

### 1. Distractors already cover multi-obs aggregation

Each boundary case (e.g., hemo 501-505) includes 2 in-window observations: the scoring value and a "better" distractor. This means MIN/MAX aggregation and ARG_MIN/ARG_MAX timestamp selection are already exercised by every boundary test. A dedicated "multi-obs selects worst" case is **redundant** and should not be added separately.

### 2. Multi-window: one direction suffices

Two multi-window cases testing different score directions (e.g., stable→deterioration and critical→recovery) exercise the same underlying logic: **window isolation** — each `(hospitalization_id, start_dttm)` pair is scored independently. One multi-window case is sufficient. The second adds no coverage.

### 3. Daily / missing data pattern (3-day case)

To test daily behavior at the subscore level, use a **3-consecutive-day case with data only on day 1**. This exercises three distinct code paths in a single hosp_id:

| Day | Mechanism | What it tests |
|-----|-----------|---------------|
| Day 1 | In-window scoring | Baseline — normal aggregation within the window |
| Day 2 | Cross-day pre-window ASOF | Day 1's observation is within the lookback window (e.g., 22hr < 24hr) → pre-window fallback activates |
| Day 3 | Lookback expiry → NULL | Day 1's observation is outside the lookback window (e.g., 46hr > 24hr) → no data → NULL |

The concrete lab timing matters: place the day 1 lab early enough that its offset to day 2's start is within the lookback, but its offset to day 3's start exceeds it.

### 4. Subscore vs daily testing boundary

Subscore tests (`_calculate_*_subscore`) validate **raw scores**, including NULL for days with no data within the lookback window. The carry-forward logic (fillna from `LAST_VALUE(... IGNORE NULLS)`) belongs to `calculate_sofa2_daily` in `_core.py` and is tested at the **integration level**, not the subscore level.

At the subscore level: day 3 = NULL (correct raw result).

At the daily level: day 3 = day 2's score (carry-forward).

### 5. Same-data-different-config (config boundary testing)

Use the same `hospitalization_id` under both `default` and a custom config case. The fixture data is identical; only `SOFA2Config` changes. If the outcome flips (scored ↔ NULL), it proves the parameter controls behavior rather than passing by coincidence.

Design: pick data that sits on the boundary — scored under one config, NULL under the other. Always use a **pair**: one case that passes under default but fails under custom, and vice versa.

| Subscore | Parameter | Cases | Default result | Custom result |
|----------|-----------|-------|----------------|---------------|
| Resp | `pf_sf_tolerance_hours` | 417 / 418 | scored / NULL | NULL / scored |
| Resp | `resp_lookback_hours` | 421 / 422 | scored / NULL | NULL / scored |
| Hemo | `hemo_lookback_hours` | 507 / 509 | scored / NULL | NULL / scored |
| Liver | `liver_lookback_hours` | 307 / 309 | scored / NULL | NULL / scored |
| Brain | `post_sedation_gcs_invalidate_hours` | 112 / 113 | invalidated / valid | valid / invalidated |

### 6. Temporal constraint interaction

When a subscore has **multiple temporal constraints** that must both be satisfied (e.g., resp's `lookback` + `tolerance`), test each independently:

| Lookback | Tolerance | Result | Example |
|----------|-----------|--------|---------|
| ✓ within | ✓ within | Scored | Most boundary cases (401-415) |
| ✓ within | ✗ exceeded | NULL | Case 418 (gap=6hr > 4hr tolerance) |
| ✗ exceeded | n/a | NULL | Case 422 (gap=7hr > 6hr lookback) |

Each constraint can also be flipped independently via custom config (patterns 5 pairs). This is more complex than subscores with a single lookback parameter (hemo, liver) and should be documented when it applies.

### 7. ANTI JOIN fallback pattern

When a secondary data source replaces a primary via ANTI JOIN, the primary must be **completely empty for the window** — not just missing individual rows. The test must ensure the primary has data that *exists* but *fails validation*, leaving the primary relation empty for that window.

| Case | Primary (P/F) | Why empty | Secondary (S/F) | Result |
|------|---------------|-----------|-----------------|--------|
| 406-408 | No PaO2 at all | No data | S/F computed | S/F as only source |
| 423 | PaO2@16:00/18:00 exist | Tolerance exceeded (6hr/8hr > 4hr) | SpO2@12:00/14:00 within tolerance | S/F as **fallback** |

Case 423 is the critical one — it proves the fallback activates when the primary *exists but fails*, not just when it's absent. If the code used a simple LEFT JOIN instead of ANTI JOIN, case 423 would behave differently.

### 8. In-window-only flag vs pre-window ASOF

Some signals use INNER JOIN (in-window only) while measurements use pre-window ASOF with configurable lookback. Test by placing the signal just **before** the window start:

- Measurement at the same time: reachable via lookback → scored

- Flag at the same time: INNER JOIN excludes it → not triggered

| Case | Signal | Time | Mechanism | Triggered? |
|------|--------|------|-----------|------------|
| 425 | ECMO | 06:00 (-2hr) | INNER JOIN | No (pre-window) |
| 421 | FiO2+PaO2 | 04:00 (-4hr) | pre-window ASOF (6hr lookback) | Yes |

This asymmetry is the point of the test. The ECMO flag does not "carry forward" like measurements do.

### 9. OR-condition arm isolation

When a flag trigger has multiple OR conditions, don't just test the case where all arms are true. Isolate each arm:

| Case | Arm 1 (`mcs_group='ecmo'`) | Arm 2 (`ecmo_config IS NOT NULL`) | Result |
|------|---------------------------|-----------------------------------|--------|
| 414 | ✓ (`ecmo`) | ✓ (`vv`) | has_ecmo=1 (both true, can't tell which) |
| 426 | ✗ (NULL) | ✓ (`va`) | has_ecmo=1 (proves arm 2 alone suffices) |

This matters when conditions overlap in production data — a bug in one arm would be masked if both are always true together.

### 10. Non-matching distractor rows

Add rows from the same table that share the `hospitalization_id` but should **not** trigger a flag. This catches overly broad WHERE clauses or missing filters.

Types of distractors:

- **Same-table different-category**: iabp/lvad rows in `ecmo_mcs` shouldn't trigger resp ECMO flag (414 + 401 distractors)

- **Same-patient better-value**: second measurement that aggregation should not select as worst (every boundary case's @14:00 distractor — see pattern 1)

- **Same-table different-config**: VA-ECMO distractor for case 414 alongside VV-ECMO — tests MIN aggregation on `ecmo_dttm_offset`

---

## Test Case Outlines

### Resp (hosp_id 401-426)

Default lookback: 6hr (`resp_lookback_hours`), default tolerance: 4hr (`pf_sf_tolerance_hours`)

**P/F score boundaries (401-405) — all with advanced support (imv), distractors at @14:00:**

| hosp_id | pf_ratio | has_advanced | has_ecmo | resp | notes |
|---------|----------|--------------|----------|------|-------|
| 401 | 400 | 1 | 0 | 0 | P/F > 300 |
| 402 | 250 | 1 | 0 | 1 | P/F <= 300 |
| 403 | 200 | 1 | 0 | 2 | P/F <= 225 |
| 404 | 120 | 1 | 0 | 3 | P/F <= 150 + adv |
| 405 | 50 | 1 | 0 | 4 | P/F <= 75 + adv |

**S/F boundaries (406-408) — no PaO2 available:**

| hosp_id | sf_ratio | has_advanced | resp | notes |
|---------|----------|--------------|------|-------|
| 406 | 380 | 0 | 0 | S/F > 300, nasal cannula |
| 407 | 176 | 0 | 2 | S/F <= 250, no adv → capped at 2 |
| 408 | 88 | 1 | 4 | S/F <= 120 + adv (imv) |

**Advanced support cap (409-410) — P/F would score higher with adv support:**

| hosp_id | pf_ratio | has_advanced | resp | notes |
|---------|----------|--------------|------|-------|
| 409 | 120 | 0 | 2 | P/F <= 150, nasal cannula → capped at 2 |
| 410 | 50 | 0 | 2 | P/F <= 75, nasal cannula → capped at 2 |

**FiO2 imputation (411-413):**

| hosp_id | pf_ratio | fio2 | resp | notes |
|---------|----------|------|------|-------|
| 411 | 400 | 0.21 | 0 | room air → FiO2=0.21 |
| 412 | 250 | 0.32 | 1 | NC + LPM=3 → FiO2=0.32 |
| 413 | 200 | 0.5 | 2 | explicit FiO2 overrides LPM imputation |

**ECMO (414, 425-426):**

| hosp_id | pf_ratio | has_ecmo | resp | notes |
|---------|----------|----------|------|-------|
| 414 | 400 | 1 | 4 | ECMO override (both arms of OR: mcs_group='ecmo' + ecmo_config='vv') |
| 425 | 250 | 0 | 1 | Pre-window ECMO@06:00 (2hr before window) → not counted (INNER JOIN) |
| 426 | 400 | 1 | 4 | ecmo_config='va' only (mcs_group=NULL) → second arm of OR triggers |

ECMO distractors in `clif_ecmo_mcs.csv`: 414 iabp, 414 temporary_lvad, 414 ecmo/va@15:00, 401 iabp.

**P/F priority + no data (415-416):**

| hosp_id | pf_ratio | sf_ratio | resp | notes |
|---------|----------|----------|------|-------|
| 415 | 250 | NULL | 1 | P/F takes priority over S/F when both available |
| 416 | NULL | NULL | NULL | No data at all |

**Tolerance (417-418):**

| hosp_id | pf_ratio | resp | notes |
|---------|----------|------|-------|
| 417 | 250 | 1 | pre-window FiO2@06:00, PaO2@09:00 (gap=3hr ≤ 4hr tolerance) |
| 418 | NULL | NULL | PaO2@16:00, FiO2@10:00 (gap=6hr > 4hr tolerance) |

**Device heuristic (419):**

| hosp_id | pf_ratio | device_category | resp | notes |
|---------|----------|-----------------|------|-------|
| 419 | 125 | imv | 3 | mode='Assist Control-Volume Control' → imv inferred |

**Multi-window (420):**

| hosp_id | window | pf_ratio | resp | notes |
|---------|--------|----------|------|-------|
| 420-w1 | d1 | 400 | 0 | Stable |
| 420-w2 | d2 | 50 | 4 | Deterioration |

**Pre-window lookback (421-422):**

| hosp_id | data_offset | default (6hr) | resp | notes |
|---------|-------------|---------------|------|-------|
| 421 | -4hr | P/F=250 | 1 | Within default lookback |
| 422 | -7hr | NULL | NULL | Outside default lookback |

**S/F fallback when P/F tolerance exceeded (423):**

| hosp_id | sf_ratio | pf_ratio | resp | notes |
|---------|----------|----------|------|-------|
| 423 | 176 | NULL | 2 | PaO2@16:00 gap=6hr > 4hr → P/F fails; SpO2@12:00 gap=2hr ≤ 4hr → S/F used |

**Daily / missing data (424) — 3 consecutive days, data late in day 1:**

| hosp_id | day | pf_ratio | resp | notes |
|---------|-----|----------|------|-------|
| 424-d1 | d1 | 200 | 2 | In-window: data@Jan2 04:00 (20hr into window) |
| 424-d2 | d2 | 200 | 2 | Pre-window: same data -4hr ≤ 6hr lookback |
| 424-d3 | d3 | NULL | NULL | Lookback expired: same data -28hr > 6hr |

Day 1 distractor: FiO2+PaO2@Jan1 10:00 (P/F=400), MIN picks 200 at Jan2 04:00.

Custom tolerance tests (via `case` column in `resp_expected.csv`):

| case | hosp_id | tolerance | pf_ratio | resp | notes |
|------|---------|-----------|----------|------|-------|
| short_tolerance | 417 | 1hr | NULL | NULL | gap=3hr > 1hr → exceeded |
| long_tolerance | 418 | 8hr | 250 | 1 | gap=6hr ≤ 8hr → within |

Custom lookback tests (via `case` column in `resp_expected.csv`):

| case | hosp_id | lookback | pf_ratio | resp | notes |
|------|---------|----------|----------|------|-------|
| short_lookback | 421 | 2hr | NULL | NULL | gap=4hr > 2hr → exceeded |
| long_lookback | 422 | 10hr | 250 | 1 | gap=7hr ≤ 10hr → within |

### Brain (hosp_id 101-120)

Default post-sedation invalidation: 12hr (`post_sedation_gcs_invalidate_hours`)

**Score boundaries (101-106) — no meds:**

| hosp_id | gcs_min | gcs_type | has_sedation | has_delirium_drug | brain | notes |
|---------|---------|----------|--------------|-------------------|-------|-------|
| 101 | 15 | gcs_total | 0 | 0 | 0 | GCS >= 15 |
| 102 | 14 | gcs_total | 0 | 0 | 1 | GCS 13-14 |
| 103 | 10 | gcs_total | 0 | 0 | 2 | GCS 9-12 |
| 104 | 7 | gcs_total | 0 | 0 | 3 | GCS 6-8 |
| 105 | 4 | gcs_total | 0 | 0 | 4 | GCS 3-5 |
| 106 | NULL | NULL | 0 | 0 | NULL | No GCS data |

**Delirium drug — footnote e (107-109):**

| hosp_id | gcs_min | gcs_type | has_sedation | has_delirium_drug | brain | notes |
|---------|---------|----------|--------------|-------------------|-------|-------|
| 107 | 15 | gcs_total | 1 | 1 | 1 | dexmed: GCS=15 before sed + delirium forces >= 1 |
| 108 | 10 | gcs_total | 1 | 1 | 2 | dexmed: GCS < 13, delirium doesn't override |
| 109 | 15 | gcs_total | 0 | 1 | 1 | haloperidol intm: delirium forces >= 1 |

**Sedation — footnote c (110-113):**

| hosp_id | gcs_min | gcs_type | has_sedation | brain | notes |
|---------|---------|----------|--------------|-------|-------|
| 110 | NULL | NULL | 1 | 0 | All GCS during sedation invalidated |
| 111 | 14 | gcs_total | 1 | 1 | Valid GCS=14 before sed used; GCS=6 during sed discarded |
| 112 | NULL | NULL | 1 | 0 | Two-episode: GCS +5hr after ep1 stop within 12hr post_sed |
| 113 | 14 | gcs_total | 1 | 1 | Two-episode: GCS +12.5hr after ep1 stop outside 12hr post_sed |

**GCS motor fallback — footnote d (114-116):**

| hosp_id | gcs_min | gcs_type | brain | notes |
|---------|---------|----------|-------|-------|
| 114 | 6 | gcs_motor | 0 | motor=6 fallback |
| 115 | 3 | gcs_motor | 3 | motor=3 fallback |
| 116 | 14 | gcs_total | 1 | gcs_total preferred over gcs_motor |

**Multi-window (117):**

| hosp_id | window | gcs_min | brain | notes |
|---------|--------|---------|-------|-------|
| 117 | w1 | 15 | 0 | Stable |
| 117 | w2 | 7 | 3 | Deterioration |

**Daily / missing data (118) — 3 consecutive days, GCS only on day 1:**

| hosp_id | gcs_min | gcs_type | offset | brain | notes |
|---------|---------|----------|--------|-------|-------|
| 118-d1 | 10 | gcs_total | +2hr | 2 | In-window: normal scoring |
| 118-d2 | NULL | NULL | NULL | NULL | No GCS in window (no pre-window lookback for brain) |
| 118-d3 | NULL | NULL | NULL | NULL | Same as d2 |

Note: Brain has no pre-window ASOF lookback for GCS (GCS is in-window only per spec). Both days 2+3 produce NULL. In `calculate_sofa2_daily`, carry-forward would fill these.

**Edge cases (119-120):**

| hosp_id | has_sedation | has_delirium_drug | brain | notes |
|---------|--------------|-------------------|-------|-------|
| 119 | 1 | 0 | 0 | Sedation present, no GCS at all |
| 120 | 0 | 1 | NULL | Haloperidol intm but no GCS |

Custom post_sedation_gcs_invalidate_hours tests (via `case` column in `brain_expected.csv`):

| case | hosp_id | post_sed_hours | gcs_min | brain | notes |
|------|---------|----------------|---------|-------|-------|
| short_post_sed | 112 | 2hr | 14 | 1 | +5hr after ep1 valid (GCS=14) |
| long_post_sed | 113 | 14hr | NULL | 0 | +12.5hr after ep1 invalidated |

### CV (hosp_id 201-226)

Default pressor min duration: 60 min (`pressor_min_duration_minutes`)

All pressor doses in preferred units (mcg/kg/min for ne/epi/dopa, u/min for vasopressin). Unit conversion tested separately. Weight=70kg provided for all pressor patients.

Each ne/epi/dopa case uses a distractor pattern: first MAR event has the scoring dose, second 'going' event has a slightly lower dose to make ARG_MAX deterministic (picks the higher dose at the first timestamp).

**MAP-only boundaries (201-202):**

| hosp_id | map_min | pressors | cv | notes |
|---------|---------|----------|-----|-------|
| 201 | 75 | none | 0 | MAP ≥ 70 |
| 202 | 65 | none | 1 | MAP < 70 |

**ne+epi dose tiers (203-205) — both ne and epi present to test SUM concurrency:**

| hosp_id | ne | epi | ne+epi | cv | notes |
|---------|-----|-----|--------|-----|-------|
| 203 | 0.06 | 0.04 | 0.10 | 2 | ne+epi ≤ 0.2 |
| 204 | 0.2 | 0.1 | 0.30 | 3 | 0.2 < ne+epi ≤ 0.4 |
| 205 | 0.3 | 0.2 | 0.50 | 4 | ne+epi > 0.4 |

**Dopamine-only (206-208) — footnote l:**

| hosp_id | dopa | cv | notes |
|---------|------|-----|-------|
| 206 | 15 | 2 | dopa ≤ 20, no ne/epi, no other |
| 207 | 30 | 3 | 20 < dopa ≤ 40 |
| 208 | 50 | 4 | dopa > 40 |

**Combination scoring (209-211):**

| hosp_id | ne+epi | other | cv | notes |
|---------|--------|-------|-----|-------|
| 209 | 0 | vasopressin | 2 | Other vaso only |
| 210 | 0.15 | vasopressin | 3 | Low ne+epi + other |
| 211 | 0.3 | vasopressin | 4 | Medium ne+epi + other |

**No data (212):**

| hosp_id | cv | notes |
|---------|-----|-------|
| 212 | NULL | No MAP, no pressors |

**Duration validation (213-214) — footnote j:**

| hosp_id | ne | episode_duration | default (60 min) | MAP | cv | notes |
|---------|-----|-----------------|-------------------|-----|-----|-------|
| 213 | 0.2 | 45 min | < 60 → ignored | 65 | 1 | Falls to MAP < 70 |
| 214 | 0.2 | 90 min | ≥ 60 → counted | 65 | 2 | ne=0.2 → score 2 |

**ne+epi SUM (215):**

| hosp_id | ne | epi | sum | cv | notes |
|---------|-----|-----|-----|----|-------|
| 215 | 0.15 | 0.1 | 0.25 | 3 | Both contribute, sum > 0.2 |

**Mechanical CV support (216-218) — footnote n/i:**

| hosp_id | device | ecmo_config | has_mech | MAP | cv | notes |
|---------|--------|-------------|----------|-----|-----|-------|
| 216 | VA-ECMO | va | 1 | 60 | 4 | Non-VV ECMO → mech support |
| 217 | IABP | | 1 | 60 | 4 | IABP → mech support |
| 218 | VV-ECMO | vv | 0 | 75 | 0 | VV excluded from CV mech support |

Distractors: 216 has temporary_rvad@14:00 (also triggers, but MIN picks VA-ECMO@12:00); 217 has VV-ECMO@14:00 (should NOT trigger).

**Pre-window tests (219-220):**

| hosp_id | scenario | cv | notes |
|---------|----------|-----|-------|
| 219 | Pre-window IABP@06:00 | 1 | INNER JOIN excludes, MAP=65 < 70 |
| 220 | Pre-window ne@07:00, in-window going@08:30+08:45 | 2 | Episode 07:00→08:45=105min; in-window alone=15min < 60 |

**MAR deduplication (221-223):**

| hosp_id | simultaneous entries | winner | ne | cv | notes |
|---------|---------------------|--------|-----|-----|-------|
| 221 | 'new_bag'/0.3 + 'verify'/0.1 @10:00 | new_bag (priority 1<9) | 0.3 | 3 | Action priority |
| 222 | 'going'/0.2 + 'stop'/0 @10:30 | going (priority 7<8) | 0.2 | 2 | Episode continues |
| 223 | 'new_bag'/0.3 + 'new_bag'/0.1 @10:00 | 0.3 (dose DESC) | 0.3 | 3 | Dose tiebreaker |

Case 222 has MAP=65 to differentiate: if 'stop' won, episode breaks → pressor invalid → score 1.

**Pre-window stop (224):**

| hosp_id | pre-window ne | MAP | cv | notes |
|---------|---------------|-----|-----|-------|
| 224 | new_bag@06:00 → stop@07:00 | 65 | 1 | ASOF picks stop → filtered. Falls to MAP |

**Multi-window (225):**

| hosp_id | window | MAP | pressors | cv | notes |
|---------|--------|-----|----------|-----|-------|
| 225-w1 | d1 | 75 | none | 0 | Stable |
| 225-w2 | d2 | 60 | ne=0.3 | 3 | Deterioration |

**3-day pattern (226) — MAP in-window only:**

| hosp_id | day | MAP | cv | notes |
|---------|-----|-----|----|-------|
| 226-d1 | d1 | 65 | 1 | In-window MAP < 70 |
| 226-d2 | d2 | none | NULL | MAP in-window only, no lookback |
| 226-d3 | d3 | none | NULL | Same |

Custom duration tests (via `case` column in `cv_expected.csv`):

| case | hosp_id | min_duration | ne | cv | notes |
|------|---------|-------------|-----|-----|-------|
| short_duration | 213 | 30 min | 0.2 | 2 | 45 ≥ 30 → ne counted |
| long_duration | 214 | 120 min | 0.2 | 1 | 90 < 120 → ne ignored → MAP=65 |

### Liver (hosp_id 301-311)

Default lookback: 24hr (`liver_lookback_hours`)

**Score boundaries (301-305) — each has a distractor observation testing MAX aggregation:**

| hosp_id | bilirubin_total | bilirubin_dttm_offset | liver | notes |
|---------|-----------------|----------------------|-------|-------|
| 301 | 1.0 | +2hr | 0 | <= 1.2 |
| 302 | 2.5 | +2hr | 1 | <= 3.0 |
| 303 | 5.0 | +2hr | 2 | <= 6.0 |
| 304 | 10.0 | +2hr | 3 | <= 12.0 |
| 305 | 15.0 | +2hr | 4 | > 12.0 |

**Edge cases (306-309):**

| hosp_id | bilirubin_total | bilirubin_dttm_offset | liver | notes |
|---------|-----------------|----------------------|-------|-------|
| 306 | NULL | NULL | NULL | No data |
| 307 | 5.0 | -8hr | 2 | Pre-window fallback (no in-window) |
| 308 | 4.0 | +2hr | 2 | In-window preferred over pre-window |
| 309 | NULL | NULL | NULL | Pre-window outside 24hr lookback |

**Multi-window (310):**

| hosp_id | bilirubin_total | bilirubin_dttm_offset | liver | notes |
|---------|-----------------|----------------------|-------|-------|
| 310-w1 | 1.0 | +2hr | 0 | Multi-window: stable |
| 310-w2 | 10.0 | +2hr | 3 | Multi-window: deterioration |

**Daily / missing data (311) — 3 consecutive days, lab only on day 1:**

| hosp_id | bilirubin_total | bilirubin_dttm_offset | liver | notes |
|---------|-----------------|----------------------|-------|-------|
| 311-d1 | 6.0 | +2hr | 2 | In-window: normal scoring |
| 311-d2 | 6.0 | -22hr | 2 | Cross-day pre-window ASOF (within 24hr lookback) |
| 311-d3 | NULL | NULL | NULL | Lookback expired (46hr > 24hr) |

Custom lookback tests (via `case` column in `liver_expected.csv`):

| case | hosp_id | lookback | bilirubin_total | liver | notes |
|------|---------|----------|-----------------|-------|-------|
| short_lookback | 307 | 6hr | NULL | NULL | -8hr pre-window excluded |
| long_lookback | 309 | 36hr | 8.0 | 3 | -28hr pre-window included |

### Kidney (hosp_id 401-408)

| hosp_id | creatinine | has_rrt | rrt_criteria | kidney | notes |
|---------|------------|---------|--------------|--------|-------|
| 401 | 1.0 | 0 | 0 | 0 | <= 1.2 |
| 402 | 1.5 | 0 | 0 | 1 | > 1.2 |
| 403 | 2.5 | 0 | 0 | 2 | > 2.0 |
| 404 | 4.0 | 0 | 0 | 3 | > 3.5 |
| 405 | 2.0 | 1 | 0 | 4 | RRT present |
| 406 | 1.5 | 0 | 1 | 4 | RRT criteria met (K>=6) |
| 407 | 1.5 | 0 | 1 | 4 | RRT criteria met (pH<=7.2, HCO3<=12) |
| 408 | NULL | 0 | 0 | NULL | No data |

### Hemo (hosp_id 501-511)

Default lookback: 24hr (`hemo_lookback_hours`)

**Score boundaries (501-505) — each has a distractor observation testing MIN aggregation:**

| hosp_id | platelet_count | platelet_dttm_offset | hemo | notes |
|---------|---------------|---------------------|------|-------|
| 501 | 200 | +2hr | 0 | > 150 |
| 502 | 140 | +2hr | 1 | <= 150 |
| 503 | 90 | +2hr | 2 | <= 100 |
| 504 | 70 | +2hr | 3 | <= 80 |
| 505 | 40 | +2hr | 4 | <= 50 |

**Edge cases (506-509):**

| hosp_id | platelet_count | platelet_dttm_offset | hemo | notes |
|---------|---------------|---------------------|------|-------|
| 506 | NULL | NULL | NULL | No data |
| 507 | 100 | -2hr | 2 | Pre-window fallback (no in-window) |
| 508 | 80 | +2hr | 3 | In-window preferred over pre-window |
| 509 | NULL | NULL | NULL | Pre-window outside 24hr lookback |

**Multi-window (510):**

| hosp_id | platelet_count | platelet_dttm_offset | hemo | notes |
|---------|---------------|---------------------|------|-------|
| 510-w1 | 200 | +2hr | 0 | Multi-window: stable |
| 510-w2 | 60 | +2hr | 3 | Multi-window: deterioration |

**Daily / missing data (511) — 3 consecutive days, lab only on day 1:**

| hosp_id | platelet_count | platelet_dttm_offset | hemo | notes |
|---------|---------------|---------------------|------|-------|
| 511-d1 | 100 | +2hr | 2 | In-window: normal scoring |
| 511-d2 | 100 | -22hr | 2 | Cross-day pre-window ASOF (within 24hr lookback) |
| 511-d3 | NULL | NULL | NULL | Lookback expired (46hr > 24hr) |

Note: In `calculate_sofa2_daily`, day 3's NULL would be carried forward from day 2. Carry-forward is tested at the integration level, not subscore level.

Custom lookback tests (via `case` column in `hemo_expected.csv`):

| case | hosp_id | lookback | platelet_count | hemo | notes |
|------|---------|----------|---------------|------|-------|
| short_lookback | 507 | 1hr | NULL | NULL | -2hr pre-window excluded |
| long_lookback | 509 | 36hr | 60 | 3 | -28hr pre-window included |

---

## Remaining Phases

### Integration Test

- Small cohort testing `calculate_sofa2()` end-to-end

- Daily carry-forward testing via `calculate_sofa2_daily()`: verify day 2+ NULL subscores are filled from last observed value (subscore tests only validate raw scores, not carry-forward)

### Encounter Stitching Integration Test

- Test `calculate_sofa2()` with `id_name='encounter_block'` and `id_mapping`

- Verify `_remap_clif_rel()`, `_dedup_cohort()`, `_validate_id_name()` and full assembly

- Key scenario: cross-hospitalization pre-window ASOF lookback under a shared encounter_block

- Lives in `tests/utils/sofa2/integration/` alongside the general integration test
