# SOFA-2 Test Plan

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
| `medication_admin_continuous` | hospitalization_id, admin_dttm, med_category, med_dose, med_dose_unit, action_category | |
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
hospitalization_id, start_dttm, pf_ratio, sf_ratio, has_advanced_support,
device_category, pao2_at_worst, pao2_dttm_offset, spo2_at_worst,
spo2_dttm_offset, fio2_at_worst, fio2_dttm_offset, has_ecmo, resp
```

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

---

## Test Case Outlines (Future Subscores)

### Resp (hosp_id 1-22)

| hosp_id | pf_ratio | sf_ratio | has_advanced | has_ecmo | resp | notes |
|---------|----------|----------|--------------|----------|------|-------|
| 1 | 380.95 | NULL | 0 | 0 | 0 | P/F > 300 |
| 2 | NULL | 333.33 | 0 | 0 | 0 | S/F > 300, no PaO2 |
| 3 | 280.00 | NULL | 0 | 0 | 1 | P/F <= 300 |
| 4 | NULL | 290.00 | 0 | 0 | 1 | S/F <= 300 |
| 5 | 200.00 | NULL | 1 | 0 | 2 | P/F <= 225 |
| 6 | NULL | 240.00 | 1 | 0 | 2 | S/F <= 250 |
| 7 | 120.00 | NULL | 1 | 0 | 3 | P/F <= 150 + vent |
| 8 | NULL | 180.00 | 1 | 0 | 3 | S/F <= 200 + vent |
| 9 | 120.00 | NULL | 0 | 0 | 2 | P/F <= 150 no vent -> capped at 2 |
| 10 | 60.00 | NULL | 1 | 0 | 4 | P/F <= 75 + vent |
| 11 | NULL | 100.00 | 1 | 0 | 4 | S/F <= 120 + vent |
| 12 | 400.00 | NULL | 1 | 1 | 4 | ECMO override |
| 13 | 380.95 | NULL | 0 | 0 | 0 | FiO2 imputed (room air) |
| 14 | 222.22 | NULL | 0 | 0 | 2 | FiO2 imputed (4 LPM) |
| 15 | 400.00 | NULL | 0 | 0 | 0 | Pre-window FiO2 fallback |
| 16 | 200.00 | NULL | 1 | 0 | 2 | Pre-window ignored |
| 17 | NULL | NULL | NULL | 0 | NULL | >4hr tolerance exceeded |
| 18 | NULL | 285.00 | 0 | 0 | 1 | S/F fallback (no PaO2) |
| 19 | NULL | NULL | NULL | 0 | NULL | SpO2 >=98% filtered |
| 20-w1 | 200.00 | NULL | 1 | 0 | 2 | Multi-window: deterioration w1 |
| 20-w2 | 60.00 | NULL | 1 | 0 | 4 | Multi-window: deterioration w2 |
| 21-w1 | 60.00 | NULL | 1 | 0 | 4 | Multi-window: recovery w1 |
| 21-w2 | 350.00 | NULL | 0 | 0 | 0 | Multi-window: recovery w2 |
| 22 | NULL | NULL | NULL | 0 | NULL | No data |

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

### CV (hosp_id 201-213)

| hosp_id | map_min | ne+epi | dopamine | other_vaso | cv | notes |
|---------|---------|--------|----------|------------|-----|-------|
| 201 | 75 | 0 | 0 | 0 | 0 | MAP >= 70, no pressors |
| 202 | 65 | 0 | 0 | 0 | 1 | MAP < 70, no pressors |
| 203 | 60 | 0.1 | 0 | 0 | 2 | Low-dose ne+epi (<=0.2) |
| 204 | 55 | 0.3 | 0 | 0 | 3 | Medium-dose (0.2-0.4) |
| 205 | 50 | 0.5 | 0 | 0 | 4 | High-dose (>0.4) |
| 206 | 60 | 0 | 15 | 0 | 2 | Dopamine <=20 |
| 207 | 55 | 0 | 30 | 0 | 3 | Dopamine 20-40 |
| 208 | 50 | 0 | 50 | 0 | 4 | Dopamine >40 |
| 209 | 60 | 0 | 0 | 1 | 2 | Other vasopressor only |
| 210 | 55 | 0.15 | 0 | 1 | 3 | Low ne+epi + other |
| 211 | NULL | 0 | 0 | 0 | NULL | No MAP data |
| 212 | 60 | 0.2 | 0 | 0 | 2 | Pressor <60 min -> ignored |
| 213 | 60 | 0.2 | 0 | 0 | 2 | Pressor >=60 min -> counted |

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

## Future Phases

### Phase 2: Respiratory Fixtures

- Create under `tests/utils/sofa2/resp/`

- Use `lab_collect_dttm` (not `lab_result_dttm`)

### Phase 3: Other Subscores

- CV, Kidney

### Phase 4: Integration Test

- Small cohort testing `calculate_sofa2()` end-to-end

- Daily carry-forward testing via `calculate_sofa2_daily()`: verify day 2+ NULL subscores are filled from last observed value (subscore tests only validate raw scores, not carry-forward)

### Phase 5: Encounter Stitching Integration Test

- Test `calculate_sofa2()` with `id_name='encounter_block'` and `id_mapping`

- Verify `_remap_clif_rel()`, `_dedup_cohort()`, `_validate_id_name()` and full assembly

- Key scenario: cross-hospitalization pre-window ASOF lookback under a shared encounter_block

- Lives in `tests/utils/sofa2/integration/` alongside the general integration test
