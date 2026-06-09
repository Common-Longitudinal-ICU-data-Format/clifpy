# DQA Check Count — Beta Tables Reference

How many DQA validation checks does `clifpy` actually run when a CLIF site has all 16 beta tables fully populated, every column present, and every required mCIDE value represented?

**Short answer:**
- **255 check function invocations** — distinct `Result` objects produced by `run_full_dqa` across all beta tables.
- **3,363 atomic checks** — individual things examined inside those results (per column, per rule, per permissible value, per range).

The atomic count is the more useful number: it scales with what's actually in the schemas and tells you how the validator's coverage grows when you add a column or an mCIDE value.

---

## 1. The 16 beta tables

From the [CLIF v2.1.0 data dictionary](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0):

`patient`, `hospitalization`, `adt`, `vitals`, `labs`, `patient_assessments`, `medication_admin_continuous`, `medication_admin_intermittent`, `respiratory_support`, `position`, `patient_procedures`, `code_status`, `crrt_therapy`, `hospital_diagnosis`, `microbiology_culture`, `microbiology_susceptibility`.

Excluded as concept (non-beta): `ecmo_mcs`, `microbiology_nonculture`.

---

## 2. Two ways to count

| View | What it counts | Total |
|---|---|---:|
| **Function invocations** | One `Result` object per (table × check function) returned by `run_full_dqa` + cross-table runners. | **255** |
| **Atomic checks** | Per-column / per-rule / per-permissible-value / per-range subchecks recorded in `result.metrics`. | **3,363** |

The function-invocation view gives the same total whether a table has 5 columns or 50. The atomic view captures the *work* the validator does and is what scales when schemas grow.

---

## 3. The 20 DQA check types

Sourced from `clifpy/utils/rule_codes.py`. The "atomic granularity" column says what counts as one check inside that rule.

| Code | Check | Atomic granularity | Function (`clifpy/utils/validator.py`) |
|---|---|---|---|
| C.1 | `table_presence` | 1 per beta table | `check_table_presence` |
| C.2 | `required_columns` | 1 per required column in schema | `check_required_columns` |
| C.3 | `column_dtypes` | 1 per column with declared dtype | `check_column_dtypes` |
| C.4 | `datetime_format` | 1 per `DATETIME` / `DATE` column | `check_datetime_format` |
| C.5 | `categorical_values` | 1 per entry in schema's `category_columns` list | `check_categorical_values` |
| C.6 | `category_group_mapping` | 1 per (category → expected_group) pair across all `*_category_to_group_mapping` keys | `check_category_group_mapping` |
| C.7 | `lab_reference_units` | 1 per entry in `labs_schema.lab_reference_units` | `check_lab_reference_units` |
| K.1 | `missingness` | 1 per required column | `check_missingness` |
| K.2 | `conditional_requirements` | 1 per (rule × `then_required` column) — matches the row granularity the check emits | `check_conditional_requirements` |
| K.3 | `mcide_value_coverage` | 1 per permissible value across category columns | `check_mcide_value_coverage` |
| K.4 | `relational_integrity` | 1 per (table × FK column) where ref ≠ self | `check_relational_integrity` |
| K.5 | `cross_table_conditional_completeness` | 1 per cross-table rule, attached to the rule's **target** table | `run_cross_table_completeness_checks` |
| P.1 | `temporal_ordering` | 1 per rule | `check_temporal_ordering` |
| P.2 | `numeric_range_plausibility` | 1 per leaf range in `outlier_config.yaml` (per col / per cat / per cat × unit) | `check_numeric_range_plausibility` |
| P.3 | `field_plausibility` | 1 per rule | `check_field_plausibility` |
| P.4 | `medication_dose_unit_consistency` | 1 per medication table | `check_medication_dose_unit_consistency` |
| P.5 | `overlapping_periods` | 1 per table with overlap rule | `check_overlapping_periods` |
| P.6 | `category_temporal_consistency` | 1 per (category column × permissible value) when table has a detectable time column | `check_category_temporal_consistency` |
| P.7 | `duplicate_composite_keys` | 1 per table with composite keys defined | `check_duplicate_composite_keys` |
| P.8 | `cross_table_temporal` | 1 per (table × time column) for non-hosp tables with `hospitalization_id` | `check_cross_table_temporal_plausibility` |

---

## 4. Per-table breakdown

Each subsection shows:
- **Schema inputs** — what's in the YAMLs that drives the count
- **Worked sum** — every nonzero check type with its derivation
- **Total** — atomic checks for that table

### patient — 92

**Schema inputs:** 11 columns • 7 required • 2 date/datetime (`birth_date`, `death_dttm`) • 4 category cols (`race_category`, `ethnicity_category`, `sex_category`, `language_category`) with 7+3+3+45=58 mCIDE values • composite key `[patient_id]` • no FK cols • no numeric ranges • no detectable time column for P.6 • **target of 1 K.5 rule** (Expired → `death_dttm`)

| Code | Count | Derivation |
|---|---:|---|
| C.1 table_presence | 1 | one per table |
| C.2 required_columns | 7 | 7 required cols |
| C.3 column_dtypes | 11 | 11 columns total |
| C.4 datetime_format | 2 | `birth_date`, `death_dttm` |
| C.5 categorical_values | 4 | 4 cat columns |
| K.1 missingness | 7 | 7 required cols |
| K.3 mcide_value_coverage | 58 | race=7 + ethnicity=3 + sex=3 + language=45 |
| K.5 cross_table_conditional_completeness | 1 | patient is target of `hospitalization.discharge_category=Expired → death_dttm` |
| P.7 duplicate_composite_keys | 1 | composite key defined |
| **Total** | **92** | |

---

### hospitalization — 88

**Schema inputs:** 18 columns • 7 required • 2 datetime (`admission_dttm`, `discharge_dttm`) • 2 category cols (`admission_type_category`=6, `discharge_category`=17) → 23 mCIDE values • FK `patient_id` → patient • composite key `[hospitalization_id]` • 1 numeric range (`age_at_admission`) • 1 temporal_ordering rule • 1 field_plausibility rule

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 7 | 7 required cols |
| C.3 | 18 | 18 columns |
| C.4 | 2 | 2 datetime cols |
| C.5 | 2 | 2 cat cols |
| K.1 | 7 | 7 required cols |
| K.3 | 23 | admission_type=6 + discharge=17 |
| K.4 relational_integrity | 1 | `patient_id` → patient |
| P.1 temporal_ordering | 1 | admission < discharge |
| P.2 numeric_range_plausibility | 1 | `age_at_admission` 0–120 |
| P.3 field_plausibility | 1 | discharge_dttm not-null ⇒ discharge_category ≠ Still Admitted |
| P.6 category_temporal_consistency | 23 | per (cat col × permissible value): admission_type=6 + discharge=17 |
| P.7 | 1 | composite key |
| **Total** | **88** | |

---

### adt — 86

**Schema inputs:** 8 cols • 7 required • 2 datetime (`in_dttm`, `out_dttm`) • 3 cat cols (`hospital_type`=3, `location_category`=12, `location_type`=10) → 25 mCIDE values • FK `hospitalization_id` • composite key `[hospitalization_id, in_dttm]` • 1 conditional req rule • 1 temporal_ordering rule • 1 field_plausibility rule • overlap rule defined

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 7 | 7 required |
| C.3 | 8 | 8 cols |
| C.4 | 2 | `in_dttm`, `out_dttm` |
| C.5 | 3 | 3 cat cols |
| K.1 | 7 | 7 required |
| K.2 conditional_requirements | 1 | ICU location ⇒ location_type required |
| K.3 | 25 | 3 + 12 + 10 |
| K.4 | 1 | `hospitalization_id` |
| P.1 | 1 | in_dttm < out_dttm |
| P.3 | 1 | non-ICU ⇒ no ICU location_type |
| P.5 overlapping_periods | 1 | overlap config defined |
| P.6 | 25 | per (cat col × permissible value): 3 + 12 + 10 |
| P.7 | 1 | composite key |
| P.8 cross_table_temporal | 2 | `in_dttm`, `out_dttm` × hospitalization_id present |
| **Total** | **86** | |

---

### vitals — 47

**Schema inputs:** 6 cols • 4 required • 1 datetime (`recorded_dttm`) • 1 cat col (`vital_category`=9) → 9 mCIDE values • FK `hospitalization_id` • composite key `[hospitalization_id, recorded_dttm, vital_category]` • 9 numeric ranges (one per vital_category)

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 4 | 4 required |
| C.3 | 6 | 6 cols |
| C.4 | 1 | `recorded_dttm` |
| C.5 | 1 | `vital_category` |
| K.1 | 4 | 4 required |
| K.3 | 9 | 9 vital_category values |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 9 | 9 (vital_category → vital_value) ranges in `outlier_config.yaml` |
| P.6 | 9 | per (cat col × permissible value): 9 vital_category values |
| P.7 | 1 | composite key |
| P.8 | 1 | `recorded_dttm` |
| **Total** | **47** | |

---

### labs — 263

**Schema inputs:** 14 cols • 8 required • 3 datetime (`lab_order_dttm`, `lab_collect_dttm`, `lab_result_dttm`) • 2 cat cols in `category_columns` (`lab_order_category`=6, `lab_category`=52) → 58 mCIDE values • FK `hospitalization_id` • composite key `[hospitalization_id, lab_collect_dttm, lab_category]` • 52 numeric ranges (`lab_value_numeric` per lab_category) • 52 lab_reference_units entries • 2 temporal_ordering rules

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 8 | 8 required |
| C.3 | 14 | 14 cols |
| C.4 | 3 | 3 dttm cols |
| C.5 | 2 | 2 cat cols |
| C.7 lab_reference_units | 52 | 52 entries in `lab_reference_units` |
| K.1 | 8 | 8 required |
| K.3 | 58 | lab_order=6 + lab=52 |
| K.4 | 1 | `hospitalization_id` |
| P.1 | 2 | order < collect, collect < result |
| P.2 | 52 | 52 (lab_category → range) entries |
| P.6 | 58 | per (cat col × permissible value): lab_order=6 + lab=52 |
| P.7 | 1 | composite key |
| P.8 | 3 | 3 time cols |
| **Total** | **263** | |

---

### patient_assessments — 339

**Schema inputs:** 8 cols • 6 required • 1 datetime (`recorded_dttm`) • 2 cat cols (`assessment_category`=70, `assessment_group`=18) → 88 mCIDE values • 70-entry `assessment_category_to_group_mapping` • FK `hospitalization_id` • composite key `[hospitalization_id, recorded_dttm, assessment_category]` • 66 numeric ranges (per assessment_category)

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 6 | 6 required |
| C.3 | 8 | 8 cols |
| C.4 | 1 | `recorded_dttm` |
| C.5 | 2 | 2 cat cols |
| C.6 category_group_mapping | 70 | 70 entries in `assessment_category_to_group_mapping` |
| K.1 | 6 | 6 required |
| K.3 | 88 | assessment_category=70 + assessment_group=18 |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 66 | 66 (assessment_category → numerical_value) ranges |
| P.6 | 88 | per (cat col × permissible value): 70 + 18 |
| P.7 | 1 | composite key |
| P.8 | 1 | `recorded_dttm` |
| **Total** | **339** | |

---

### medication_admin_continuous — 352

**Schema inputs:** 13 cols • 9 required • 1 datetime (`admin_dttm`) • 4 cat cols (`med_category`=75, `med_group`=13, `med_route_category`=3, `mar_action_category`=6) → 97 mCIDE values • 75-entry `med_category_to_group_mapping` • FK `hospitalization_id` • composite key 4-col • 42 numeric ranges (per med × dose unit) • P.4 applies

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 9 | 9 required |
| C.3 | 13 | 13 cols |
| C.4 | 1 | `admin_dttm` |
| C.5 | 4 | 4 cat cols |
| C.6 | 75 | 75 entries in `med_category_to_group_mapping` |
| K.1 | 9 | 9 required |
| K.3 | 97 | 75 + 13 + 3 + 6 |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 42 | 16 meds × dose units (e.g., norepinephrine has 3 unit ranges) |
| P.4 medication_dose_unit_consistency | 1 | continuous med dose units must be rate-based |
| P.6 | 97 | per (cat col × permissible value): 75 + 13 + 3 + 6 |
| P.7 | 1 | composite key |
| P.8 | 1 | `admin_dttm` |
| **Total** | **352** | |

---

### medication_admin_intermittent — 393

**Schema inputs:** 13 cols • 9 required • 1 datetime (`admin_dttm`) • 3 cat cols in `category_columns` (`med_category`=165, `med_route_category`=5, `mar_action_category`=4) → 174 mCIDE values • no `med_category_to_group_mapping` • FK `hospitalization_id` • composite key 4-col • 5 numeric ranges • P.4 applies

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 9 | 9 required |
| C.3 | 13 | 13 cols |
| C.4 | 1 | `admin_dttm` |
| C.5 | 3 | 3 cat cols |
| K.1 | 9 | 9 required |
| K.3 | 174 | med=165 + route=5 + mar=4 |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 5 | 5 (med, "mg"/"mcg") ranges (propofol, midazolam, fentanyl, hydromorphone, lorazepam) |
| P.4 | 1 | intermittent med dose units must be discrete |
| P.6 | 174 | per (cat col × permissible value): 165 + 5 + 4 |
| P.7 | 1 | composite key |
| P.8 | 1 | `admin_dttm` |
| **Total** | **393** | |

---

### respiratory_support — 150

**Schema inputs:** 26 cols • 16 required • 1 datetime (`recorded_dttm`) • 2 cat cols in `category_columns` (`device_category`=9, `mode_category`=8) → 17 mCIDE values • FK `hospitalization_id` • composite key `[hospitalization_id, recorded_dttm]` • 17 numeric ranges (one per setting/observation column) • **15 conditional requirement rules** (the busiest table for K.2) covering 34 (rule × `then_required` column) atomic combos

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 16 | 16 required |
| C.3 | 26 | 26 cols |
| C.4 | 1 | `recorded_dttm` |
| C.5 | 2 | 2 cat cols |
| K.1 | 16 | 16 required |
| K.2 | 34 | sum of `then_required` col counts across 15 rules (e.g. CPAP=4, NIPPV Volume Support=5, IMV Pressure Control=3) |
| K.3 | 17 | device=9 + mode=8 |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 17 | 17 numeric setting/observation columns (`fio2_set`, `lpm_set`, … `mean_airway_pressure_obs`) |
| P.6 | 17 | per (cat col × permissible value): device=9 + mode=8 |
| P.7 | 1 | composite key |
| P.8 | 1 | `recorded_dttm` |
| **Total** | **150** | |

---

### position — 20

**Schema inputs:** 4 cols • 3 required • 1 datetime (`recorded_dttm`) • 1 cat col (`position_category`=2) • FK `hospitalization_id` • composite key `[hospitalization_id, recorded_dttm]` • no numeric ranges

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 3 | 3 required |
| C.3 | 4 | 4 cols |
| C.4 | 1 | `recorded_dttm` |
| C.5 | 1 | `position_category` |
| K.1 | 3 | 3 required |
| K.3 | 2 | position_category=2 (prone, not_prone) |
| K.4 | 1 | `hospitalization_id` |
| P.6 | 2 | per (cat col × permissible value): position_category=2 |
| P.7 | 1 | composite key |
| P.8 | 1 | `recorded_dttm` |
| **Total** | **20** | |

---

### patient_procedures — 18

**Schema inputs:** 6 cols • 4 required • 1 datetime (`procedure_billed_dttm`) • `category_columns: [null]` (schema artifact — the null entry has no matching column, so C.5 contributes 0 at runtime) • FK `hospitalization_id` • composite key 3-col • no permissible_values defined • no numeric ranges

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 4 | 4 required |
| C.3 | 6 | 6 cols |
| C.4 | 1 | `procedure_billed_dttm` |
| C.5 | 0 | `check_categorical_values` only counts columns with `permissible_values`; the null entry matches nothing |
| K.1 | 4 | 4 required |
| K.3 | 0 | no permissible_values on cat cols |
| K.4 | 1 | `hospitalization_id` |
| P.6 | 0 | no category columns present; `check_category_temporal_consistency` hits the "not applicable" early-return (0/0 since `d56bf12`) |
| P.7 | 1 | composite key |
| **Total** | **18** | |

---

### code_status — 34

**Schema inputs:** 4 cols • 3 required • 1 datetime (`start_dttm`) • 1 cat col (`code_status_category`=10) • FK `patient_id` • **no composite key** in `validation_rules.yaml`

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 3 | 3 required |
| C.3 | 4 | 4 cols |
| C.4 | 1 | `start_dttm` |
| C.5 | 1 | `code_status_category` |
| K.1 | 3 | 3 required |
| K.3 | 10 | 10 code_status_category values |
| K.4 | 1 | `patient_id` |
| P.6 | 10 | per (cat col × permissible value): 10 code_status_category values |
| **Total** | **34** | |

P.7 is 0 because `validation_rules.yaml` does not declare composite keys for `code_status`. P.8 is 0 because `code_status` is not in `_CROSS_TABLE_TIME_COLUMNS`.

---

### crrt_therapy — 50

**Schema inputs:** 11 cols • 8 required • 1 datetime (`recorded_dttm`) • 1 cat col (`crrt_mode_category`=5) • FK `hospitalization_id` • **no composite key** • 5 numeric ranges • 2 conditional requirement rules

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 8 | 8 required |
| C.3 | 11 | 11 cols |
| C.4 | 1 | `recorded_dttm` |
| C.5 | 1 | `crrt_mode_category` |
| K.1 | 8 | 8 required |
| K.2 | 3 | CVVH/CVVHDF (2 req cols) + CVVHD/CVVHDF (1 req col) = 3 combos |
| K.3 | 5 | 5 crrt_mode_category values |
| K.4 | 1 | `hospitalization_id` |
| P.2 | 5 | blood_flow_rate, pre/post filter rates, dialysate, ultrafiltration |
| P.6 | 5 | per (cat col × permissible value): 5 crrt_mode_category values |
| P.8 | 1 | `recorded_dttm` |
| **Total** | **50** | |

---

### hospital_diagnosis — 18

**Schema inputs:** 5 cols • 5 required • **0 datetime** • **0 category_columns** • FK `hospitalization_id` • composite key `[hospitalization_id, diagnosis_code]`

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 5 | all 5 cols required |
| C.3 | 5 | 5 cols |
| K.1 | 5 | 5 required |
| K.4 | 1 | `hospitalization_id` |
| P.7 | 1 | composite key |
| **Total** | **18** | |

The smallest contributor — no datetime, no categorical content, no numeric ranges.

---

### microbiology_culture — 1,227

**Schema inputs:** 12 cols • 10 required • 3 datetime (`order_dttm`, `collect_dttm`, `result_dttm`) • 3 cat cols in `category_columns` (`fluid_category`=44, `method_category`=3, `organism_category`=543) → **590 mCIDE values** • FKs `hospitalization_id` and `patient_id` • composite key 4-col • 2 temporal_ordering rules

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 10 | 10 required |
| C.3 | 12 | 12 cols |
| C.4 | 3 | 3 dttm cols |
| C.5 | 3 | 3 cat cols |
| K.1 | 10 | 10 required |
| K.3 | 590 | fluid=44 + method=3 + organism=543 |
| K.4 | 2 | `hospitalization_id`, `patient_id` |
| P.1 | 2 | order < collect, collect < result |
| P.6 | 590 | per (cat col × permissible value): fluid=44 + method=3 + organism=543 |
| P.7 | 1 | composite key |
| P.8 | 3 | 3 time cols |
| **Total** | **1,227** | |

The single largest contributor: `organism_category` alone has 543 permissible values.

---

### microbiology_susceptibility — 186

**Schema inputs:** 6 cols • 2 required • **0 datetime** • 2 cat cols (`antimicrobial_category`=167, `susceptibility_category`=4) → 171 mCIDE values • FK `organism_id` • composite key `[organism_id, antimicrobial_category]`

| Code | Count | Derivation |
|---|---:|---|
| C.1 | 1 | |
| C.2 | 2 | 2 required |
| C.3 | 6 | 6 cols |
| C.5 | 2 | 2 cat cols |
| K.1 | 2 | 2 required |
| K.3 | 171 | antimicrobial=167 + susceptibility=4 |
| K.4 | 1 | `organism_id` |
| P.7 | 1 | composite key |
| **Total** | **186** | |

P.6 = 0 because there's no datetime column at all (no `_detect_time_column` candidate present).

---

## 5. Roll-up matrix

| Table | C.1 | C.2 | C.3 | C.4 | C.5 | C.6 | C.7 | K.1 | K.2 | K.3 | K.4 | K.5 | P.1 | P.2 | P.3 | P.4 | P.5 | P.6 | P.7 | P.8 | **Total** |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| patient | 1 | 7 | 11 | 2 | 4 | 0 | 0 | 7 | 0 | 58 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | **92** |
| hospitalization | 1 | 7 | 18 | 2 | 2 | 0 | 0 | 7 | 0 | 23 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 23 | 1 | 0 | **88** |
| adt | 1 | 7 | 8 | 2 | 3 | 0 | 0 | 7 | 1 | 25 | 1 | 0 | 1 | 0 | 1 | 0 | 1 | 25 | 1 | 2 | **86** |
| vitals | 1 | 4 | 6 | 1 | 1 | 0 | 0 | 4 | 0 | 9 | 1 | 0 | 0 | 9 | 0 | 0 | 0 | 9 | 1 | 1 | **47** |
| labs | 1 | 8 | 14 | 3 | 2 | 0 | 52 | 8 | 0 | 58 | 1 | 0 | 2 | 52 | 0 | 0 | 0 | 58 | 1 | 3 | **263** |
| patient_assessments | 1 | 6 | 8 | 1 | 2 | 70 | 0 | 6 | 0 | 88 | 1 | 0 | 0 | 66 | 0 | 0 | 0 | 88 | 1 | 1 | **339** |
| medication_admin_continuous | 1 | 9 | 13 | 1 | 4 | 75 | 0 | 9 | 0 | 97 | 1 | 0 | 0 | 42 | 0 | 1 | 0 | 97 | 1 | 1 | **352** |
| medication_admin_intermittent | 1 | 9 | 13 | 1 | 3 | 0 | 0 | 9 | 0 | 174 | 1 | 0 | 0 | 5 | 0 | 1 | 0 | 174 | 1 | 1 | **393** |
| respiratory_support | 1 | 16 | 26 | 1 | 2 | 0 | 0 | 16 | 34 | 17 | 1 | 0 | 0 | 17 | 0 | 0 | 0 | 17 | 1 | 1 | **150** |
| position | 1 | 3 | 4 | 1 | 1 | 0 | 0 | 3 | 0 | 2 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 2 | 1 | 1 | **20** |
| patient_procedures | 1 | 4 | 6 | 1 | 0 | 0 | 0 | 4 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | **18** |
| code_status | 1 | 3 | 4 | 1 | 1 | 0 | 0 | 3 | 0 | 10 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 10 | 0 | 0 | **34** |
| crrt_therapy | 1 | 8 | 11 | 1 | 1 | 0 | 0 | 8 | 3 | 5 | 1 | 0 | 0 | 5 | 0 | 0 | 0 | 5 | 0 | 1 | **50** |
| hospital_diagnosis | 1 | 5 | 5 | 0 | 0 | 0 | 0 | 5 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | **18** |
| microbiology_culture | 1 | 10 | 12 | 3 | 3 | 0 | 0 | 10 | 0 | 590 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 590 | 1 | 3 | **1,227** |
| microbiology_susceptibility | 1 | 2 | 6 | 0 | 2 | 0 | 0 | 2 | 0 | 171 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | **186** |
| **GRAND TOTAL** | **16** | **108** | **165** | **21** | **31** | **145** | **52** | **108** | **38** | **1,327** | **16** | **1** | **6** | **197** | **2** | **2** | **1** | **1,098** | **14** | **15** | **3,363** |

K.5 (the cross-table `Expired → death_dttm` rule) attaches to its **target** table in the validator's output — `run_cross_table_completeness_checks` does `results.setdefault(target_table, {})[rule_key] = result`. The only current rule targets `patient`, so patient gets K.5 = 1 and every other table gets 0. Add a rule targeting another table and that table's K.5 column bumps.

### Pillar totals per table

Collapsed view of the matrix above, summed into the three DQA pillars (C = C.1–C.7, K = K.1–K.5, P = P.1–P.8). Useful for comparing against DQA report outputs that only report per-pillar counts.

| Table | Conformance | Completeness | Plausibility | **Total** |
|---|--:|--:|--:|--:|
| patient | 25 | 66 | 1 | **92** |
| hospitalization | 30 | 31 | 27 | **88** |
| adt | 21 | 34 | 31 | **86** |
| vitals | 13 | 14 | 20 | **47** |
| labs | 80 | 67 | 116 | **263** |
| patient_assessments | 88 | 95 | 156 | **339** |
| medication_admin_continuous | 103 | 107 | 142 | **352** |
| medication_admin_intermittent | 27 | 184 | 182 | **393** |
| respiratory_support | 46 | 68 | 36 | **150** |
| position | 10 | 6 | 4 | **20** |
| patient_procedures | 12 | 5 | 1 | **18** |
| code_status | 10 | 14 | 10 | **34** |
| crrt_therapy | 22 | 17 | 11 | **50** |
| hospital_diagnosis | 11 | 6 | 1 | **18** |
| microbiology_culture | 29 | 602 | 596 | **1,227** |
| microbiology_susceptibility | 11 | 174 | 1 | **186** |
| **GRAND TOTAL** | **538** | **1,490** | **1,335** | **3,363** |

---

## 6. Where the count concentrates

Two checks alone produce **2,425 of the 3,363 atomic checks (~73%)** — K.3 and P.6 share the same per-(category column × permissible value) granularity and dominate the count:

- **K.3 mcide_value_coverage (1,327)** — driven by enumerations with hundreds of values:
  - `microbiology_culture.organism_category`: 543
  - `microbiology_susceptibility.antimicrobial_category`: 167
  - `medication_admin_intermittent.med_category`: 165
  - `medication_admin_continuous.med_category`: 75
  - `patient_assessments.assessment_category`: 70
  - `labs.lab_category`: 52
  - `patient.language_category`: 45
  - `microbiology_culture.fluid_category`: 44
  - everything else: ~166

- **P.6 category_temporal_consistency (1,098)** — same per-value granularity as K.3 but only for tables with a detectable time column (patient, hospital_diagnosis, microbiology_susceptibility, patient_procedures all contribute 0). Value totals per table match K.3 minus the tables with no time column (so 1,098 = 1,327 − 45 − 171 − 0 − 0 − the 13 group-column values already excluded from both).

- **P.2 numeric_range_plausibility (197)** — every leaf range in `outlier_config.yaml`:
  - `patient_assessments.numerical_value` × 66 assessment categories
  - `labs.lab_value_numeric` × 52 lab categories
  - `medication_admin_continuous.med_dose` × 42 (med × dose unit)
  - `respiratory_support` × 17 setting/observation columns
  - `vitals.vital_value` × 9 vital categories
  - `crrt_therapy` × 5
  - `medication_admin_intermittent.med_dose` × 5
  - `hospitalization.age_at_admission` × 1

The next meaningful contributor is **C.6 category_group_mapping (145)** = 70 (patient_assessments) + 75 (medication_admin_continuous).

Add a column or a permissible value to a schema and the atomic count moves; add a whole new table and the structural checks (C.1–C.5, K.1, K.4, P.7, P.8) all bump.

---

## 7. Function-invocation view (255)

Counted as one `Result` per (table × check function), regardless of internal granularity.

| Pillar | Check | Invocations | Notes |
|---|---|--:|---|
| Conformance | C.1 table_presence | 16 | 1 per beta table |
|  | C.2 required_columns | 16 | |
|  | C.3 column_dtypes | 16 | |
|  | C.4 datetime_format | 16 | |
|  | C.5 categorical_values | 16 | |
|  | C.6 category_group_mapping | 16 | always invoked; emits "no mappings" info on tables without one |
|  | C.7 lab_reference_units | 1 | labs only |
| **Conformance subtotal** | | **97** | |
| Completeness | K.1 missingness | 16 | |
|  | K.2 conditional_requirements | 16 | always invoked; emits info on tables without rules |
|  | K.3 mcide_value_coverage | 16 | |
|  | K.4 relational_integrity | 16 | one per (table × FK col): 13× hospitalization_id + 2× patient_id + 1× organism_id |
|  | K.5 cross_table_conditional_completeness | 1 | the Expired→death_dttm rule; result attaches to `patient`'s completeness bucket (its `target_table`) |
| **Completeness subtotal** | | **65** | |
| Plausibility | P.1 temporal_ordering | 16 | |
|  | P.2 numeric_range_plausibility | 16 | |
|  | P.3 field_plausibility | 16 | |
|  | P.4 medication_dose_unit_consistency | 2 | mac, mai |
|  | P.5 overlapping_periods | 1 | adt |
|  | P.6 category_temporal_consistency | 16 | |
|  | P.7 duplicate_composite_keys | 16 | |
|  | P.8 cross_table_temporal | 10 | non-hosp tables with `hospitalization_id` and time cols |
| **Plausibility subtotal** | | **93** | |
| **GRAND TOTAL** | | **255** | |

---

## 8. Caveats

- Numbers reflect a "perfect" site: every column present, every mCIDE value found, every category represented. Real sites will trigger fewer because some checks emit "not applicable" info messages instead of running.
- Counts shift any time `validation_rules.yaml`, `outlier_config.yaml`, or any `*_schema.yaml` is edited. Re-run the script in §9 to refresh.
- **Schema inconsistencies** worth knowing about:
  - `labs`, `respiratory_support`: more columns flagged `is_category_column: true` than appear in the table-level `category_columns` list. The validator uses the list, so the count reflects the list.
  - `patient_procedures.category_columns: [null]` — a null entry that doesn't match any real column. `check_categorical_values` only counts columns that are both in `category_columns` **and** have `permissible_values`, so C.5 = 0 here; K.3 is also 0. P.6 is also 0 — `check_category_temporal_consistency` hits the "not applicable" early-return (0/0 since clifpy commit `d56bf12`).
  - `code_status` and `crrt_therapy` have no entry in `composite_keys`, so P.7 = 0 for them.
- **K.3 excludes group-column permissible values** by design: 124 permissible values on `med_group`, `mar_action_group`, `organism_group` are not counted (the check only walks columns in `category_columns`, not `is_group_column`).
- **P.6 requires a detectable time column** from `_detect_time_column`'s candidate list (`recorded_dttm`, `admin_dttm`, `admission_dttm`, `lab_result_dttm`, `in_dttm`, `procedure_billed_dttm`, `result_dttm`, `start_dttm`). `patient` (only has `birth_date`/`death_dttm`), `hospital_diagnosis` (no datetime at all), and `microbiology_susceptibility` (no datetime) all get 0.
- The atomic count is a conservative underestimate of "things examined": some checks report messages at finer granularity (e.g., P.6 emits per (cat × value × year) info messages, P.2 reports out-of-range counts per row). Those are not counted as atomic checks here — only the "configured rule slots" the validator iterates over are.

---

## 9. How to reproduce

This script reads only schema YAMLs and prints the per-table breakdown. Re-run it after editing any schema to refresh the doc:

```python
import yaml

BETA = ['patient','hospitalization','adt','vitals','labs','patient_assessments',
        'medication_admin_continuous','medication_admin_intermittent','respiratory_support',
        'position','patient_procedures','code_status','crrt_therapy','hospital_diagnosis',
        'microbiology_culture','microbiology_susceptibility']

schemas = {t: yaml.safe_load(open(f'clifpy/schemas/2.1/{t}_schema.yaml')) for t in BETA}
vrules = yaml.safe_load(open('clifpy/schemas/validation_rules.yaml'))
outliers = yaml.safe_load(open('clifpy/schemas/outlier_config.yaml'))

def cols(t):       return schemas[t].get('columns', [])
def cnames(t):     return {c['name'] for c in cols(t)}
def cat_cols(t):   return schemas[t].get('category_columns') or []
def req(t):        return schemas[t].get('required_columns', [])
def count_ranges(node):
    if not isinstance(node, dict): return 0
    if 'min' in node or 'max' in node: return 1
    return sum(count_ranges(v) for v in node.values())

ck    = vrules.get('composite_keys', {})
fk    = vrules.get('relational_integrity', {})
cond  = vrules.get('conditional_requirements', {})
temp  = vrules.get('temporal_ordering', {})
fp    = vrules.get('field_plausibility_rules', {})
op    = vrules.get('overlapping_periods', {})
ctc   = vrules.get('cross_table_conditional_requirements', [])
ct_time = {
    'adt': ['in_dttm','out_dttm'],
    'labs': ['lab_order_dttm','lab_collect_dttm','lab_result_dttm'],
    'vitals': ['recorded_dttm'], 'respiratory_support': ['recorded_dttm'],
    'medication_admin_continuous': ['admin_dttm'],
    'medication_admin_intermittent': ['admin_dttm'],
    'patient_assessments': ['recorded_dttm'], 'position': ['recorded_dttm'],
    'microbiology_culture': ['order_dttm','collect_dttm','result_dttm'],
    'crrt_therapy': ['recorded_dttm'],
}
TIME_DETECT = {'recorded_dttm','admin_dttm','admission_dttm','lab_result_dttm',
               'in_dttm','procedure_billed_dttm','result_dttm','start_dttm'}

grand = 0
for t in BETA:
    s, cn = schemas[t], cnames(t)
    C1 = 1
    C2 = len(req(t))
    C3 = len(cols(t))
    C4 = len([c for c in cols(t) if c.get('data_type') in ('DATETIME','DATE')])
    C5 = sum(1 for c in cols(t)
             if c['name'] in cat_cols(t) and c.get('permissible_values'))
    C6 = sum(len(v) for k,v in s.items()
             if k.endswith('_category_to_group_mapping') and isinstance(v, dict))
    C7 = len(s.get('lab_reference_units', {})) if t == 'labs' else 0
    K1 = len(req(t))
    # K.2 counts 1 per (rule × then_required col) — matches the granularity
    # the check emits a row for.
    K2 = sum(
        sum(1 for c in rule.get('then_required', []) if c in cn)
        for rule in cond.get(t, [])
    )
    K3 = sum(len(c.get('permissible_values', [])) for c in cols(t)
             if c['name'] in cat_cols(t) and c.get('permissible_values'))
    K4 = sum(1 for fc, r in fk.items()
             if fc in cn and r['references_table'] != t)
    K5 = sum(1 for rule in ctc if rule.get('target_table') == t)
    P1 = len(temp.get(t, []))
    P2 = count_ranges(outliers.get('tables', {}).get(t, {}) or {})
    P3 = len(fp.get(t, []))
    P4 = 1 if t in ('medication_admin_continuous','medication_admin_intermittent') else 0
    P5 = 1 if t in op else 0
    # P.6 counts 1 per (cat col × permissible value) for tables with a
    # detectable time column. Null-entry category_columns (patient_procedures)
    # contribute 0.
    P6 = sum(
        len(c.get('permissible_values', []) or [])
        for c in cols(t)
        if c['name'] in cat_cols(t) and c['name'] in cn
    ) if (cn & TIME_DETECT) else 0
    P7 = 1 if t in ck else 0
    P8 = sum(1 for c in ct_time.get(t, []) if c in cn) if 'hospitalization_id' in cn else 0
    tot = C1+C2+C3+C4+C5+C6+C7+K1+K2+K3+K4+K5+P1+P2+P3+P4+P5+P6+P7+P8
    grand += tot
    print(f'{t:<32} {tot:>5}')

print(f'{"GRAND TOTAL":<32} {grand:>5}')
```

Expected output: `GRAND TOTAL 3363`.

---

## 10. Source files

| File | What it contributes |
|---|---|
| `clifpy/utils/validator.py` | Every `check_*` function (Polars + DuckDB implementations) and the `run_*` orchestrators |
| `clifpy/utils/rule_codes.py` | Rule code → check name mapping (the C.x / K.x / P.x codes) |
| `clifpy/schemas/validation_rules.yaml` | Composite keys, conditional reqs, temporal ordering, FK rules, overlapping periods, cross-table conditional reqs, field plausibility, med dose unit rules |
| `clifpy/schemas/outlier_config.yaml` | Numeric range plausibility (P.2) |
| `clifpy/schemas/{16 beta tables}_schema.yaml` | Column lists, required cols, category_columns, permissible_values, category→group mappings, lab_reference_units |
