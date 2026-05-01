# Test Update Memo — `unit_converter` weight-aware redesign

> **Audience**: human reviewer manually updating fixtures + tests in this directory.
> **Author**: AI agent implementing the upstream code changes in `clifpy/utils/unit_converter.py`.
> **Status**: code changes pending. Do NOT trust this directory's tests until both code and fixtures are updated and the test runs all green.

---

## 1. What's changing in the code (in plain English)

The current pipeline always routes weighted units (`mcg/kg/hr`, `mcg/lb/min`, etc.) through an **unweighted base unit** (`mcg/min`). To do that it multiplies by `weight_kg` in stage 1 and divides by `weight_kg` in stage 2 — which (a) loses precision via float roundtrip and (b) fails when `weight_kg` is missing, even when both ends of the conversion share the same weight qualifier and weight isn't actually needed.

The new pipeline:

- **Stage 1 (`clean → base`) preserves the weight qualifier.** `mcg/kg/hr` now goes to base `mcg/kg/min` (not `mcg/min`). Stage 1 only normalizes amount and time; it does not touch `weight_kg`.

- **Stage 2 (`base → preferred`) handles the weight transition with a 9-case factor.** Same weight qualifier on both sides → factor 1, no weight needed. `kg ↔ lb` → constant factor 2.20462, no weight needed. Adding/removing a weight qualifier → multiply or divide by `weight_kg` (this is the only case where patient weight is required).

- **`_needs_wt` column added.** Computed from `(_base_unit, _preferred_unit)`. It's 1 only when source and target differ in *presence* of a weight qualifier. The orchestrator uses this to do a lazy weight join — only `_needs_wt = 1` rows go through `find_most_recent_weight`.

- **`fallback_on_earliest: bool = False`** new parameter on `find_most_recent_weight` and `convert_dose_units_by_med_category`. When True, rows whose ASOF returns NULL (med admin precedes the first charted weight) fall back to the earliest weight for that hospitalization. New `_weight_source` column ('asof' / 'earliest_fallback' / NULL) for auditability.

- **User-prefilled `weight_kg` is honored exactly.** If `'weight_kg' in med_df.columns`, our internal lookup is skipped entirely. NULLs in user-supplied weight stay NULL (no second-guessing).

- **End-to-end `DuckDBPyRelation`.** No mid-pipeline `.to_df()`. `set(...to_df()...)` validation calls become anti-join + `.fetchall()`. The `_needs_wt` gate uses `.fetchone()`. Final pandas materialization is gated by `return_rel`. Following `docs/duckdb_perf_guide.md`: replacement scans (no `.register()`), `CREATE TEMP TABLE` for the input cohort (boundary 2a) and any relation referenced more than once (boundary 2b), `UNION ALL BY NAME` for the lazy weight join, `SEMI JOIN` over `WHERE x IN (SELECT ...)`.

---

## 2. Inventory of this directory & what each file needs

(`tests/utils/med_unit_converter/`)

| File                                                             | Lines | Rows | Update scope |
|------------------------------------------------------------------|-------|------|--------------|
| `_clean_dose_unit_formats_test_data.csv`                         | 17    | 16   | **UNCHANGED**. Format cleaning is untouched. |
| `_clean_dose_unit_names_test_data.csv`                           | 43    | 42   | **UNCHANGED**. Name cleaning is untouched. |
| `_convert_clean_units_to_base_units_test_data.csv`               | 25    | 24   | **TARGETED**. ~13 weighted rows: `_base_unit` keeps weight qualifier; `_base_dose` no longer multiplied by `weight_kg`. |
| `test_unit_converter - standardize_dose_to_base_units.csv`       | 972   | 971  | **BULK**. Same edit as above, at scale. Every row matching `WEIGHT_REGEX` in `_clean_unit`. |
| `test_unit_converter - convert_dose_units_by_med_category.csv`   | 992   | 991  | **LARGEST**. `_base_unit` and `_base_dose` shifts as above + `_convert_status` two-message split + new `_needs_wt` column + APPEND new rows for 14 scenarios. |
| `vitals_weights.csv`                                             | 4     | 3    | **OPTIONAL ADDITION** for the `fallback_on_earliest` integration test. |
| `test_unit_converter.py`                                         | —     | —    | **PATH FIX + new test stubs** (see §6). |

**Suggested order** (smallest blast radius first):

1. File 3 (24 rows) — fastest to spot-check by hand. Builds your mental model.
2. File 4 (971 rows) — same edit pattern at scale.
3. File 5 (991 rows + new) — apply the pattern, then split `_convert_status` messages, then add `_needs_wt`, then append new rows.
4. File 6 (optional) — only if you want the fallback integration test.
5. File 7 — fix path, add new test stubs.

I can hand you a one-liner Python script to derive the new `_base_unit` / `_base_dose` / `_needs_wt` columns from `_clean_unit` and `_preferred_unit` once the code lands, so the bulk edits are computable rather than hand-typed. Just ask.

---

## 3. Concrete examples — what changes in each row

### 3a. `_convert_clean_units_to_base_units_test_data.csv`

Current rows (selected):

```
rn,case,med_dose,_clean_unit,weight_kg,_base_dose,_base_unit,note
0,valid,6,mcg/kg/hr,70,7,mcg/min,test variation of the /hr unit
4,valid,6,mcg/kg/min,74,444,mcg/min,test variation of the /min unit
5,valid,6,u/kg/hr,75,7.5,u/min,test variation of the unit amount
13,valid,6,mcg/lb/hr,83,18.298346,mcg/min,test lb is acceptable
```

After redesign:

```
rn,case,med_dose,_clean_unit,weight_kg,_base_dose,_base_unit,note
0,valid,6,mcg/kg/hr,70,0.1,mcg/kg/min,test variation of the /hr unit
4,valid,6,mcg/kg/min,74,6,mcg/kg/min,test variation of the /min unit
5,valid,6,u/kg/hr,75,0.1,u/kg/hr → keeps /kg in base; only time normalized   (NOTE corrected expected: 6/60 = 0.1; _base_unit = u/kg/min)
13,valid,6,mcg/lb/hr,83,0.1,mcg/lb/min,test lb is acceptable; weight is unused
```

(Re-do the math for every weighted row: `_base_dose = med_dose × amount_factor × time_factor`. No `weight_kg` factor.)

### 3b. `test_unit_converter - standardize_dose_to_base_units.csv`

Same transformation rule across all 971 rows. Notably one *changed semantic*: row `rn=4` (the previously-`invalid` `'cannot standardize a weighted_unit if weight is missing'` case) now becomes **valid** — base conversion no longer needs `weight_kg`. Update the `case` column for any such rows from `invalid` → `valid` and clear or update the `note`.

### 3c. `test_unit_converter - convert_dose_units_by_med_category.csv`

Apply the stage-1 edits from above first. Then:

- **`_convert_status` message split.** Replace
  - `'cannot convert to a weighted unit if weight_kg is missing'`

  with one of the two more specific messages depending on direction:
  - Source weighted, target unweighted: `'cannot convert weighted to unweighted: weight_kg is missing'`
  - Source unweighted, target weighted: `'cannot convert unweighted to weighted: weight_kg is missing'`

- **Add `_needs_wt` column** (1 / 0). The formula:
  - `_needs_wt = 1` iff exactly one of `_base_unit` / `_preferred_unit` contains `/kg/` or `/lb/` (i.e., XOR of weight-qualifier presence).
  - Otherwise `_needs_wt = 0`.

- **Same-class row review.** Look for rows where the previous `_convert_status` was a `'cannot convert ...'` flavor that's now successful. Example:
  - `mcg/kg/hr → mcg/kg/min` with `weight_kg = NULL`: previously failed in stage 1, now succeeds with `med_dose_converted = med_dose / 60`, `_convert_status = 'success'`. Update both `med_dose_converted` and `_convert_status`.

- **Append new rows for the 14 scenarios in §4.**

### 3d. `vitals_weights.csv` (optional fallback test)

Currently 3 rows. To exercise `fallback_on_earliest=True`, add a hospitalization where the *earliest* weight is recorded **after** the first med admin time. The corresponding new row in `convert_dose_units_by_med_category.csv` should test that with `fallback_on_earliest=False` the conversion fails (no prior weight), and with `fallback_on_earliest=True` it succeeds and `_weight_source = 'earliest_fallback'`.

---

## 4. New test scenarios to append (rows in `convert_dose_units_by_med_category.csv`)

All rows below should round-trip correctly under the new design. Use unique `rn` values continuing from the current max.

| #  | Scenario                                  | `med_dose_unit` | preferred (per `med_category`) | `weight_kg` | Expected `med_dose_converted`        | `_needs_wt` | Expected `_convert_status` |
|----|-------------------------------------------|-----------------|--------------------------------|-------------|--------------------------------------|-------------|----------------------------|
| 1  | Identity, weighted                        | `mcg/kg/min`    | `mcg/kg/min`                   | NULL        | `dose`                               | 0           | `success`                  |
| 2  | Identity after cleaning                   | `mcg/kg/minute` | `mcg/kg/min`                   | NULL        | `dose`                               | 0           | `success`                  |
| 3  | Weighted, time-only                       | `mcg/kg/hr`     | `mcg/kg/min`                   | NULL        | `dose / 60`                          | 0           | `success`                  |
| 4  | Weighted, amount-only                     | `mg/kg/min`     | `mcg/kg/min`                   | NULL        | `dose × 1000`                        | 0           | `success`                  |
| 5  | Weighted, amount + time                   | `mcg/kg/min`    | `mg/kg/hr`                     | NULL        | `dose × 0.06`                        | 0           | `success`                  |
| 6  | kg → lb                                   | `mcg/kg/min`    | `mcg/lb/min`                   | NULL        | `dose / 2.20462`                     | 0           | `success`                  |
| 7  | lb → kg                                   | `mcg/lb/min`    | `mcg/kg/min`                   | NULL        | `dose × 2.20462`                     | 0           | `success`                  |
| 8  | kg → lb, mixed amount/time                | `mg/kg/hr`      | `mcg/lb/min`                   | NULL        | `dose × 1000 / 60 / 2.20462`         | 0           | `success`                  |
| 9  | Weighted → unweighted, weight present     | `mcg/kg/min`    | `mcg/min`                      | 70          | `dose × 70`                          | 1           | `success`                  |
| 10 | Weighted → unweighted, weight NULL        | `mcg/kg/min`    | `mcg/min`                      | NULL        | `dose` (fallback)                    | 1           | `'cannot convert weighted to unweighted: weight_kg is missing'` |
| 11 | Unweighted → weighted, weight present     | `mcg/min`       | `mcg/kg/min`                   | 70          | `dose / 70`                          | 1           | `success`                  |
| 12 | Unweighted → weighted, weight NULL        | `mcg/min`       | `mcg/kg/min`                   | NULL        | `dose` (fallback)                    | 1           | `'cannot convert unweighted to weighted: weight_kg is missing'` |
| 13 | Same-class identity, unweighted           | `mcg/min`       | `mcg/min`                      | NULL        | `dose`                               | 0           | `success`                  |
| 14 | Amount unit identity                      | `mg`            | `mg`                           | NULL        | `dose`                               | 0           | `success`                  |

**Notes:**

- For #2, the cleaning step normalizes `mcg/kg/minute` → `mcg/kg/min`, which is identical to the preferred unit. The identity short-circuit in stage 2 returns `med_dose` exactly (bit-equal to input).

- For #6 and #7, the constant factor 2.20462 should be defined as a single literal in the code — make sure your expected values match the implementation's literal. If you want bit-exact tests, document the literal in the fixture's `note` column.

- For #10 and #12, `med_dose_converted` falls back to the **input dose**, NOT to `_base_dose`. Verify this matches the fallback fields chosen by the implementation (`_clean_unit` / `med_dose` if available, else `_base_unit` / `_base_dose`).

---

## 5. Tests that should remain passing throughout my work (unchanged)

These cover code paths I'm not modifying:

- `test__clean_dose_unit_formats_duckdb`
- `test__clean_dose_unit_names_duckdb`
- `test__acceptable_rate_units`

I'll run only these (and ad-hoc smoke tests against my new code) to verify my work-in-progress without touching the under-update fixtures.

## 6. Tests that need updating in `test_unit_converter.py`

### 6a. Path fix (mandatory — fixtures moved)

```python
# Before (lines 34-38):
def _load(filename) -> pd.DataFrame:
    path = Path(__file__).parent.parent / 'fixtures' / 'unit_converter' / filename
    df = pd.read_csv(path)
    return df

# After (fixtures now colocated with the test file):
def _load(filename) -> pd.DataFrame:
    path = Path(__file__).parent / filename
    df = pd.read_csv(path)
    return df
```

### 6b. Existing tests to refresh (no logic change, just may need new imports)

- `test__convert_clean_units_to_base_units` — keep, will pass once File 3 is updated.
- `test_standardize_dose_to_base_units` — keep, will pass once File 4 is updated.
- `test__convert_base_units_to_preferred_units_inverse` — keep, will pass once File 4 is updated.
- `test__convert_base_units_to_preferred_units_new` — keep, will pass once File 5 is updated.
- `test_convert_dose_units_by_med_category` — keep, will pass once File 5 is updated.
- `test_convert_dose_units_by_med_category_return_rel` — keep, may need to assert `_needs_wt` in `result_df.columns`.

### 6c. New tests to add

- `test__needs_wt_column_present_and_correct` — assert `_needs_wt` column exists and matches expected values from File 5.
- `test_no_vitals_join_when_not_needed` — `med_df` without `weight_kg`, `vitals_df=None`, all conversions weight-compatible. Assert no error and correct results.
- `test_partial_vitals_join` — half rows weight-compatible, half need weight. Assert ASOF only fires for the needing-weight subset.
- `test_user_prefilled_weight_kg_honored` — pre-filled `weight_kg` (mix of values + NULLs), `vitals_df=None`. Assert lookup skipped, NULLs preserved.
- `test_fallback_on_earliest_recovers_lagged_charting` — fixture where med admin precedes first weight chart time. With `fallback_on_earliest=False`, fails; with `True`, succeeds and `_weight_source = 'earliest_fallback'`.
- `test_identity_short_circuit_bit_exact` — non-integer `weight_kg` (e.g., 73.4836), `mcg/kg/min → mcg/kg/min`, assert `med_dose_converted == med_dose` exactly (no float drift).

---

## 7. Workflow

1. I implement the code changes in `clifpy/utils/unit_converter.py`.
2. I run **only** the unchanged tests from §5 + ad-hoc smoke tests against my code.
3. I post a summary of the code changes here when done.
4. You manually update the fixtures and tests per §3 / §4 / §6.
5. You signal completion. I run the full test suite to verify correctness end-to-end.

If at any point you'd prefer me to generate the `_base_dose` / `_needs_wt` derivations as a helper script (so the bulk fixture edits are computable rather than hand-typed), say the word. I'll write it as a one-shot file under `dev/` that reads the old fixture and writes the new one.
