# SOFA-2 cardiovascular subscore reads the unconverted dose

**Component:** `clifpy/utils/sofa2/_cv.py`
**Severity:** high — produces wrong scores silently, no error, no warning
**Status:** unfixed. Written up standalone so it can be scheduled independently.

This is a defect in one consumer of the unit converter, not in the converter
itself. It is unrelated to the weighted-vs-unweighted policy question, but was
found while investigating it.

## What happens

`_cv.py` converts vasopressor doses to weight-indexed units, then reads the
column it did not write.

```python
# _cv.py:245 -- convert to mcg/kg/min
pressor_events_rel, _ = convert_dose_units_by_med_category(
    med_df=pressor_events_deduped,
    vitals_df=cohort_weights,
    preferred_units=PRESSOR_PREFERRED_UNITS,   # norepinephrine -> mcg/kg/min
    override=True,
    ...
)
```

```sql
-- _cv.py:272 -- but the pivot reads med_dose, not med_dose_converted
PIVOT (
    ANY_VALUE(med_dose)
    FOR med_category IN ('norepinephrine' AS norepi_raw, ...)
```

The converter **adds** `med_dose_converted` and `med_dose_unit_converted`
alongside the originals; it does not overwrite `med_dose`. So the pivot, and
every downstream calculation, uses the dose exactly as charted.

Confirmed by search: neither `med_dose_converted` nor `_convert_status` appears
anywhere under `clifpy/utils/sofa2/`.

## Why it is silent

`override=True` means a row whose conversion fails does not raise. It keeps its
raw value and raw unit and records the reason in `_convert_status` — which
`_cv.py` never inspects.

So there are two overlapping problems:

1. **Every** row uses the unconverted dose, because of the column name.
2. Even had the right column been read, unconvertible rows would silently carry raw values through it.

## The consequence

The CV subscore thresholds are defined in `mcg/kg/min`:

| score | norepinephrine / epinephrine |
|---|---|
| 3 | ≤ 0.1 mcg/kg/min |
| 4 | > 0.1 mcg/kg/min |

A site charting `mcg/min` — RUSH, NU, Sunnybrook and UPenn all do, at ~100% for
these drugs — supplies raw values in the range 2–20. Because `_cv.py` reads
`med_dose` (never converted) rather than `med_dose_converted`, those raw values
are compared directly against the 0.1 threshold, and **every dose scores maximum
vasopressor support**.

For an 80 kg patient a genuine 5 mcg/min is 0.0625 mcg/kg/min, which should
score 3. It scores 4.

Note this is not the converter mislabelling anything. `med_dose_converted` holds
the correct weight-indexed value and `med_dose_unit_converted` names the correct
unit — `_cv.py` simply never reads either column.

Sites charting `mcg/kg/min` natively are unaffected: for them `med_dose` and
`med_dose_converted` are the same number, so the bug is invisible. That is
precisely why it has survived — it only manifests at the four unweighted sites.

## Scale

From the CLIF consortium DQA inventory, observations charted on unweighted units
for drugs in `PRESSOR_PREFERRED_UNITS`:

| drug | unweighted observations | sites affected |
|---|---:|---|
| norepinephrine | 8,625,042 | NU, RUSH, Sunnybrook, UPenn + 6 partial |
| phenylephrine | 2,320,723 | 11 sites, 6 predominantly |
| epinephrine | 1,409,080 | NU, RUSH, Sunnybrook, UPenn + 5 partial |
| isoproterenol | 54,314 | 10 sites |
| dopamine | 4,785 | 8 sites partial |

Any SOFA-2 result computed at an affected site should be treated as suspect
until this is fixed.

## The fix

`CLIF-epi-of-sedation` demonstrates the correct pattern — rename so the working
column *is* the converted one (`code/04_covariates.py`):

```python
cont_veso_converted.rename(columns={
    "med_dose": "med_dose_original",
    "med_dose_unit": "med_dose_unit_original",
    "med_dose_converted": "med_dose",
    "med_dose_unit_converted": "med_dose_unit",
}, inplace=True)
```

Applied to `_cv.py`, after the call at line 245 and before the materialization
at line 254. Everything downstream then works unchanged.

Two things should accompany it:

1. **Inspect `_convert_status`.** Rows that did not convert should be excluded or flagged rather than scored, otherwise fixing the column name still leaves raw values flowing through for unconvertible rows.
2. **A regression test.** Feed a frame of `mcg/min` norepinephrine with known weights and assert the CV subscore matches the hand-computed `mcg/kg/min` value. The current suite cannot catch this because its fixtures are already weight-indexed.

## An adjacent hazard in the same path

Not the cause here, but live in the converter and cheap to fix alongside:

- **`_preferred.py`** — the weight transition factor has no `weight_kg > 0` guard. A zero weight yields `inf` rather than failing. `_kidney.py:266` guards; this does not. Measured at zero occurrences at RUSH and NU, so not currently manifesting, but the guard costs nothing.

(`outlier_config.yaml`'s `weight_kg` floor of 30 kg would discard pediatric
weights, but CLIF ICU cohorts are adult, so it is not a concern in practice.)

## How this was found

While measuring what forcing weight-indexed dose units would cost across sites
(`dev/weighted-unit-impact/`). The investigation needed to know which downstream
consumers depend on the converted column, and `_cv.py` turned out not to use it
at all.

That analysis deliberately excludes SOFA-2 from its own metrics: a measurement
defined through a consumer measures that consumer's implementation as much as
the thing under study — which this defect illustrates well.
