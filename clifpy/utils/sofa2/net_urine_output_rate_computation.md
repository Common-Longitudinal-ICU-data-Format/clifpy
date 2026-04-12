# Net Urine Output Rate Computation

Reference documentation for the UO rate pipeline in `_kidney.py::_calculate_uo_score()`.
Adapted from the [MIMIC-IV urine_output_rate.sql](../../docs/urine_output_rate.sql) reference implementation.

## Problem

Urine output is charted as **aperiodic point volumes** (e.g., "150 mL at 11:00"), not as continuous rates. A charted volume represents output accumulated in the collection bag since the last time someone checked. To convert this into mL/kg/h we need:

1. **Total volume** produced over a trailing period (6h, 12h, 24h)

2. **Total observation time** those volumes represent

3. **Patient weight**

Then: `rate = total_volume / weight_kg / observation_hours`

## Pipeline overview

```
Step A   Collect UO events (output − irrigation) with 24h pre-window lookback
Step B   Aggregate net volume per timestamp
Step C   LAG-based observation intervals (tm_since_last_uo)
Step D   Self-join trailing accumulation → 6h/12h/24h volumes + obs times
Step E   ASOF JOIN to weight → mL/kg/h rates (with validity gate)
Step F   Per-window MIN(rate) → UO score 0–3
```

## Worked example

**Setup:** One patient, one 24h scoring window, weight = 70 kg, no irrigation.

### Step A–B: collect UO events and net volume

The `output` table (filtered to `output_group = 'urine'`) for this patient:

| hospitalization_id | recorded_dttm | output_volume |
|----|----|----|
| P1 | 08:00 | 0 |
| P1 | 09:00 | 40 |
| P1 | 12:00 | 120 |
| P1 | 17:00 | 100 |
| P1 | 18:30 | 30 |
| P1 | 20:00 | 25 |

If irrigation existed (e.g., bladder flushes from `input.input_category = 'flush_irrigation_urine'`), those rows would be UNION ALL'd with **negated** volumes and then summed per timestamp. This patient has no irrigation, so `net_output = output_volume`.

The 24h pre-window lookback (`start_dttm - INTERVAL '24 hours'`) includes pre-window measurements so that trailing rates near window start are computable from real data rather than starting cold.

**Output of Step B** (`net_uo`):

| hospitalization_id | start_dttm | end_dttm | recorded_dttm | net_output |
|----|----|----|----|----|
| P1 | 08:00 | 08:00+1d | 08:00 | 0 |
| P1 | 08:00 | 08:00+1d | 09:00 | 40 |
| P1 | 08:00 | 08:00+1d | 12:00 | 120 |
| P1 | 08:00 | 08:00+1d | 17:00 | 100 |
| P1 | 08:00 | 08:00+1d | 18:30 | 30 |
| P1 | 08:00 | 08:00+1d | 20:00 | 25 |

### Step C: observation intervals (LAG)

Each measurement represents output collected **since the previous measurement**. The LAG window function computes this interval. For the first measurement, there is no prior row — the baseline is configurable:

- `window_start` (default): `tm = recorded_dttm − start_dttm` (credits time from window start)

- `first_measurement`: `tm = 0` (conservative — only counts from second measurement)

**Output of Step C** (`with_tm`) — using `baseline = window_start`:

| recorded_dttm | net_output | prev_dttm (LAG) | tm_since_last_uo_min |
|----|----|----|----|
| 08:00 | 0 | *(none)* | 0 min ← `DATEDIFF('min', start=08:00, 08:00)` |
| 09:00 | 40 | 08:00 | 60 min (1h) |
| 12:00 | 120 | 09:00 | 180 min (3h) |
| 17:00 | 100 | 12:00 | 300 min (5h) ← long gap |
| 18:30 | 30 | 17:00 | 90 min (1.5h) |
| 20:00 | 25 | 18:30 | 90 min (1.5h) |

*(hospitalization_id, start_dttm, end_dttm columns omitted for readability — same for all rows)*

### Step D: self-join trailing accumulation

This is the core step. The `with_tm` table is joined **to itself**: each row acts as an **anchor** (`io`), and we find all rows (`iosum`) that precede it within 23 hours.

```sql
FROM with_tm io
LEFT JOIN with_tm iosum
    ON io.hospitalization_id = iosum.hospitalization_id
    AND io.start_dttm = iosum.start_dttm
    AND io.recorded_dttm >= iosum.recorded_dttm
    AND io.recorded_dttm <= iosum.recorded_dttm + INTERVAL '23 hours'
```

For each anchor, conditional SUMs bucket the matched rows into 6h / 12h / 24h trailing windows:

```sql
SUM(CASE WHEN DATEDIFF('hour', iosum.recorded_dttm, io.recorded_dttm) <= 5
    THEN iosum.net_output END) AS uo_vol_6hr
```

#### Why ≤ 5 for a "6-hour" rate?

`DATEDIFF('hour', A, B)` counts how many **clock-hour boundaries** are crossed between A and B. This is the same as `floor(B) - floor(A)` in hours — it is NOT the duration in hours.

| iosum | io (anchor) | DATEDIFF('hour') | Clock distance | In 6h bucket? (≤ 5) |
|----|----|----|----|-----|
| 11:00 | 17:00 | 6 | 6h | **No** (6 > 5) |
| 12:00 | 17:00 | 5 | 5h | **Yes** (5 ≤ 5) |

So the 6h bucket includes measurements up to 5 clock-hour boundaries away. This is the **N-1 convention** from MIMIC: each measurement represents roughly one charting interval of collection, so `≤ 5` captures approximately 6 intervals of data. The real safeguard is the **validity gate** in Step E — a rate is only computed when observation time ≥ 6h, regardless of how many rows entered the bucket.

The same logic applies: `≤ 11` for 12h, `≤ 23` for 24h.

#### Trace: anchor = 17:00 (first anchor with a valid 6h rate)

**Join matches** — every row from `with_tm` checked against anchor 17:00:

| iosum row | DATEDIFF('hour', iosum, **17:00**) | In 6h? (≤ 5) | In 12h? (≤ 11) | net_output | tm_min |
|----|----|----|----|----|----|
| 08:00 | 9 | No | Yes | 0 | 0 |
| 09:00 | 8 | No | Yes | 40 | 60 |
| 12:00 | 5 | **Yes** | Yes | 120 | 180 |
| 17:00 | 0 | **Yes** | Yes | 100 | 300 |

**Bucket sums:**

| Bucket | Rows included | Total vol | Total obs_min | Total obs_hr |
|----|----|----|----|-----|
| 6h (≤ 5) | 12:00, 17:00 | 120 + 100 = **220** | 180 + 300 = 480 | **8.0h** |
| 12h (≤ 11) | 08:00, 09:00, 12:00, 17:00 | 0 + 40 + 120 + 100 = **260** | 0 + 60 + 180 + 300 = 540 | **9.0h** |

Note: the 6h obs time (8.0h) exceeds the ~5h clock span (12:00 → 17:00) because 12:00's `tm = 3h` reaches back to its predecessor (09:00) which is **outside** the 6h bucket. See [Observation time vs. clock span](#observation-time-vs-clock-span).

#### Trace: anchor = 20:00 (worst 6h rate, and only valid 12h rate)

**Join matches:**

| iosum row | DATEDIFF('hour', iosum, **20:00**) | In 6h? (≤ 5) | In 12h? (≤ 11) | net_output | tm_min |
|----|----|----|----|----|----|
| 08:00 | 12 | No | No (12 > 11) | 0 | 0 |
| 09:00 | 11 | No | **Yes** | 40 | 60 |
| 12:00 | 8 | No | **Yes** | 120 | 180 |
| 17:00 | 3 | **Yes** | **Yes** | 100 | 300 |
| 18:30 | 1 | **Yes** | **Yes** | 30 | 90 |
| 20:00 | 0 | **Yes** | **Yes** | 25 | 90 |

**Bucket sums:**

| Bucket | Rows included | Total vol | Total obs_hr |
|----|----|----|-----|
| 6h (≤ 5) | 17:00, 18:30, 20:00 | 100 + 30 + 25 = **155** | (300 + 90 + 90) / 60 = **8.0h** |
| 12h (≤ 11) | 09:00, 12:00, 17:00, 18:30, 20:00 | 40 + 120 + 100 + 30 + 25 = **315** | (60 + 180 + 300 + 90 + 90) / 60 = **12.0h** |

The 12h bucket just barely meets the 12h obs threshold. 08:00 has DATEDIFF = 12 > 11, so it's excluded — but 09:00's `tm = 1h` reaches back to 08:00, making total obs = exactly 12.0h.

#### Trace: anchor = 12:00 (insufficient observation — rate is NULL)

**Join matches:**

| iosum row | DATEDIFF('hour', iosum, **12:00**) | In 6h? (≤ 5) | net_output | tm_min |
|----|----|----|----|----|
| 08:00 | 4 | Yes | 0 | 0 |
| 09:00 | 3 | Yes | 40 | 60 |
| 12:00 | 0 | Yes | 120 | 180 |

**Bucket sums:**

| Bucket | Rows included | Total vol | Total obs_hr | Rate |
|----|----|----|----|-----|
| 6h (≤ 5) | 08:00, 09:00, 12:00 | 160 | (0 + 60 + 180) / 60 = **4.0h** | **NULL** (4h < 6h) |

All three rows pass the DATEDIFF ≤ 5 filter, but the total observation time is only 4h. The validity gate in Step E requires obs ≥ 6h, so no 6h rate is produced. Only 4h has elapsed since window start — we can't assess a 6h rate yet.

#### Complete output of Step D (`trailing_uo`)

| recorded_dttm (anchor) | uo_vol_6hr | uo_tm_6hr | uo_vol_12hr | uo_tm_12hr | uo_vol_24hr | uo_tm_24hr |
|----|----|----|----|----|----|----|
| 08:00 | 0 | 0h | 0 | 0h | 0 | 0h |
| 09:00 | 40 | 1h | 40 | 1h | 40 | 1h |
| 12:00 | 160 | 4h | 160 | 4h | 160 | 4h |
| 17:00 | 220 | 8h | 260 | 9h | 260 | 9h |
| 18:30 | 130 | 6.5h | 290 | 10.5h | 290 | 10.5h |
| 20:00 | 155 | 8h | 315 | 12h | 315 | 12h |

### Step E: weight join and rate calculation

Weight is matched via `ASOF LEFT JOIN` — the most recent `weight_kg` measurement at or before each UO timestamp. This patient's weight = 70 kg throughout.

Rates are computed only when **obs_hr ≥ threshold AND weight > 0** (the validity gate):

| recorded_dttm (anchor) | uo_vol_6hr | uo_tm_6hr | **uo_rate_6hr** | uo_vol_12hr | uo_tm_12hr | **uo_rate_12hr** |
|----|----|----|----|----|----|----|
| 08:00 | 0 | 0h | NULL (0 < 6) | 0 | 0h | NULL |
| 09:00 | 40 | 1h | NULL (1 < 6) | 40 | 1h | NULL |
| 12:00 | 160 | 4h | NULL (4 < 6) | 160 | 4h | NULL |
| 17:00 | 220 | 8h | 220/70/8 = **0.393** | 260 | 9h | NULL (9 < 12) |
| 18:30 | 130 | 6.5h | 130/70/6.5 = **0.286** | 290 | 10.5h | NULL (10.5 < 12) |
| 20:00 | 155 | 8h | 155/70/8 = **0.277** | 315 | 12h | 315/70/12 = **0.375** |

*(24h rates all NULL — max obs time is 12h, needs ≥ 24h)*

### Step F: per-window aggregation and scoring

Take `MIN(rate)` across all anchors for each trailing window (MIN = worst kidney function):

| Metric | Value | At anchor |
|----|----|----|
| min_rate_6hr | **0.277** | 20:00 |
| min_rate_12hr | **0.375** | 20:00 (only valid 12h point) |
| min_rate_24hr | NULL | *(no anchor reached 24h obs)* |

Apply the SOFA-2 scoring cascade:

| Check | Condition | Result |
|----|----|----|
| Score 3 (rate) | min_rate_24hr < 0.3? | NULL → skip |
| Score 3 (anuria) | min_uo_vol_12hr ≤ 0? | 315 > 0 → skip |
| Score 2 | min_rate_12hr < 0.5? | 0.375 < 0.5 → **Score 2** |
| *(Score 1 not reached)* | | |

| Flag | Condition | Result |
|----|----|----|
| has_uo_oliguria | min_rate_6hr < 0.3? | 0.277 < 0.3 → **1** |

Final: `sofa2_kidney = GREATEST(creatinine_score, uo_score=2)` with RRT override to 4.


## Key design decisions

### Observation time vs. clock span

Two different measures of "how long" a trailing bucket covers:

**Clock span** = wall-clock distance from the first included `iosum` row to the anchor `io`.

**Observation time (obs)** = sum of `tm_since_last_uo` for all `iosum` rows in the bucket.

These differ because the first included row's `tm` reaches back to its **predecessor** — which may be outside the bucket. The total telescopes to:

```
obs_time = anchor_time − prev_excluded_time  ≥  clock_span
```

Concrete example from the trace above (6h bucket, anchor = 17:00):

| | Clock span | Observation time |
|----|----|-----|
| Measured between | first included (12:00) → anchor (17:00) | prev excluded (09:00) → anchor (17:00) |
| Value | 5h | 8h |
| Used as rate denominator? | No | **Yes** |

The rate formula uses observation time. This means rates are computed over a slightly broader period than the nominal 6h — but every hour in the denominator corresponds to real elapsed collection time.

### Why no proportional splitting at the boundary

When a measurement enters a trailing bucket, its **entire** volume and observation interval are included — no fractional splitting, even if part of the observation interval extends beyond the bucket boundary.

Proportional splitting would assume urine was produced at a **uniform rate** throughout each collection interval. This assumption fails precisely for the patients we care about — those whose kidney function is *changing*:

- **Kidneys failing mid-interval**: most output happened early, but proportional splitting credits too much to the late (in-bucket) portion, masking the deterioration.

- **Kidneys recovering mid-interval**: most output happened late, but proportional splitting credits too little to the in-bucket portion, missing the recovery.

The atomic approach is honest about what it doesn't know: we have a lump of volume and a time span, with no information about the within-interval distribution. Using both in full avoids injecting a false uniformity assumption.

Additionally, the bucket boundary itself is imprecise — `DATEDIFF('hour')` counts clock-hour boundary crossings, not exact durations — so a precise proportional split against an imprecise boundary would be false precision.

### Why self-join over window functions

SQL window functions like `SUM(...) OVER (RANGE BETWEEN INTERVAL '6 hours' PRECEDING)` can compute trailing sums, but only for one window size per expression. The self-join computes 6h, 12h, and 24h trailing totals simultaneously via conditional `CASE WHEN DATEDIFF(...) <= 5/11/23` in a single pass.

### Validity gate

A rate is only computed when observation time meets the threshold (`uo_tm_6hr >= 6`, etc.) AND weight is positive. This prevents premature scoring early in the window and handles missing weight data.

### First measurement baseline

Configurable via `SOFA2Config.uo_first_measurement_baseline`:

- `'window_start'` (default): first measurement's `tm = recorded_dttm − start_dttm`. Credits unmonitored time from window start. Matches MIMIC convention (uses ICU admission time as baseline).

- `'first_measurement'`: first measurement's `tm = 0`. Conservative — only counts time from the second measurement onward.

### Between-window gap: deterioration that falls between 6h/12h/24h

The algorithm evaluates trailing rates at fixed windows (6h, 12h, 24h). A period of low output can fall "between" these windows and not trigger the expected score.

**Example — high early output buffers the 12h rate:**

```
Weight = 80 kg.  Window 08:00 → 08:00+1d.

Time:    08:00   10:00   12:00   14:00   16:00   18:00   20:00   22:00   00:00+1d
mL:        0     600     500     500      2       1       1       1       0
           │     ─────────────────────   ──────────────────────────────────
           │     massive output (~3.5    near-anuria for 8 hours
           │     mL/kg/h)               (~0.006 mL/kg/h)
```

At anchor 00:00+1d, 12h bucket (DATEDIFF ≤ 11):

| iosum | DATEDIFF | In? | vol | tm |
|----|----|----|----|----|
| 12:00 | 12 | No (12 > 11) | — | — |
| 14:00 | 10 | Yes | 500 | 2h |
| 16:00 | 8 | Yes | 2 | 2h |
| 18:00 | 6 | Yes | 1 | 2h |
| 20:00 | 4 | Yes | 1 | 2h |
| 22:00 | 2 | Yes | 1 | 2h |
| 00:00 | 0 | Yes | 0 | 2h |

vol = 505, obs = 12h → rate = 505/80/12 = **0.526 ≥ 0.5** — not triggered.

Meanwhile, 6h rate at this anchor = (1+1+0)/80/6h = **0.004 < 0.5**.

Result: 6h < 0.5 AND 12h ≥ 0.5 → **Score 1** (oliguria for 6–12h).

**This is correct per the SOFA spec.** The spec asks about *duration of sustained oliguria*, not about finding any arbitrary window ≥ 12h where the average is low. This patient had excellent output for 8h, then 8h of acute near-anuria. They've been oliguric for 8 hours — score 1 ("6–12h") is the right clinical interpretation.

A "16-hour average < 0.3" is not the same as "oliguria for ≥12 hours." The first 8 hours weren't oliguric — they were hyperproductive.

### Sparse data and the "6h rate" misnomer

The "6h rate" is actually "the rate over the observation period accumulated by measurements in the DATEDIFF ≤ 5 bucket." Due to boundary back-reach, this is often **more than 6h**:

```
Time:    08:00          15:00                   23:00
mL:        0             40                       10
tm:       (0)            7h                       8h
```

At anchor 23:00, 6h bucket: only 23:00 enters (DATEDIFF('hr', 15:00, 23:00) = 8 > 5). But 23:00's `tm = 8h` (reaching back to 15:00). obs = **8h** ≥ 6h ✓. Rate = 10/80/8 = **0.0156**.

The algorithm correctly detects this low output despite having only one measurement in the bucket. The boundary back-reach makes it robust to sparse charting — a single measurement with a large gap since the previous one carries enough observation time to pass the validity gate.

**When detection fails:** only when cumulative obs in the bucket is < 6h, which occurs in the first few hours after window start (the patient was just admitted, or the catheter was just placed). In that case, we genuinely lack enough data to assess a 6h rate.

### Why the overall average (and not per-interval checks)

The spec says *"UO < 0.5 mL/kg/h for ≥12h"*. There are three plausible ways to interpret this with aperiodic bucket measurements; we use **interpretation (3)**.

Given measurements at t1, t2, t3, t4 spanning ≥12h:

1. **Every consecutive interval < 0.5** — rates for t1→t2, t2→t3, t3→t4 ALL < 0.5

2. **Every cumulative rate from t1 < 0.5** — rates for t1→t2, t1→t3, t1→t4 ALL < 0.5

3. **Overall rate t1→t4 < 0.5** — total volume / weight / total obs_time < 0.5 (what we implement)

**Concrete example** (weight = 80 kg, 24h period):

```
t1=0h, t2=6h, t3=12h, t4=24h
t1→t2: 5 mL   over 6h   → 0.010 mL/kg/h
t2→t3: 200 mL over 6h   → 0.417 mL/kg/h   ← one moderately high interval
t3→t4: 8 mL   over 12h  → 0.008 mL/kg/h
Total: 213 mL over 24h  → 0.111 mL/kg/h   ← deeply oliguric overall
```

| Interpretation | Check | Result |
|----|----|----|
| (1) every interval < 0.5 | 0.010 ✓, **0.417 ✗**, 0.008 ✓ | **Fail** — one interval breaks it |
| (2) every cumulative from t1 | 0.010 ✓, 0.214 ✓, 0.111 ✓ | Pass |
| (3) overall t1→t4 | 0.111 ✓ | Pass |

Interpretation (1) would miss this clearly oliguric patient because of a single 200 mL stretch.

**Why (1) is too fragile**: a single interval's "rate" is the average over a **variable-length collection period** determined by when the nurse emptied the bag. The same 200 mL over 6h could appear as one moderate interval (rate = 0.417) or as two shorter intervals (e.g., 180 mL in 4h at rate 0.563, then 20 mL in 2h at rate 0.125). Same clinical reality, different scoring outcome under (1).

**Why (2) is too sensitive to the starting point**: the first interval has outsized influence. A borderline or artifact-inflated first reading (residual urine from before the catheter was placed) can invalidate the entire 24h assessment even if the subsequent 23h were deeply oliguric.

**Why (3) works**: treats 24h of output as a single pool — total volume / weight / total obs time. It is:

- **Robust to charting timing**: same total volume gives the same rate regardless of when the nurse checked

- **Robust to single-interval noise**: one artifactual spike doesn't cancel 23h of clear oliguria

- **Robust to the starting artifact**: early residual volume gets diluted over the full period

- **What the clinician actually looks at**: "the patient made 213 mL in 24 hours" — not "intervals 1 and 3 were bad and interval 2 was OK"

The MIN across anchors adds the remaining piece: if there is **any** point during the scoring window where the trailing 24h average is < 0.3, score 3 triggers. This captures the worst 24h stretch even if the patient recovered later in the window.

**The fundamental insight**: UO measurements are not independent samples of a continuous process — they are arbitrary slices of collection, bounded by nursing workflow rather than kidney physiology. Interpretation (3) respects this by smoothing over the arbitrary slice boundaries. Interpretations (1) and (2) treat the slices as physiologically meaningful, which they aren't.

### Integration with creatinine scoring

The UO score (0–3) is combined with the creatinine score via `GREATEST(creat_score, uo_score)`. RRT overrides both to score 4. Footnote p (RRT criteria) includes an oliguria path: `has_uo_oliguria` (rate < 0.3 for > 6h) can substitute for creatinine > 1.2 in the criteria check.

## SOFA-2 UO scoring thresholds

| Score | Criterion | Rate check |
|-------|-----------|------------|
| 0 | Normal output | All rates ≥ 0.5 or insufficient data |
| 1 | UO < 0.5 mL/kg/h for 6–12h | `min_rate_6hr < 0.5` AND (`min_rate_12hr ≥ 0.5` OR `min_rate_12hr IS NULL`) |
| 2 | UO < 0.5 mL/kg/h for ≥ 12h | `min_rate_12hr < 0.5` |
| 3 | UO < 0.3 mL/kg/h for ≥ 24h OR anuria ≥ 12h | `min_rate_24hr < 0.3` OR `min_uo_vol_12hr ≤ 0` |

## Source mapping

| Pipeline step | Code location | MIMIC equivalent |
|---------------|---------------|------------------|
| Step A–B | `_kidney.py:90–129` | `urine_output` derived table (pre-computed) |
| Step C | `_kidney.py:131–149` | `uo_tm` CTE |
| Step D | `_kidney.py:156–184` | `ur_stg` CTE |
| Step E | `_kidney.py:188–203` | Final SELECT with `weight_durations` join |
| Step F | `_kidney.py:207–248` | (not in MIMIC — MIMIC outputs rates, not scores) |
