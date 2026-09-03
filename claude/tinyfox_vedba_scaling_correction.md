# Tinyfox V13P/V14P VEDBA Scaling Correction

## Background

Tinyfox tags accumulate VEDBA (Vectorial Dynamic Body Acceleration) in a cumulative counter and transmit the running total at regular intervals. The per-message change (`tinyfox_diff_vedba`) divided by the inter-message interval gives a per-burst activity rate. In theory this rate should be directly comparable between all Tinyfox firmware versions because they use the same 28 Hz, 1-second accelerometer bursts.

In practice, some V13P and V14P firmware batches report cumulative values that are scaled roughly **10× higher** than V13 firmware. This was first observed in the `ExploreTinyFox` dataset, where post-fall-off (stationary) per-burst VEDBA was:

| firmware | stationary median (m/s² per burst) |
|---|---|
| V13  | ~0.24 |
| V13P | ~2.48 |
| V14P | ~2.40 |

The same comparison in the older `CommonNoctuleSpringMigration` dataset did **not** show this inflation: its V13P stationary baseline was ~0.25 m/s² per burst, essentially identical to V13. This means the offset is **firmware-batch and/or dataset-specific**, and should be detected from the data rather than hard-coded.

## Detection method

The SigfoxTagPrep import pipeline uses the **stationary baseline** to detect the scaling offset.

### Stationary rows

1. Primary: `tag_fell_off == TRUE` rows (computed by `.detect_tag_fell_off_loc()`).
2. Fallback: rows where `tinyfox_activity_percent_last_24h == 0`.

If fewer than three stationary rows are available for a firmware, the function falls back to the 5th percentile of all non-stationary per-burst values for that firmware.

### Per-burst proxy

The pipeline already computes:

```r
tinyfox_vedba_rate = tinyfox_diff_vedba / dt_prev_hours
```

Per-burst VEDBA (m/s² per 1-second burst) is:

```r
tinyfox_vedba_per_burst = tinyfox_vedba_rate / 60
```

### Correction trigger

For each affected firmware (V13P, V14P):

```r
ratio = baseline_v13p_or_v14p / baseline_v13
if (ratio > threshold_ratio) {   # default 2.0
  factor = round(ratio, 1)
} else {
  factor = 1.0  # no correction
}
```

If V13 is absent from the dataset, auto-detection is skipped and the note is set to `"unknown_no_v13_reference"`. Users can then supply `tinyfox_v13p_v14p_force_factor` if they know the correct factor.

## Correction

When triggered, the following columns are divided by the computed factor for V13P/V14P rows:

- `tinyfox_total_vedba`
- `tinyfox_diff_vedba`
- `tinyfox_vedba_rate`

Original values are preserved:

- `tinyfox_total_vedba_orig`
- `tinyfox_diff_vedba_orig`
- `tinyfox_vedba_rate_orig`

Metadata columns are added:

- `.tinyfox_scaling_factor`: numeric factor applied to each row (1.0 = no change)
- `.tinyfox_scaling_note`: status string

## Metadata values

| Value | Meaning |
|---|---|
| `"corrected_v13p_to_v13"` | V13P scaled down to match V13 baseline |
| `"corrected_v14p_to_v13"` | V14P scaled down to match V13 baseline |
| `"already_v13_scale"` | No correction needed / V13 row |
| `"not_v13p_v14p"` | V13 row or non-Tinyfox row |
| `"unknown_no_stationary_data"` | Not enough stationary data for this firmware |
| `"unknown_no_v13_reference"` | V13 reference missing; factor could not be determined |
| `"forced_factor_v13p"` / `"forced_factor_v14p"` | User-supplied `force_factor` applied |

## Pipeline order

```r
1. .correct_vedba_scaling(b_loc)        # Nanofox
2. .correct_sensor_vedba_scaling(b)     # Nanofox sensor
3. .add_tinyfox_diff_vedba(b_loc)        # Tinyfox diff/rate
4. .detect_tag_fell_off_loc(b_loc)       # stationary baseline
5. .correct_tinyfox_vedba_scaling(b_loc) # NEW: Tinyfox firmware scaling
6. propagate corrected columns to b and b_daily2
```

Tag-fall-off detection must run before the Tinyfox scaling correction because the correction relies on the stationary baseline.

## Manual override

If a dataset contains only V13P/V14P (no V13 reference), auto-detection cannot determine the factor. In that case, set the parameter in `import_nanofox_movebank()`:

```r
import_nanofox_movebank(
  study_id = ...,
  tinyfox_v13p_v14p_force_factor = 10
)
```

This bypasses auto-detection and divides all V13P/V14P values by 10.

## Cross-firmware comparison

After the correction:

| Tag / firmware | Representative per-burst VEDBA (m/s²) | How to compute |
|---|---|---|
| Tinyfox V13 | ~0.2–0.3 | `tinyfox_vedba_rate / 60` |
| Tinyfox V13P/V14P (corrected) | ~0.2–0.3 | `tinyfox_vedba_rate / 60` (after correction) |
| Nanofox 30Days / 30DaysFineScalePressure | ~10–30 median per window, or per-burst = `vedba_sum / 90` | `vedba_sum / (5 windows × 18 bursts)` |

For daily comparison:

- **Tinyfox**: `daily_total_vedba_24h / daily_total_vedba_24h_n` (≈ 1,440 bursts/day for V13, similar for corrected V13P/V14P)
- **Nanofox**: `daily_vedba_sum / (daily_vedba_sum_n × 18)` (≈ 720 bursts/day)

Always check the metadata columns (`.tinyfox_scaling_note`, `.vedba_scaling_note`) before comparing datasets to ensure both have been normalized.

## When to expect correction

- **ExploreTinyFox (2024–2026, V13P/V14P)**: expect factor ≈ 10.
- **CommonNoctuleSpringMigration (2024 V13P only)**: expect factor ≈ 1.0 (no correction).
- **Mixed V13 + V13P/V14P studies**: correction factor estimated from the data.
- **V13P/V14P-only studies**: auto-detection returns `"unknown_no_v13_reference"`; use `force_factor` if needed.

## References

- Implementation: `.correct_tinyfox_vedba_scaling()` in `R/import_nanofox_movebank.R`
- Related: `claude/vedba_calculation_guide.md`
