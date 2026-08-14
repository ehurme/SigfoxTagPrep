# 6-hour sampling generalizability summary

## What was tested
- `R/pressure_wind_profile.R` on synthetic data that mimics a tag sending **one location every 6 h**, where `barometric_pressure` is the **minimum pressure (maximum altitude) in the previous 6-hour window**.
- Config: `pressure_type = "min_6h"`, `window_type = "nightly"`, `interpolation = "linear"`, `interp_timestep_min = 360`.

## Result: the pipeline runs without errors
`explore/test_pressure_wind_profile_6h.R` completes end-to-end after installing the missing `elevatr` package:
- `find_complete_periods()` found **3 qualifying nights**.
- `pressure_wind_profile()` produced **PNG + RDS output for all 3 nights**.
- All core panels rendered: map, elevation profile, wind heatmap, tag-pressure step function, wind-per-level, and wind comparison.

## Key observations

### 1. The nightly window is truncated
`find_complete_periods()` defines a nightly window as **last fix on day N → first fix on day N+1**. With a 6-hour schedule of 20:00 / 02:00 / 08:00, that becomes a **20:00 → 02:00** window. The 08:00 arrival fix is excluded. This means the current default only captures the first half of the night for 6-hour tags.

**Workaround:** manually extend `t_end` to the arrival fix (e.g. 08:00). A quick extension test (20:00 → 08:00) produced **3 fixes** and ran without error, showing the function itself can handle the longer window.

### 2. Interpolation offers little benefit at 6-hour sampling
With `interp_timestep_min = 360` and a 6-hour window, interpolation returns the same number of points as raw fixes (2 points for 20:00→02:00, 3 points for 20:00→08:00). This is expected, but it means interpolation does not recover any within-night dynamics.

### 3. The `min_6h` step pressure panel works as intended
When `pressure_type = "min_6h"`, the tag-pressure panel uses `geom_step()`, so the plot shows a step function that correctly represents the minimum-pressure-over-window semantics.

### 4. Plots are visually sparse
`wind_long` has only 18 rows (9 ERA5 levels × 2 time points per window), so the wind heatmap and line panels are dominated by large blocks/straight lines. The code does not fail, but interpretation becomes coarser.

### 5. Dependency note
`pressure_wind_profile()` requires `elevatr`, which was not installed in this environment. Installing `elevatr` from CRAN (plus dependencies `furrr` and `slippymath`) resolved the error.

## Conclusion on generalizability
The code is **functionally generalizable** to 6-hour location + 6-hour minimum-pressure data: it runs, detects nights, and produces outputs. However, the **nightly window definition in `find_complete_periods()` is the main limitation** for 6-hour tags because it truncates the night at the first morning fix. For 6-hour data you will likely want to either:
- manually extend `t_end` to include the morning arrival fix, or
- adapt `find_complete_periods()` so that the first morning fix is chosen later (e.g. 08:00) when the tag schedule is known to be 6-hourly.

## Files created
- `explore/test_pressure_wind_profile_6h.R` — self-contained synthetic test.
- `explore/tmp_extend_window_6h.R` — quick check of a manually extended window.
- `output/test_wind_profiles_6h/TAG_6H_*.png` and `.rds` — test outputs.
