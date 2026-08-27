# VEDBA Calculation Guide: TinyfoxBatt vs Nanofox

## Overview

VEDBA (Vectorial Dynamic Body Acceleration) quantifies overall body movement and activity in bats. Both TinyfoxBatt and Nanofox use identical hardware sampling (28 Hz for 1 second), but **differ fundamentally in how they accumulate and store VEDBA data**. The cleanest way to compare between tag types is to **normalize to VEDBA per sample**, which represents the mean activity per 1-second accelerometer burst.

---

## Fundamental Differences

| Aspect | TinyfoxBatt V13 | Nanofox |
|--------|-----------------|---------
| **Sampling specification** | 28 Hz for 1 second | 28 Hz for 1 second |
| **Sampling interval** | Every 1 minute | Every 2 minutes |
| **Accumulation method** | Cumulative between transmissions | 36-minute windowed sums |
| **Transmission schedule** | 4 messages/day (0, 30, 60, 90 min intervals) | 1 message every 3 hours |
| **Gap to next day** | 22 hours after final message | 3 hours (continuous rolling window) |
| **Data granularity** | 4 interval totals + 1 daily summary | ~40 measurements per day (full/daily) or 8 (location) |

---

## TinyfoxBatt V13 VEDBA Calculation

### Hardware Configuration
```
Accelerometer: ±8G range
Sampling frequency: 28 Hz for 1 second duration
Samples per burst: 28 samples per measurement
LSB (Least Significant Bit): 3.9 mg per unit
Measurement interval: Every 60 seconds (1 minute)
Bursts per day: ~1,440 (one per minute)
```

### Transmission Schedule
```
Message 1: Time 0 min    → VEDBA accumulated in first ~30 min period   (~30 bursts)
Message 2: Time 30 min   → VEDBA accumulated in next ~30 min period    (~30 bursts)
Message 3: Time 60 min   → VEDBA accumulated in next ~30 min period    (~30 bursts)
Message 4: Time 90 min   → VEDBA accumulated in final ~30 min period   (~30 bursts)
           (22-hour gap)
Next day Message 1: Time 0 min → cycle repeats
```

**Note**: Each message carries the cumulative VEDBA **since the previous transmission**, not absolute daily total. The daily aggregate combines all 4 transmissions.

### Calculation Process
1. **Sample acquisition**: Every 60 seconds, the accelerometer records 28 samples at 28 Hz (1 second of continuous data)
2. **VEDBA per sample**: For each of the 28 samples, compute: `VEDBA_sample = √(ax² + ay² + az²) - 1g` 
3. **Burst aggregation**: Sum or average the 28 VEDBA values from this burst
4. **Cumulative tracking**: Add this burst's value to a running total between transmissions
5. **Transmission**: Every 30 minutes (4 times per day), the accumulated VEDBA is sent and counter resets
6. **Daily output**: All 4 transmitted values are summed to produce the daily total

### Available Variables (Daily Level)
```
daily_total_vedba_24h          # Sum of all 4 transmissions over 24 hours (m/s²·hours)
daily_total_vedba_rate_h       # VEDBA rate normalized per hour (m/s²·h⁻¹)
daily_total_vedba_24h_n        # Number of bursts recorded in the 24-hour period (~1,440)
```

**Note on interpretation**: The raw Sigfox messages contain 4 VEDBA values (one per 30-min interval), while Movebank/processed data aggregates these into the daily summary variables above.

### Key Details for Analysis
- **Temporal resolution in raw data**: 4 transmissions per day (every ~30 minutes)
- **Temporal resolution in daily aggregate**: One cumulative value per day
- **Time unit for rate**: Use **hours** for `vedba_rate_h` (already normalized to per-hour)
- **Do NOT use days** when calculating dt for TinyfoxBatt rates—the metric is already annualized
- **Comparison**: Total VEDBA can vary by battery voltage, tag orientation, and individual variation
- **dt per sample**: For rate calculations within a 30-min window, dt = 1 minute = 1/60 hour

---

## Nanofox VEDBA Calculation

### Hardware Configuration
```
Accelerometer: (same as TinyfoxBatt)
Sampling frequency: 28 Hz for 1 second duration
Samples per burst: 28 samples per measurement
Measurement interval: Every 2 minutes (120 seconds)
Bursts per day: ~720 (one per 2 minutes)
Window duration: 36 minutes
Measurements per window: 18 (36 min ÷ 2 min per burst)
Transmission interval: Every 3 hours (8 messages/day)
Scaling factor: 2600 (device-specific)
Conversion to SI units: × 3.9 × 0.00980665
Noise reduction: 0
```

### Calculation Process
1. **Sample acquisition**: Every 2 minutes, record 28 samples at 28 Hz
2. **VEDBA per sample**: For each of the 28 samples, compute: `VEDBA_sample = √(ax² + ay² + az²) - 1g`
3. **Burst aggregation**: Sum the 28 VEDBA values from this burst
4. **Window accumulation**: Every 2-minute burst is added to a 36-minute running window (accumulates 18 bursts)
5. **Window completion**: After 18 bursts (36 minutes), the window sum is recorded and a new window begins
6. **3-hour rollup**: For transmission, sum the last 5 consecutive 36-minute windows (= 180 minutes = 3 hours)

### Conversion Formula
```
VEDBA (m/s²) = vedbaX_raw * 2600 * 3.9 * 0.00980665

Value range: 0 – 25,357.05 m/s²
```

### Available Variables by Data Type

#### **Full Variables** (36-minute resolution)
- `vedba1` – VeDBA sum 0–36 minutes ago (most recent)
- `vedba2` – VeDBA sum 36–72 minutes ago
- `vedba3` – VeDBA sum 72–108 minutes ago
- `vedba4` – VeDBA sum 108–144 minutes ago
- `vedba5` – VeDBA sum 144–180 minutes ago (oldest)
- `vedba_sum` – Rolling 3-hour total (sum of vedba1 through vedba5)

**Note**: `vedba1` represents the *most recent* 36-minute window; older windows are in vedba2–5.

#### **Location Variables** (3-hour resolution)
- `vedba_sum` – Single 3-hour VEDBA sum only

**Use when**: Reduced temporal granularity is acceptable and file size is important.

#### **Daily Variables** (24-hour aggregates)
```
Totals & counts:
  daily_vedba_sum             # Total VEDBA over 24 hours
  daily_vedba_sum_n           # Number of 36-min windows in the day
  daily_vedba_mean            # Mean VEDBA per 36-min window

Activity-based metrics:
  daily_vedba_active_n        # Count of active measurements (36-min windows)
  daily_vedba_flying_n        # Count of flight-identified measurements
  daily_vedba_median          # Median VEDBA (robust to outliers)

Day/night breakdown:
  daily_vedba_day_sum         # Daytime VEDBA sum
  daily_vedba_day_mean        # Daytime VEDBA mean
  daily_vedba_day_median      # Daytime VEDBA median
  daily_vedba_day_n           # Daytime window count
  daily_vedba_day_flying_n    # Daytime flight count
  
  daily_vedba_night_sum       # Nighttime VEDBA sum
  daily_vedba_night_mean      # Nighttime VEDBA mean
  daily_vedba_night_median    # Nighttime VEDBA median
  daily_vedba_night_n         # Nighttime window count
  daily_vedba_night_flying_n  # Nighttime flight count
```

### Key Details for Analysis
- **Temporal resolution**: One value per 36 minutes (full), one per 3 hours (location), one per 24 hours (daily)
- **Time windows**: vedba1–5 represent *consecutive non-overlapping* 36-minute blocks
- **Daily sampling**: Typically ~40 measurements per day (24 hours ÷ 36 min ≈ 40 windows)
- **Burst density**: 18 bursts per 36-minute window = 1 burst every 2 minutes
- **Comparison**: Day/night splits use sunrise/sunset times; values are already segregated

---

## Post-Import Correction (Temporary Workaround)

As of August 2026, Nanofox VEDBA in Movebank export uses **per-sample scaling** (÷504). The import pipeline automatically detects and corrects this to **per-burst scaling** (÷18) on import.

### What Happens During Import

- **Auto-detection**: if median(vedba_sum) < 5 m/s², assumes per-sample scaling (current Movebank data)
- **Correction**: multiplies by 28 (= 504/18) to convert to per-burst
- **Metadata**: column `.vedba_scaling_note` tracks whether correction was applied
  - `"corrected_per_sample_to_per_burst"`: correction was applied
  - `"already_per_burst"`: no correction needed (future Movebank uploads)
- **Transparency**: original per-sample values preserved in `vedba_sum_orig` column

### After Correction — Expected Value Ranges

Values should now represent VEDBA per one-second burst (per-burst scaling, ÷18):

**By activity state:**
- Rest: ~9 m/s² per burst
- Moderate activity: ~27 m/s² per burst
- High activity (flight): ~205 m/s² per burst
- Daily median (typical mixed activity): ~15–25 m/s² per burst

**Daily aggregates:**
- Typical daily range: ~100–2000 m/s² (sum of all bursts, varies by activity level)
- Very active day: >2000 m/s²
- Rest day: <100 m/s²

**Warning signs of incorrect scaling:**
- Median vedba_sum < 5 m/s²: likely still per-sample, correction should have been applied
- Median vedba_sum > 300 m/s²: likely already per-burst, no correction should be needed

### When This Correction Can Be Removed

Once all data is re-uploaded to Movebank with correct scaling, this correction function can be removed from the import pipeline. The auto-detection will then report "already_per_burst" for all data without applying the ×28 correction.

---

## Comparable Units: VEDBA Per Burst

To compare activity across tag types, both are normalized to **VEDBA per one-second burst** (28 Hz recording).

### Nanofox (Normalized to Per-Burst)

**In Movebank:** per-sample scaling (÷504) — auto-corrected on import to per-burst (÷18)

**After import correction:** `vedba_sum / 18` (m/s² per burst)

**Correction factor:** ×28 (applied automatically on import)

**Expected range after correction:**
- Per-burst values: ~0.5–20 m/s²
- Daily totals: ~100–2000 m/s² (varies by activity level)

### TinyfoxBatt V13 (Already Per-Burst)

**Per-message VEDBA:** `tinyfox_diff_vedba` (cumulative diff between messages in m/s²)

**Per-burst VEDBA:** `tinyfox_diff_vedba / (dt_prev_hours × 60)`
- `dt_prev_hours` = time from the previous message to the current message (accounts for ±30 min/day clock drift)
- `tinyfox_diff_vedba` is the change since the previous fix, so it must be paired with the previous inter-message interval
- Already computed in import pipeline as `tinyfox_vedba_rate / 60` (now using `dt_prev`)

**Expected range (per-burst):**
- Per-burst values: ~0.5–2.0 m/s²
- Daily totals: ~1000–2000 m/s² per 24h (varies by activity level)

### Direct Comparison

After normalization, both tag types are on the **same scale**:

- **Nanofox per-burst:** ~0.5–20 m/s² (after ×28 correction)
- **TinyfoxBatt per-burst:** ~0.5–2.0 m/s² (computed from diff/dt)
- **Both represent:** activity level in one 1-second acceleration burst
- **Comparable:** use either the per-burst metric directly or sum to daily/hourly totals

**Note:** Despite similar per-burst ranges, daily totals differ because the tag types have different sampling densities (Nanofox: ~720 bursts/day at 2-min intervals; TinyfoxBatt: ~1,440 bursts/day at 1-min intervals).

---

## Comparing TinyfoxBatt and Nanofox

### Transmission & Accumulation Strategy Comparison

```
Tinyfoxbatt:
TIME (hours):       0     0.5   1.0   1.5   2.0   ... 22.0  22.5  23.0  23.5  24.0
MESSAGE:            TX1   TX2   TX3   TX4         [gap]                    TX1(next)
Sampling (samples): SSSS...SSSS (every 1 min) ...  SSSS...SSSS (every 1 min)

Nanofox:
TIME (hours):       0     3     6     9     12    15    18    21    24
MESSAGE:            TX1   TX2   TX3   TX4   TX5   TX6   TX7   TX8   TX1(next)
Sampling (samples): SSSS...SSSS (every 2 min) ... (continuous rolling window)
```

**S** = 1-second burst at 28 Hz

- **TinyfoxBatt**: ~1,440 bursts/day (1 per minute) → grouped into 4 transmissions → daily total recalculated
- **Nanofox**: ~720 bursts/day (1 per 2 min) → continuously rolled into 36-min windows → 8 transmissions/day

### Normalizing to VEDBA Per Sample (Primary Comparison Method)

The most scientifically sound way to compare between tag types is to **normalize to mean VEDBA per 1-second accelerometer burst**. This removes the effects of different sampling intervals and accumulation strategies, leaving only the fundamental activity intensity.

#### **Daily Comparison (normalized)**

```
Tinyfoxbatt:
  vedba_per_sample = daily_total_vedba_24h / daily_total_vedba_24h_n
                   = daily_total_vedba_24h / ~1440

Nanofox (after correction):
  vedba_per_sample = vedba_sum / (daily_vedba_sum_n * 18)
                   
  where daily_vedba_sum_n = ~40 windows per day
        each window = 18 bursts
        total bursts = ~40 × 18 = ~720
```

#### **Interpretation**
- **vedba_per_sample** = mean activity intensity per 1-second sampling event (m/s²)
- **Values are directly comparable** between tag types
- **Same units** for both: VEDBA accumulated per accelerometer burst
- **Accounts for**: Different sampling densities (1 min vs 2 min) and accumulation strategies automatically

#### **Example Comparison**
```
Tag A (TinyfoxBatt): daily_total_vedba_24h = 14,400 m/s²·h, n = 1,440
                    vedba_per_sample = 14,400 / 1,440 = 10.0 m/s²

Tag B (Nanofox):    daily_vedba_sum = 7,200 m/s², sum_n = 40 windows
                    vedba_per_sample = 7,200 / (40 × 18) = 10.0 m/s²

→ Both tags show equivalent activity intensity despite different raw accumulation!
```

### Data Structure Differences (Raw Values)

| Question | TinyfoxBatt | Nanofox |
|----------|-------------|---------
| How many VEDBA values per day? | 1 (daily aggregate) | ~40 (daily) or ~8 (location) or 5 (full, per message) |
| Time unit for rates? | Hours (vedba_rate_h) | Sums per window (vedba_sum) – must normalize by window count |
| Can I see activity across the day? | No (daily only) | Yes (use full or daily variables with day/night splits) |
| Raw transmission frequency | 4 messages/day | 8 messages/day |
| Temporal granularity of raw data | ~30 minutes per transmission | ~3 hours per transmission |
| Gap between days | 22 hours after final message | 3 hours (rolling window bridges gap) |
| **Normalized comparison unit** | **VEDBA per sample** | **VEDBA per sample** |

### Old Comparison Methods (Not Recommended)

To **compare daily activity totals** (without normalizing):
1. **TinyfoxBatt**: Use `daily_total_vedba_24h` (sum of 4 transmissions)
2. **Nanofox**: Use `daily_vedba_sum` (total VEDBA over 24 hours)

**Caveat**: These are not directly comparable because they accumulate different numbers of bursts. Use only when normalizing by sample count.

### Key Caveats
- **Same sensor hardware**: Both use 28 Hz sampling, but different accumulation strategies
- **Different temporal windows**: TinyfoxBatt resets every 30 minutes (4×/day); Nanofox uses rolling 36-min windows
- **Resolution trade-off**: TinyfoxBatt is simpler but only offers 4 data points per day in raw form; Nanofox provides continuous rolling window data (~40 points/day)
- **Daily gap**: TinyfoxBatt has a 22-hour gap between days; Nanofox's window seamlessly spans midnight
- **Calibration**: Nanofox has a fixed conversion factor; TinyfoxBatt depends on firmware version

---

## Practical Guidance for Analysis

### When Analyzing TinyfoxBatt Data
1. Use **`daily_total_vedba_24h`** for daily totals (combines 4 transmissions)
2. Use **`daily_total_vedba_rate_h`** for activity intensity (already normalized)
3. **Normalize to per-sample**: `daily_total_vedba_24h / daily_total_vedba_24h_n`
4. Check **`daily_total_vedba_24h_n`** for data completeness (expect ~1,440 bursts total)
5. **Do not** apply per-hour scaling to `vedba_rate_h`—it's already annualized
6. Note: Cannot assess fine-grained (intra-day) activity patterns; only 4 transmission points per day
7. **Be aware**: If a tag misses a transmission, that time window's activity is lost

### When Analyzing Nanofox Data
1. Choose data type based on temporal needs:
   - **Full**: When fine-grained (36-min) activity patterns matter
   - **Location**: For migration corridor tracking with minimal data
   - **Daily**: For overall activity summaries and day/night patterns
2. **VEDBA values are now per-burst** after auto-correction on import (÷18, not ÷504)
3. **Normalize to per-sample**: `daily_vedba_sum / (daily_vedba_sum_n * 18)`
4. Use `daily_vedba_sum_n` to validate that a full day's data is present (expect ~40)
5. Leverage **day/night splits** for circadian rhythm analysis
6. Use **`daily_vedba_flying_n`** to filter for actual flight behavior
7. **Advantage**: Rolling window means midnight-spanning activity is captured across the day boundary
8. **Check metadata**: `.vedba_scaling_note` column indicates if correction was applied

### Cross-Type Comparison (RECOMMENDED APPROACH)
1. **Always normalize to VEDBA per sample** (mean VEDBA per 1-second burst)
2. **Formula for TinyfoxBatt**: `daily_total_vedba_24h / daily_total_vedba_24h_n`
3. **Formula for Nanofox** (after auto-correction): `daily_vedba_sum / (daily_vedba_sum_n * 18)`
4. **Compare results directly**—both are now in the same units (m/s² per burst)
5. **Report**: Tag type, raw values, and normalized per-sample values

---

## Summary Table: Variable Mapping

| Analysis Need | TinyfoxBatt | Nanofox (Full) | Nanofox (Daily) |
|---------------|-------------|---|---|
| Daily activity total (raw) | `daily_total_vedba_24h` | Sum of `vedba1:5` | `daily_vedba_sum` |
| Daily activity per sample | `daily_total_vedba_24h / daily_total_vedba_24h_n` | `vedba_sum / (5 × 18)` | `daily_vedba_sum / (daily_vedba_sum_n × 18)` |
| Activity rate (h⁻¹) | `daily_total_vedba_rate_h` | Manual calculation required | `daily_vedba_sum / daily_vedba_sum_n × 1.667` |
| Day/night comparison | Not available | Must manually segment | `daily_vedba_day_*` vs `daily_vedba_night_*` |
| Temporal resolution (daily) | 1 per day | 36 minutes | 24 hours |
| Temporal resolution (raw) | 4 transmissions (~30 min intervals) | 5 windows (~3 hours) or 40 (36-min windows) | N/A |
| Sample interval | 1 minute | 2 minutes | N/A (aggregated) |
| Flight identification | Not directly available | Not directly available | `daily_vedba_flying_n` |

---

## References & Notes

- **Both tags**: Sample at 28 Hz for 1 second per burst (28 samples total)
- **TinyfoxBatt V13**: 
  - Sampling: 1-minute interval → 1,440 bursts/day
  - Transmission: 4 messages/day at 0, 30, 60, 90 min intervals (22-hour gap to next day)
  - Storage: Cumulative totals between transmissions + daily aggregate
  - **Comparison**: Normalize by dividing daily total by burst count (~1,440)
- **Nanofox**: 
  - Sampling: 2-minute interval → 720 bursts/day
  - Transmission: 8 messages/day (every 3 hours)
  - Storage: 36-minute windowed sums grouped into daily aggregates
  - **Comparison**: Normalize by dividing by window count × 18 bursts per window (~720)
  - **Post-import correction**: Auto-detected and applied to convert per-sample (÷504) to per-burst (÷18)
- **LSB conversion**: 1 LSB = 3.9 mg for both systems
- **Gravity offset**: Both remove static 1g component from dynamic acceleration
- **Key insight**: Same hardware, different transmission/accumulation strategies → normalize by sample count for direct comparison
