# VEDBA Import Code Review

## Overview

This document tracks implementation status of VEDBA scaling corrections in the SigFox tag prep R code, specifically for Nanofox and TinyFoxBatt per-burst normalization.

---

## Implementation Status

### Gap #1: TinyFoxBatt per-message VEDBA rate
**Status:** IMPLEMENTED  
**Details:** Temporal normalization of TinyFoxBatt VEDBA diffs by the **previous** inter-message interval (`dt_prev`).  
**Location:** `.add_tinyfox_diff_vedba()` in `R/import_nanofox_movebank.R` (lines ~1830–1885)  
**What it does:**
- Computes `tinyfox_diff_vedba`: raw counter change between consecutive fixes
- Computes `tinyfox_vedba_rate`: normalized by `dt_prev_hours` for comparable activity signals
- Handles counter resets and overflow gracefully

**Important:** `tinyfox_diff_vedba` is the change *since the previous fix*, so it must be divided by the time from the previous fix to the current fix (`dt_prev`), not the forward interval `dt`. Using `dt` inflates rates when the forward interval is short (e.g., the next message arrives quickly) and can produce extreme per-burst spikes that make Tinyfox and Nanofox values incomparable.

---

### Gap #2: NanoFox per-window VEDBA aggregation
**Status:** IMPLEMENTED  
**Details:** Existing Movebank download already provides `vedba_sum` (sum of 5 windows × 18 bursts = 90 samples).  
**Location:** `.add_location_sensor_metrics()` in `R/import_nanofox_movebank.R` (lines 1713–1812)  
**What it does:**
- Aggregates VeDBA sensor rows (per-sample VEDBA values) onto location rows
- Computes sum of all sensor values at each transmission timestamp
- Returns `vedba_sum` on location rows

---

### Gap #3: VEDBA per-burst normalization [MITIGATED]

**Previous Status:** CRITICAL  
**Current Status:** MITIGATED (temporary workaround in place)

**Problem:**
- Nanofox VEDBA in Movebank export is scaled as per-sample average (÷504 samples per window)
- Should be scaled as per-burst average (÷18 bursts per window)
- Results in values ~28× too small (504/18 = 28)
- Makes direct comparison with TinyfoxBatt impossible without manual correction

**Solution Implemented:** Post-Import Correction Function
- ✅ **Auto-detection:** Checks if median(vedba_sum) < 5 m/s² (per-sample signature)
- ✅ **Automatic correction:** Multiplies by 28 (= 504/18) to convert to per-burst
- ✅ **Nanofox:** Import pipeline now produces correct per-burst VEDBA (÷18, not ÷504)
- ✅ **Metadata tracking:** Column `.vedba_scaling_note` indicates correction status
- ✅ **Data transparency:** Original per-sample values preserved in `vedba_sum_orig`
- ✅ **TinyfoxBatt:** Per-burst computation documented for future implementation

**Implementation Details:**
- **Function:** `.correct_vedba_scaling()` in `R/import_nanofox_movebank.R` (lines 1870–1957)
- **Called after:** `.add_location_sensor_metrics()` when vedba_sum is first computed
- **Detection threshold:** median(vedba_sum) < 5 m/s²
- **Correction factor:** 504 / 18 = 28
- **Metadata columns added:**
  - `vedba_sum_orig`: original per-sample values
  - `.vedba_scaling_note`: "corrected_per_sample_to_per_burst" or "already_per_burst"

**Expected Value Ranges After Correction:**
- Rest: ~9 m/s² per burst
- Moderate activity: ~27 m/s² per burst
- Flight/high activity: ~205 m/s² per burst
- Daily median (mixed activity): ~15–25 m/s² per burst

**Future Path:**
- ✅ Currently: Auto-correct on import, work with correct data
- 🔄 Later: Re-upload all data to Movebank with correct scaling
- Eventually: Remove correction function from import_nanofox_movebank.R (will become unnecessary)

**Status Report:** During import, the pipeline now reports:
```
[VEDBA scaling] Status report:
  corrected_per_sample_to_per_burst: NNN rows
  already_per_burst: NNN rows (if any future Movebank uploads use correct scaling)
```

---

### Gap #4: NanoFox raw sensor-level `vedba` correction
**Status:** IMPLEMENTED  
**Details:** Raw sensor `vedba` values in the full multi-sensor object are per-sample averages (÷504). They are now auto-corrected to per-burst units (×28) during import.  
**Location:** `.correct_sensor_vedba_scaling()` in `R/import_nanofox_movebank.R` (after `.correct_vedba_scaling()`)  
**What it does:**
- Detects per-sample scaling using the same median < 5 m/s² threshold
- Multiplies `full$vedba` by 28 so each row represents one 1-second burst
- Preserves original values in `vedba_orig`
- Adds metadata column `.vedba_sensor_scaling_note`

**Why:** This makes `full$vedba` directly comparable to `location$vedba_sum` (after dividing by burst count) and to Tinyfox per-burst VEDBA, without requiring a separate post-processing script.

---

## Remaining Considerations

### TinyFoxBatt Per-Burst Computation (Optional Enhancement)
**Status:** DOCUMENTED, not yet implemented  
**Reference:** Comment block in `R/import_nanofox_movebank.R` (after `.correct_vedba_scaling()`)

To compute per-burst VEDBA for TinyFoxBatt when needed:
```r
tinyfox_per_burst_vedba = tinyfox_diff_vedba / (dt_prev_hours × 60)
```
Where `dt_prev_hours` is already available from `.add_tinyfox_diff_vedba()` (it now uses `dt_prev`, not `dt`).

### Movebank Upload Pipeline (`wildcloud_to_movebank.R`)
**Status:** Not yet updated  
**Future work:** When data is re-uploaded to Movebank with correct scaling:
1. Update wildcloud_to_movebank.R to use `sampling_count = 18` (not 504)
2. Re-export and re-upload all studies to Movebank
3. Remove `.correct_vedba_scaling()` from import_nanofox_movebank.R
4. Update this document to mark correction as historical

---

## Code Review Checklist

### Implementation Quality
- [x] `.correct_vedba_scaling()` function is syntactically correct R code
- [x] Correction factor math verified: 504/18 = 28 ✓
- [x] Column names use proper quoting/naming conventions (`.vedba_scaling_note`, `vedba_sum_orig`)
- [x] Metadata tracking implemented (`.vedba_scaling_note` column added)
- [x] Original values preserved for transparency/reversibility (`vedba_sum_orig`)
- [x] Function called at correct pipeline stage (after vedba_sum computed)
- [x] Error handling: safe via .safe_try() wrapper in pipeline
- [x] Reporting: status summary logged to console with message()

### Testing Recommendations
1. **Auto-detection test:**
   - Verify function identifies current Movebank data as per-sample (median < 5)
   - Verify correction factor ×28 applied
   - Check status report appears in console output

2. **Corrected data validation:**
   - Median vedba_sum should be ~15–25 m/s² (after correction, before: ~0.5–1.0)
   - `.vedba_scaling_note = "corrected_per_sample_to_per_burst"` on corrected rows
   - `vedba_sum_orig` contains original per-sample values
   - No duplicate rows or data loss

3. **Daily metrics check:**
   - Daily vedba_sum should range ~100–2000 m/s² (realistic activity variation)
   - Rest days: <100 m/s²
   - Active days: >1000 m/s²

4. **Cross-type comparison test:**
   - Both Nanofox (corrected) and TinyfoxBatt per-burst should be on similar scale
   - TinyfoxBatt rate should use `dt_prev` (not `dt`) so extreme forward-dt spikes disappear
   - Daily totals should be in comparable range when normalized by burst count

---

## Documentation Updates

### Updated in `claude/vedba_calculation_guide.md`
- [x] Added "Post-Import Correction (Temporary Workaround)" section
- [x] Updated "Comparable Units: VEDBA Per Burst" section with correct value ranges
- [x] Added expected ranges after correction (~10–200 m/s²)
- [x] Documented metadata column meanings
- [x] Added guidance on using `.vedba_scaling_note` and `vedba_sum_orig`

### Updated in `R/import_nanofox_movebank.R`
- [x] Function documentation added to `.correct_vedba_scaling()`
- [x] Details section added to main function docs explaining correction
- [x] Call site documented with inline comments
- [x] Status reporting added (scaling summary logged at end of processing)
- [x] TinyFoxBatt per-burst reference comment added (for future work)
- [x] `.add_tinyfox_diff_vedba()` updated to use `dt_prev` instead of `dt` for `tinyfox_vedba_rate`
- [x] Inline docs for `.add_tinyfox_diff_vedba()` updated to explain `dt_prev` usage
- [x] `.correct_sensor_vedba_scaling()` added to correct raw `full$vedba` to per-burst units
- [x] Main `import_nanofox_movebank()` roxygen docs updated to describe sensor-level correction

---

## References

- **VEDBA scaling issue root cause:** Movebank export uses per-sample averaging (÷504) instead of per-burst (÷18)
- **Correction implementation:** Post-import function auto-detects and corrects scaling
- **Timeline:** August 27, 2026 — implementation date
- **Original instructions:** `/tmp/SigfoxTagPrep/VEDBA_NORMALIZATION_INSTRUCTIONS.md`
