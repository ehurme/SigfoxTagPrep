# CLAUDE.md

Project overview, structure, build/test commands and conventions live in AGENTS.md:

@AGENTS.md

This file holds **rules** (how to work in this repo) and **memory** (durable lessons learned).
Keep entries short and dated. Add new lessons under "Memory" when something non-obvious is
discovered; delete entries that become wrong.

## Rules

- Primary data structure is `move2`. Pipelines return three objects: full, location, daily. Preserve that pattern.
- Keep `pacman::p_load(...)` convention inside functions unless deliberately refactoring.
- Never hand-edit `NAMESPACE` or `man/`; run `devtools::document()`.
- Exported functions need roxygen docs; use `@examples \dontrun{...}` for anything needing network/data.
- No top-level `library()`, `load()`, `source()` or script code in `R/` files — R CMD check runs them. Site-specific scripts go in `explore/`.
- Don't duplicate helpers across `R/` files (e.g. `calculate_bearing()` lives in `tracking_data_processing.R` only).
- Python helpers are in `python/`, not `inst/python/`.
- Network paths (`//10.0.16.7/grpdechmann/...`) are MPI-only; never assume they exist, never hard-code new ones into package functions — take them as arguments.
- Never commit credentials (`~/.cdsapirc`, `.Renviron`, Movebank logins).
- Validate changes to core functions on real/representative data — unit tests are offline and don't cover Movebank download.
- `sigfox_download()` is legacy (NanoFox only); new data flows Wildcloud → Movebank → `import_nanofox_movebank()`.
- VeDBA scaling: follow `claude/vedba_calculation_guide.md` and `claude/tinyfox_vedba_scaling_correction.md`.

## Memory

### ERA5 / `annotate_era5()` (`R/annotate_era5.R`)
- **2026-09-15 — GRIB `md = FALSE` required.** With terra 1.9.34 + GDAL 3.12.1, `terra::rast()` opens the monthly ERA5 GRIBs via GDAL's multidimensional API by default: `time()` returns NA and bands get regrouped by variable, so `wind_support_*`/`crosswind_*`/`airspeed_*` silently stayed empty. Every `rast()` on these GRIBs passes `md = FALSE`. Check after any terra/GDAL upgrade: `names(terra::rast(grib))` must show interleaved variable names, not blocks of one stem.
- Only GRIB files for needed year-months are opened (`.era5_needed_year_months()`); each file takes ~15–20 s over the network share.
- Altitude source auto-detected per row: tag pressure > GPS altitude. Mixed `mt_stack()` GPS + Sigfox data needs no prep. `flight_pressure_hPa` is the universal altitude-as-pressure output column.

### Wind profile analysis (`R/scan_night_wind_profile.R`)
- `find_complete_nights()`, `scan_night_wind_profile()`, `summarize_wind_altitude()` share pressure-column auto-detection ending in `flight_pressure_hPa`.
- Species without discrete migratory nights (e.g. common swift): use `find_complete_nights(..., min_displacement_km = 0)`; the 100 km default assumes bat roost-to-roost bouts.
- `bat_flight_params` (`R/lagerveld_flight_profile.R`) is bats only; non-bats pass `vmp_sl_ms`/`vmr_sl_ms` directly. Swift values (~8.0/11.5 m/s) are unverified placeholders.

### Tags
- Three schemas: tinyfox (flat columns), nanofox (separate sensor rows), uwasp (simpler).
- `detect_tag_fell_off()` has wrappers `tag_fell_off_tinyfox/nanofox/uwasp`.
- `import_nanofox_movebank()` helpers are top-level internals in `R/import_nanofox_movebank_helpers.R`. `verbose` reaches them via `options(SigfoxTagPrep.verbose)` (set + restored by the main function); `.msg()` reads it.
- **2026-11-04 — no duplicate top-level names.** R collates `R/` alphabetically and a later file silently replaces an earlier same-named function (this hid fixes in `extract_avg_night_env*` and gave `%||%` three meanings). `%||%` lives only in `R/utils.R`.
- No `library()`/`require()` in `R/`: Imports go in `R/SigfoxTagPrep-package.R`, optional packages are called `pkg::fn()`.
