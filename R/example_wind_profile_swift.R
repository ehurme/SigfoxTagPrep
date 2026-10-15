# example_wind_profile_swift.R
# ─────────────────────────────────────────────────────────────────────────────
# Workflow: wind/altitude analysis for common swift (Apus apus) — GPS + Sigfox
#
# Common swifts fly continuously for up to ~10 months (no nightly roost
# return), so this differs from example_wind_profile_2026.R (bats, discrete
# migratory nights) in two ways:
#   - find_complete_nights() is called with min_displacement_km = 0 so every
#     well-covered calendar-day window qualifies, not just >100 km bouts.
#   - summarize_wind_altitude() is used to look at the altitude/wind
#     relationship across the *entire* deployment and by time of day, in
#     addition to (or instead of) per-night scan_night_wind_profile() PNGs.
#
# Data sources (three Movebank studies stacked into one move2 object):
#   GPS  — raw GPS study (30-min fixes, height_raw altitude, no barometric
#          pressure sensor)
#   roth — Sigfox/TinyFox study (sensor_type_id "sigfox-geolocation")
#   olpe — Sigfox/TinyFox study
# ─────────────────────────────────────────────────────────────────────────────

library(move2)
library(sf)
library(dplyr)
library(units)

source("R/annotate_era5.R")
source("R/calculate_wind_features.R")
source("R/pressure_to_altitude_m.R")
source("R/scan_night_wind_profile.R")
source("R/lagerveld_flight_profile.R")
source("R/extract_elevation_segments.R")

# ── Species flight-speed reference (Vmp / Vmr) ───────────────────────────────
# bat_flight_params (lagerveld_flight_profile.R) only covers bats — common
# swift is not a bat, so its values are supplied here directly rather than
# added to that table. These are approximate literature/theoretical figures
# (~40-50 g, ~42 cm wingspan; radar/GPS-tracked migratory airspeeds around
# 10-12 m/s, e.g. Bäckman & Alerstam 2001) — VERIFY against a primary source
# before quoting them in any output; the Lagerveld panel is a rough guide for
# this species, not a validated one.
vmp_swift <- 8.0    # m/s, approximate sea-level minimum-power speed
vmr_swift <- 11.5   # m/s, approximate sea-level max-range speed

# ── 1. Download and stack the three studies ──────────────────────────────────
# GPS: raw 30-min fixes, no on-board pressure sensor — altitude comes from
# height_raw (annotate_era5() auto-detects this).
GPS <- movebank_download_study(study_id = 7013024916)  # replace with your study_id
GPS <- GPS %>% filter(!st_is_empty(.))
mt_track_id(GPS) <- "deployment_id"

# roth / olpe: Sigfox/TinyFox studies — tag pressure via
# tinyfox_pressure_min_last_24h (daily min, not instantaneous).
roth <- movebank_download_study(study_id = 4731778376, sensor_type_id = "sigfox-geolocation")
olpe <- movebank_download_study(study_id = 4731733529)
mt_track_id(roth) <- "deployment_id"
olpe <- olpe %>% mt_as_event_attribute(individual_local_identifier)

swift.raw <- mt_stack(roth, olpe, GPS)
swift.raw <- swift.raw %>% filter(!st_is_empty(.))

# Drop rows with no real activity (Sigfox-only column; GPS rows keep NA here
# and pass through untouched)
if ("tinyfox_activity_percent_last_24h" %in% names(swift.raw)) {
  swift.raw <- swift.raw %>%
    filter(is.na(tinyfox_activity_percent_last_24h) |
             tinyfox_activity_percent_last_24h >= set_units(5, "%"))
}

# ── 2. Annotate with ERA5 wind at all pressure levels ────────────────────────
# annotate_era5() auto-detects altitude_col (height_raw, GPS) and
# tag_pressure_col (tinyfox_pressure_min_last_24h, Sigfox) per row, priority
# tag-pressure > GPS-altitude — no per-source branching needed even though
# swift.raw mixes both sensor types.
era5_dir <- "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"
p_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)

swift_annotated <- annotate_era5(
  data             = swift.raw,
  era5_dir         = era5_dir,
  pressure_levels  = p_levels,
  compute_wind_support = TRUE,
  verbose          = TRUE
)

# ── 3. Find well-covered day/night windows across the full deployment ───────
# min_displacement_km = 0: swifts don't return to a roost each night, so the
# >100 km bat-migration threshold would exclude almost everything. This keeps
# every calendar-day pair with sufficient ERA5 wind coverage instead.
good_windows <- find_complete_nights(
  data                 = swift_annotated,
  pressure_levels      = p_levels,
  min_displacement_km  = 0,
  min_coverage         = 0.5,
  verbose              = TRUE
)

print(good_windows)
cat("\nQualifying windows by individual:\n")
print(table(good_windows$individual_local_identifier))

# ── 4. Per-window diagnostic PNGs ────────────────────────────────────────────
out_dir <- "Plots/WindProfile/Swift/"

for (i in seq_len(nrow(good_windows))) {
  w <- good_windows[i, ]
  message("\n[", i, "/", nrow(good_windows), "] ",
          w$individual_local_identifier, "  ", w$night_date,
          "  (", round(w$era5_coverage * 100), "% ERA5 coverage)")

  try({
    scan_night_wind_profile(
      data            = swift_annotated,
      individual_id   = w$individual_local_identifier,
      t_dep           = w$t_dep,
      t_arr           = w$t_arr,
      pressure_levels = p_levels,
      vmp_sl_ms       = vmp_swift,
      vmr_sl_ms       = vmr_swift,
      species_label   = "Apus apus",
      elev_z          = 6,
      buffer_deg      = 1.5,
      out_path        = out_dir,
      theme_dark      = FALSE,
      verbose         = TRUE
    )
  })
}

# ── 5. Full-period altitude/wind summary (all individuals, whole deployment) ─
# The per-window PNGs above are still useful for inspecting one day/night in
# detail, but for a species airborne for months the more informative view is
# across the entire annotated dataset at once.
wind_alt <- summarize_wind_altitude(
  data             = swift_annotated,
  individual_col   = "individual_local_identifier",
  hour_bin_width   = 2,
  out_path         = out_dir,
  theme_dark       = FALSE,
  verbose          = TRUE
)

print(wind_alt$plots$combined)

# Diel summary table (mean altitude / wind support by local-solar-hour bin)
print(wind_alt$diel_summary)

# Per-fix data with derived altitude_m / local_solar_hour, for further
# modelling (e.g. mixed-effects altitude ~ wind_support + (1 | individual))
head(wind_alt$data)
