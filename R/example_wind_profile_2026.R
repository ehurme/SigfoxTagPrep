# example_wind_profile_2026.R
# ─────────────────────────────────────────────────────────────────────────────
# Workflow: wind-column analysis for 2026 finescale NanoFox tracks
#
# 2026 tag produces instantaneous barometric_pressure every ~36 min.
# This script:
#   1. Loads your move2 object (bats_loc — location-only rows, already prepared)
#   2. Runs annotate_era5() to extract wind at all pressure levels
#   3. Calls find_complete_nights() to identify nights with
#         displacement > 100 km  AND  complete ERA5 wind coverage
#   4. Loops over qualifying nights and saves scan_night_wind_profile() PNGs
# ─────────────────────────────────────────────────────────────────────────────

library(move2)
library(sf)
library(dplyr)

source("R/annotate_era5.R")
source("R/calculate_wind_features.R")
source("R/pressure_to_altitude_m.R")
source("R/scan_night_wind_profile.R")
source("R/lagerveld_flight_profile.R")
source("R/extract_elevation_segments.R")

# ── Look up species-specific Vmp / Vmr ───────────────────────────────────────
# bat_flight_params contains defaults for common species.
# Override if you have wind-tunnel data for your species.
print(bat_flight_params[, c("species", "vmp_sl_ms", "vmr_sl_ms", "source")])

# For Nyctalus leisleri (Leisler's bat):
sp_params <- bat_flight_params[bat_flight_params$species == "Nyctalus leisleri", ]
vmp_use   <- sp_params$vmp_sl_ms   # 7.0 m/s
vmr_use   <- sp_params$vmr_sl_ms   # 9.2 m/s
vmp_sd    <- sp_params$vmp_sd_ms   # NA → will use default 1.0
vmr_sd    <- sp_params$vmr_sd_ms   # NA → will use default 1.1

# ── 1. Load data ─────────────────────────────────────────────────────────────
# Replace with your actual data loading step, e.g.:
#   bats <- readRDS("data/bats_2026.rds")
#   bats_loc <- bats$location    # location-only rows
#
# For 2026 finescale NanoFox, bats_loc should have:
#   timestamp, geometry (lon/lat), individual_local_identifier,
#   barometric_pressure (instantaneous hPa every ~36 min)

# bats_loc <- readRDS("path/to/bats_loc_2026.rds")

# ── 2. Annotate with ERA5 wind at all pressure levels ────────────────────────
era5_dir <- "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"

# Pressure levels to extract (must match files in era5_dir/pressure_levels/)
p_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)

bats_annotated <- annotate_era5(
  data             = bats_loc,
  era5_dir         = era5_dir,
  pressure_levels  = p_levels,
  pressure_col     = "barometric_pressure",   # 2026 finescale column
  compute_wind_support = TRUE,
  verbose          = TRUE
)

# ── 3. Find qualifying migration nights ──────────────────────────────────────
# Nights where displacement > 100 km and ERA5 wind data covers >= 50% of fixes.
good_nights <- find_complete_nights(
  data                 = bats_annotated,
  pressure_levels      = p_levels,
  min_displacement_km  = 100,
  min_coverage         = 0.5,
  pressure_col         = "barometric_pressure",
  verbose              = TRUE
)

print(good_nights)

# Quick summary
cat("\nQualifying nights by individual:\n")
print(table(good_nights$individual_local_identifier))

# ── 4. Generate wind-profile plots for all qualifying nights ─────────────────
out_dir <- "Plots/WindProfile/2026/"

for (i in seq_len(nrow(good_nights))) {
  night <- good_nights[i, ]
  message("\n[", i, "/", nrow(good_nights), "] ",
          night$individual_local_identifier, "  ",
          night$night_date,
          "  (", night$displacement_km, " km, ",
          round(night$era5_coverage * 100), "% ERA5 coverage)")

  try({
    scan_night_wind_profile(
      data            = bats_annotated,
      individual_id   = night$individual_local_identifier,
      t_dep           = night$t_dep,
      t_arr           = night$t_arr,
      pressure_levels = p_levels,
      pressure_col    = "barometric_pressure",
      # Lagerveld profile: species-specific reference speeds
      vmp_sl_ms       = vmp_use,
      vmr_sl_ms       = vmr_use,
      vmp_sd_ms       = if (!is.na(vmp_sd)) vmp_sd else 1.0,
      vmr_sd_ms       = if (!is.na(vmr_sd)) vmr_sd else 1.1,
      species_label   = "Nyctalus leisleri",
      elev_z          = 6,
      buffer_deg      = 1.5,
      out_path        = out_dir,
      theme_dark      = FALSE,
      verbose         = TRUE
    )
  })
}

# ── 5. Inspect a single night interactively ──────────────────────────────────
# Pick any row from good_nights:
# i <- 1
# p <- scan_night_wind_profile(
#   data            = bats_annotated,
#   individual_id   = good_nights$individual_local_identifier[i],
#   t_dep           = good_nights$t_dep[i],
#   t_arr           = good_nights$t_arr[i],
#   pressure_levels = p_levels,
#   pressure_col    = "barometric_pressure",
#   vmp_sl_ms       = vmp_use,
#   vmr_sl_ms       = vmr_use,
#   species_label   = "Nyctalus leisleri",
#   out_path        = NULL   # return without saving
# )
# print(p)

# ── 6. Compute Lagerveld profiles directly ────────────────────────────────────
# For a specific night, you can also compute and inspect the profiles table
# without going through scan_night_wind_profile():
#
# night_sf <- bats_annotated[
#   bats_annotated$individual_local_identifier == good_nights$individual_local_identifier[1] &
#   bats_annotated$timestamp >= good_nights$t_dep[1] &
#   bats_annotated$timestamp <= good_nights$t_arr[1], ]
#
# profiles <- compute_night_profiles(
#   night_df        = night_sf,
#   pressure_levels = p_levels,
#   vmp_sl_ms       = vmp_use,
#   vmr_sl_ms       = vmr_use
# )
#
# # Optimal altitude per interval
# opt <- summarise_optimal_altitudes(profiles, vmp_sl_ms = vmp_use)
# print(opt[, c("t_mid", "optimal_alt_m", "min_va_ms",
#               "at_vmp_optimal", "at_vmr_optimal",
#               "feasible_alt_min_m", "feasible_alt_max_m")])
#
# # Standalone Lagerveld plot
# p_lag <- plot_lagerveld_profile(profiles, vmp_sl_ms = vmp_use,
#                                 species_label = "Nyctalus leisleri")
# print(p_lag)

# ── 6. Check wind-selection behaviour ─────────────────────────────────────────
# After annotation, each location row has:
#   at_best_wind       — logical: is the animal at the best available wind level?
#   best_wind_level    — hPa level with highest wind support that night
#   wind_support_flight — wind support at the animal's actual flight level
#   best_wind_support  — wind support at the best available level
#   airspeed_flight    — estimated airspeed at flight level (requires heading)

# Proportion of night fixes where animal was at best-wind level:
bats_annotated %>%
  sf::st_drop_geometry() %>%
  filter(!is.na(at_best_wind)) %>%
  group_by(individual_local_identifier) %>%
  summarise(
    n_fixes           = n(),
    pct_at_best_wind  = round(mean(at_best_wind, na.rm = TRUE) * 100, 1),
    mean_ws_flight    = round(mean(wind_support_flight, na.rm = TRUE), 2),
    mean_ws_best      = round(mean(best_wind_support, na.rm = TRUE), 2),
    mean_airspeed     = round(mean(airspeed_flight, na.rm = TRUE), 2),
    .groups = "drop"
  ) %>%
  arrange(desc(pct_at_best_wind)) %>%
  print()
