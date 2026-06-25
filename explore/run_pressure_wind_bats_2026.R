# run_pressure_wind_bats_2026.R
# ─────────────────────────────────────────────────────────────────────────────
# Workflow: wind-column analysis for 2026 bat migration (finescale NanoFox)
# Uses pressure_wind_profile() / find_complete_periods() from
# R/pressure_wind_profile.R.
#
# Data source:
#   leisler — bats_full filtered to Nyctalus leisleri 2026; all 36-min messages
#             including non-location rows (pressure + vedba but no GPS fix).
#             annotate_era5() auto-detects barometric_pressure; non-location rows
#             get NA wind values but retain their sensor measurements.
#
# Tag specs:
#   Firmware    : NanoFox_finescale
#   Pressure    : barometric_pressure (instantaneous, every ~36 min)
#   VeDBA       : vedba (per 36-min interval)
#   Species     : Nyctalus leisleri (Leisler's bat)
#   Window      : nightly
#   ERA5 dir    : //10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData
# ─────────────────────────────────────────────────────────────────────────────

library(tidyverse)
library(move2)
library(sf)
library(terra)

source("R/annotate_era5.R")
source("R/calculate_wind_features.R")
source("R/pressure_to_altitude_m.R")
source("R/pressure_wind_profile.R")
source("R/extract_elevation_segments.R")

# ── Config ────────────────────────────────────────────────────────────────────
era5_dir <- "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"
p_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)
out_dir  <- "output/WindProfile_bats_2026/"

cfg <- list(
  firmware            = "NanoFox_finescale",
  pressure_type       = "instantaneous",
  timestamp_col       = "timestamp",
  individual_col      = "individual_local_identifier",
  pressure_col        = "barometric_pressure",
  heading_col         = NULL,
  window_type         = "nightly",
  interpolation       = "linear",
  interp_timestep_min = 10,
  vedba_col           = "vedba",
  vedba_aggregation   = "precomputed",  # already a per-interval value
  temp_col            = NULL,
  temp_aggregation    = NULL,
  pressure_levels     = p_levels,
  min_displacement_km = 100,
  out_path            = out_dir
)

# ── 1. Load data ──────────────────────────────────────────────────────────────
e <- new.env(parent = emptyenv())
load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily.robj", envir = e)
bats_full <- e$bats_full

leisler <- bats_full %>% filter(species == "Nyctalus leisleri", year == 2026, 
tag_local_identifier == "9EA1D0", timestamp < ymd("2026-04-27"))
# 9EAE4E

cat("Total rows:    ", nrow(leisler), "\n")
cat("Location rows: ", sum(!sf::st_is_empty(leisler)), "\n")
cat("Individuals:   ", n_distinct(leisler$individual_local_identifier), "\n")

# ── 2. Annotate with ERA5 wind ────────────────────────────────────────────────
# barometric_pressure is auto-detected; non-location rows (empty geometry) get
# NA for all ERA5 wind columns but retain their pressure and vedba values.
leisler_annotated <- annotate_era5(
  data                 = leisler,
  era5_dir             = era5_dir,
  pressure_levels      = p_levels,
  compute_wind_support = TRUE,
  verbose              = TRUE
)

# ── 3. Find qualifying migration nights ───────────────────────────────────────
good_nights <- find_complete_periods(
  data         = leisler_annotated,
  cfg          = cfg,
  min_coverage = 0.5,
  verbose      = TRUE
)

print(good_nights)
cat("\nQualifying nights by individual:\n")
print(table(good_nights$individual_local_identifier))

# ── 4. Generate plots for all qualifying nights ───────────────────────────────
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results <- vector("list", nrow(good_nights))

for (i in seq_len(nrow(good_nights))) {
  night <- good_nights[i, ]
  id    <- night$individual_local_identifier

  message("\n[", i, "/", nrow(good_nights), "] ", id,
          "  ", as.Date(night$t_start),
          "  (", round(night$displacement_km), " km)")

  results[[i]] <- tryCatch(
    pressure_wind_profile(
      data          = leisler_annotated,
      cfg           = cfg,
      individual_id = id,
      t_start       = night$t_start,
      t_end         = night$t_end,
      sensor_data   = leisler_annotated,  # all rows incl. non-location, for pressure/VeDBA panels
      elev_z        = 6,
      buffer_deg    = 1.5,
      theme_dark    = FALSE,
      verbose       = TRUE
    ),
    error = function(e) {
      message("  ✗ failed: ", conditionMessage(e))
      NULL
    }
  )
}

# ── 5. Summary ────────────────────────────────────────────────────────────────
ok <- !vapply(results, is.null, logical(1))
cat("\nCompleted:", sum(ok), "/", length(results), "nights\n")

# ── 6. Inspect a single night interactively ───────────────────────────────────
# Uncomment to replot without saving:
#
# i <- 1
# r <- pressure_wind_profile(
#   data          = leisler_annotated,
#   cfg           = cfg,
#   individual_id = good_nights$individual_local_identifier[i],
#   t_start       = good_nights$t_start[i],
#   t_end         = good_nights$t_end[i],
#   sensor_data   = leisler_annotated,
#   verbose       = TRUE
# )
# print(r$plot)
#
# Saved objects in r:
#   r$raw_data      — location fixes (ERA5-annotated) in the window
#   r$sensor_data   — all 36-min rows (pressure + VeDBA) in the window
#   r$interp_track  — interpolated track (timestamp, .lon, .lat)
#   r$wind_long     — wind support at each pressure level × time
#   r$elev_profile  — terrain elevation along the flight path
#   r$plot          — assembled ggplot

# ── 7. Wind-selection summary ─────────────────────────────────────────────────
if (all(c("wind_support_flight", "best_wind_support", "at_best_wind") %in%
        names(sf::st_drop_geometry(leisler_annotated)))) {
  leisler_annotated %>%
    sf::st_drop_geometry() %>%
    filter(!is.na(at_best_wind)) %>%
    group_by(individual_local_identifier) %>%
    summarise(
      n_fixes          = n(),
      pct_at_best_wind = round(mean(at_best_wind, na.rm = TRUE) * 100, 1),
      mean_ws_flight   = round(mean(wind_support_flight, na.rm = TRUE), 2),
      mean_ws_best     = round(mean(best_wind_support,   na.rm = TRUE), 2),
      .groups = "drop"
    ) %>%
    arrange(desc(pct_at_best_wind)) %>%
    print()
}
