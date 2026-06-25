# run_pressure_wind_bats_2026_gee.R
# ─────────────────────────────────────────────────────────────────────────────
# Same workflow as run_pressure_wind_bats_2026.R but uses annotate_era5_gee()
# for faster ERA5 extraction (no local GRIB files required).
#
# IMPORTANT — pressure-level wind:
#   The public GEE ERA5 catalog only has single-level variables.
#   This script extracts u/v at 10 m and 100 m from GEE, then optionally
#   supplements with pressure-level wind from local GRIBs via annotate_era5()
#   (set era5_dir below; set to NULL to skip).
#
# Setup (once per machine):
#   install.packages("rgee")
#   rgee::ee_install()
#   rgee::ee_Initialize(user = "your@gmail.com", drive = TRUE)
# ─────────────────────────────────────────────────────────────────────────────

library(tidyverse)
library(move2)
library(sf)

source("R/annotate_era5_gee.R")
source("R/annotate_era5.R")           # helpers reused by GEE version
source("R/calculate_wind_features.R")
source("R/pressure_to_altitude_m.R")
source("R/pressure_wind_profile.R")
source("R/extract_elevation_segments.R")

# ── Config ─────────────────────────────────────────────────────────────────────
# Set era5_dir to your local GRIB path for pressure-level wind, or NULL to
# use only the single-level (10 m / 100 m) wind from GEE.
era5_dir <- "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"  # NULL to skip
p_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)
out_dir  <- "output/WindProfile_bats_2026_gee/"

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
  vedba_aggregation   = "precomputed",
  temp_col            = NULL,
  temp_aggregation    = NULL,
  pressure_levels     = p_levels,
  min_displacement_km = 100,
  out_path            = out_dir
)

# ── 0. Python / GEE path ──────────────────────────────────────────────────────
# Point this at the Python that has earthengine-api installed.
# Authenticate once from a terminal:
#   conda activate rgee311
#   python -c "import ee; ee.Authenticate()"
python_exe <- "C:/Users/Edward/anaconda3/envs/rgee311/python.exe"

# ── 1. Load data ───────────────────────────────────────────────────────────────
e <- new.env(parent = emptyenv())
load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily.robj",
     envir = e)
bats_full <- e$bats_full

leisler <- bats_full %>%
  filter(species == "Nyctalus leisleri", year == 2026,
         tag_local_identifier == "9EA1D0")

cat("Total rows:    ", nrow(leisler), "\n")
cat("Location rows: ", sum(!sf::st_is_empty(leisler)), "\n")
cat("Individuals:   ", n_distinct(leisler$individual_local_identifier), "\n")

# ── 2a. Annotate ERA5 via GEE Python script ───────────────────────────────────
# Extracts: u/v at 10m, 100m, 500hPa, 850hPa, t2m, msl, sp, tp, gust, tcc, cbh
message("\n── Step 2a: GEE ERA5 (500 + 850 hPa + surface) ─────────────────────")
leisler_annotated <- annotate_era5_gee(
  data                 = leisler,
  python               = python_exe,
  compute_wind_support = FALSE,   # compute once after optional pressure supplement
  verbose              = TRUE
)

# ── 2b. Supplement with pressure-level wind from local GRIBs (optional) ───────
# This adds era5_uXXX / era5_vXXX columns that GEE cannot supply.
# Skip if era5_dir is NULL or the network drive is unavailable.
if (!is.null(era5_dir) && dir.exists(era5_dir)) {
  message("\n── Step 2b: pressure-level ERA5 from local GRIBs ────────────────────")

  # Temporarily call annotate_era5() for pressure levels only (skip single-level
  # re-extraction and wind-support until both data sources are merged).
  # We pass compute_wind_support = FALSE here and compute it once below.
  pressure_dir <- file.path(gsub("\\\\", "/", era5_dir), "pressure_levels")

  if (dir.exists(pressure_dir)) {
    coords_mat <- sf::st_coordinates(leisler_annotated)
    timestamps <- if (inherits(leisler_annotated, "move2"))
      move2::mt_time(leisler_annotated)
    else
      leisler_annotated$timestamp

    leisler_annotated <- .era5_extract_pressure(
      data          = leisler_annotated,
      pressure_dir  = pressure_dir,
      timestamps    = timestamps,
      coords_mat    = coords_mat,
      pressure_levels = p_levels,
      max_gap       = 3,
      verbose       = TRUE
    )
  } else {
    message("  pressure_levels/ folder not found — skipping.")
  }
} else {
  message("\n── Step 2b: skipped (era5_dir = NULL or not accessible)")
  message("  Wind support will be computed from 10 m / 100 m surface wind only.")
}

# ── 2c. Compute wind support (once, after all wind columns are present) ────────
message("\n── Step 2c: computing wind support ──────────────────────────────────")
alt_col  <- intersect(c("height_raw","altitude","altitude_m","altitude_sea",
                        "altitude.sea","height_above_msl"), names(leisler_annotated))[1]
pres_col <- intersect(c("tinyfox_pressure_min_last_24h","min_3h_pressure",
                        "tag_pressure","barometric_pressure","pressure_hpa_used"),
                      names(leisler_annotated))[1]

leisler_annotated <- .era5_wind_support(
  data             = leisler_annotated,
  pressure_levels  = p_levels,
  altitude_col     = if (length(alt_col)  > 0 && !is.na(alt_col))  alt_col  else NULL,
  tag_pressure_col = if (length(pres_col) > 0 && !is.na(pres_col)) pres_col else NULL
)

# ── 3. Find qualifying migration nights ───────────────────────────────────────
message("\n── Step 3: find_complete_periods() ──────────────────────────────────")
good_nights <- find_complete_periods(
  data         = leisler_annotated,
  cfg          = cfg,
  min_coverage = 0.5,
  verbose      = TRUE
)

print(good_nights)
cat("\nQualifying nights by individual:\n")
print(table(good_nights$individual_local_identifier))

# ── 4. Generate plots ─────────────────────────────────────────────────────────
message("\n── Step 4: pressure_wind_profile() for all qualifying nights ────────")
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
      sensor_data   = leisler_annotated,
      elev_z        = 6,
      buffer_deg    = 1.5,
      theme_dark    = FALSE,
      verbose       = TRUE
    ),
    error = function(e) {
      message("  failed: ", conditionMessage(e))
      NULL
    }
  )
}

ok <- !vapply(results, is.null, logical(1))
cat("\nCompleted:", sum(ok), "/", length(results), "nights\n")

# ── 5. Wind-selection summary ─────────────────────────────────────────────────
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
