# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5_many_offsets.R
# ─────────────────────────────────────────────────────────────────────────────
# Wraps annotate_era5() (single-level + pressure-level ERA5 extraction from
# the monthly EnvData folder layout) to support the same ±N-day offset
# workflow that add_env_many_offsets() provided for the old yearly GRIB
# stacks — e.g. u10_-48h, u10_-24h, u10_0h, u10_24h, u10_48h for offsets_days
# = -2:2, ready to feed into calculate_wind_features().
#
# For each offset, the animal's timestamp is shifted by offset_days and
# annotate_era5() is re-run against that shifted time (same location) to
# look up the ERA5 step nearest the shifted time. Only the offset (day = 0)
# pass computes wind_support/crosswind/airspeed/flight-pressure matching,
# since heading and ground speed are derived from fix-to-fix geometry and
# are invariant to a uniform timestamp shift — recomputing them per offset
# would be redundant.
# ─────────────────────────────────────────────────────────────────────────────

#' Annotate a move2/sf object with ERA5 data at multiple day offsets
#'
#' @param data            A \code{move2} or \code{sf} object with timestamps.
#' @param era5_dir        Path to the EnvData directory (parent of
#'   \code{single_levels/} and \code{pressure_levels/}).
#' @param offsets_days    Numeric vector of day offsets, e.g. \code{-2:2}.
#' @param pressure_levels Numeric vector of pressure levels (hPa).
#' @param altitude_col,tag_pressure_col,max_time_gap_hours See \code{\link{annotate_era5}}.
#' @param verbose Logical; print progress messages?
#' @return \code{data} with per-offset single/pressure-level columns
#'   (\code{u10_-48h}, ..., \code{cbh_48h}) plus the day-0 wind-support /
#'   flight-pressure-matching columns from \code{\link{annotate_era5}}.
#' @export
annotate_era5_many_offsets <- function(
    data,
    era5_dir,
    offsets_days,
    pressure_levels  = c(500, 600, 700, 800, 850, 900, 925, 950, 1000),
    altitude_col     = NULL,
    tag_pressure_col = NULL,
    max_time_gap_hours = 3,
    verbose = TRUE
) {
  require(move2)
  require(sf)

  stopifnot(inherits(data, "sf"))

  base_time <- if (inherits(data, "move2")) move2::mt_time(data) else data$timestamp
  if (is.null(base_time)) stop("Cannot find timestamps in data.")

  existing_cols <- names(sf::st_drop_geometry(data))
  out <- data

  for (d in offsets_days) {
    if (verbose) message("Offset days: ", d)

    shifted <- data
    shifted_time <- base_time + d * 86400  # days -> seconds
    if (inherits(shifted, "move2")) {
      move2::mt_time(shifted) <- shifted_time
    } else {
      shifted$timestamp <- shifted_time
    }

    res <- annotate_era5(
      data                 = shifted,
      era5_dir             = era5_dir,
      pressure_levels      = pressure_levels,
      altitude_col         = altitude_col,
      tag_pressure_col     = tag_pressure_col,
      compute_wind_support = (d == 0),
      max_time_gap_hours   = max_time_gap_hours,
      verbose              = verbose
    )

    res_df  <- sf::st_drop_geometry(res)
    suffix  <- paste0("_", d * 24, "h")

    # single/pressure-level env vars: era5_u10 -> u10_-48h, era5_u500 -> u500_-48h, ...
    env_cols <- grep("^era5_", names(res_df), value = TRUE)
    for (nm in env_cols) {
      base_name <- sub("^era5_", "", nm)
      out[[paste0(base_name, suffix)]] <- res_df[[nm]]
    }

    # wind-support / flight-pressure-matching columns: only from the true (day-0) time
    if (d == 0) {
      extra_cols <- setdiff(names(res_df), c(existing_cols, env_cols))
      for (nm in extra_cols) out[[nm]] <- res_df[[nm]]
    }
  }

  out
}
