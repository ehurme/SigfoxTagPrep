# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5_sunset.R
# ─────────────────────────────────────────────────────────────────────────────
# Sunset-referenced counterpart to annotate_era5_many_offsets(): instead of
# looking up the ERA5 step nearest the fix time, looks up the step nearest
# sunset (+1h + shift_hours) at the fix's own location, falling back to the
# *previous* fix's location (lon_prev_col/lat_prev_col) when that sunset has
# already passed by the time of the current fix. Uses the SAME single-level
# variable/position table as annotate_era5() (.era5_single_var_positions(),
# incl. cbh) rather than a hand-maintained var_names list, and reads from the
# monthly era5_dir/single_levels/ GRIB files instead of a yearly
# raster_by_year stack — replaces extract_sunset_env().
#
# Depends on annotate_era5.R being sourced first (.era5_single_var_positions).
# ─────────────────────────────────────────────────────────────────────────────

#' Annotate a move2/sf object with ERA5 single-level data at sunset (+ offset)
#'
#' For each fix, finds sunset at the fix's own coordinates. If that sunset is
#' already in the past by the fix's timestamp, sunset is recomputed at the
#' \emph{previous} fix's coordinates instead, so the extraction always looks
#' up a sunset that actually preceded the fix. The ERA5 step nearest
#' \code{sunset + 1h + shift_hours} is then extracted from the monthly
#' \code{era5_dir/single_levels/} GRIB files, for each value in
#' \code{shift_hours}.
#'
#' @param data         A \code{move2} or \code{sf} object with timestamps.
#' @param era5_dir     Path to the EnvData directory (parent of
#'   \code{single_levels/}).
#' @param shift_hours  Numeric vector of hour offsets from sunset+1h, e.g.
#'   \code{seq(-48, 48, by = 24)}.
#' @param lon_prev_col,lat_prev_col Column names holding the previous fix's
#'   coordinates, used as the fallback location when the current-location
#'   sunset has already passed. \code{NULL} = no fallback (always use the
#'   fix's own coordinates).
#' @param tz              Timezone for timestamp handling.
#' @param coord_crs       CRS of the extraction coordinates.
#' @param keep_debug_cols Logical; also return \code{sunset_time_<suffix>},
#'   \code{target_time_<suffix>}, \code{use_prev_<suffix>} per offset?
#' @param verbose Logical; print progress messages?
#' @return \code{data} with per-offset columns (\code{u10_0h}, ..., \code{cbh_0h},
#'   ... for each \code{shift_hours} value, suffixed \code{_<shift>h}).
#' @export
annotate_era5_sunset_many_offsets <- function(
    data,
    era5_dir,
    shift_hours = 0,
    lon_prev_col = NULL,
    lat_prev_col = NULL,
    tz = "UTC",
    coord_crs = "EPSG:4326",
    keep_debug_cols = TRUE,
    verbose = TRUE
) {
  require(terra)
  require(lubridate)
  require(dplyr)
  require(suncalc)
  require(sf)

  stopifnot(inherits(data, "sf"))

  single_dir <- file.path(era5_dir, "single_levels")
  if (!dir.exists(single_dir)) stop("No single_levels/ folder found under ", era5_dir)

  base_time <- if (inherits(data, "move2")) move2::mt_time(data) else data$timestamp
  if (is.null(base_time)) stop("Cannot find timestamps in data.")

  attr_df <- sf::st_drop_geometry(data)
  coords  <- sf::st_coordinates(data)
  lon <- coords[, 1]
  lat <- coords[, 2]

  if (!is.null(lon_prev_col) && !is.null(lat_prev_col)) {
    stopifnot(lon_prev_col %in% names(attr_df), lat_prev_col %in% names(attr_df))
    lon_prev <- as.numeric(attr_df[[lon_prev_col]])
    lat_prev <- as.numeric(attr_df[[lat_prev_col]])
  } else {
    lon_prev <- lon
    lat_prev <- lat
  }

  n <- nrow(data)

  # ── base sunset table ────────────────────────────────────────────────────
  df <- data.frame(
    .row_id   = seq_len(n),
    timestamp = as.POSIXct(base_time, tz = tz),
    date      = as.Date(as.POSIXct(base_time, tz = tz), tz = tz),
    lat       = lat,
    lon       = lon,
    lat_prev  = lat_prev,
    lon_prev  = lon_prev,
    stringsAsFactors = FALSE
  )

  # ── sunset at current coords ─────────────────────────────────────────────
  if (verbose) message("Computing sunset at current coords...")
  sun_now <- suncalc::getSunlightTimes(data = df, tz = tz, keep = "sunset")
  df$sunset_time_now <- as.POSIXct(sun_now$sunset, tz = tz)

  # ── switch to prev coords when sunset has already passed ────────────────
  # Rule: if sunset(now) <= timestamp, recompute sunset using lon_prev/lat_prev
  # and extract there instead.
  df$use_prev <- !is.na(df$sunset_time_now) & (df$sunset_time_now <= df$timestamp)
  if (verbose) message("Rows using prev coords: ", sum(df$use_prev, na.rm = TRUE), " / ", nrow(df))

  df$sunset_time <- df$sunset_time_now
  if (any(df$use_prev, na.rm = TRUE)) {
    if (verbose) message("Recomputing sunset for rows where sunset<=timestamp using lon_prev/lat_prev...")
    idx <- which(df$use_prev)
    df_prev <- df[idx, ]
    df_prev$lat <- df_prev$lat_prev
    df_prev$lon <- df_prev$lon_prev
    sun_prev <- suncalc::getSunlightTimes(data = df_prev, tz = tz, keep = "sunset")
    df$sunset_time[idx] <- as.POSIXct(sun_prev$sunset, tz = tz)
  }

  df$lon_use <- ifelse(df$use_prev, df$lon_prev, df$lon)
  df$lat_use <- ifelse(df$use_prev, df$lat_prev, df$lat)

  var_positions <- .era5_single_var_positions()
  n_var    <- length(var_positions)
  var_cols <- names(var_positions)

  out <- data

  for (shift in shift_hours) {
    if (verbose) message("Sunset shift hours: ", shift)

    target_time <- lubridate::round_date(
      df$sunset_time + lubridate::hours(1 + shift), unit = "hour"
    )

    sub_tbl <- data.frame(
      .row_id     = df$.row_id,
      target_time = target_time,
      lon         = df$lon_use,
      lat         = df$lat_use,
      ym          = format(target_time, "%Y-%m"),
      stringsAsFactors = FALSE
    )
    for (nm in var_cols) sub_tbl[[nm]] <- NA_real_

    yms_needed <- sort(unique(sub_tbl$ym[!is.na(sub_tbl$ym)]))
    if (verbose) message("  Year-months needed: ", paste(yms_needed, collapse = ", "))

    for (ym_i in yms_needed) {
      f <- file.path(single_dir, paste0("era5_single_", gsub("-", "_", ym_i), ".grib"))
      if (!file.exists(f)) {
        if (verbose) message("  [skip] no file for ", ym_i)
        next
      }

      r  <- terra::rast(f)
      rt <- as.POSIXct(terra::time(r), tz = tz)
      if (is.null(rt) || length(rt) == 0) {
        if (verbose) message("  [skip] ", ym_i, " has no time vector.")
        next
      }
      rt_hour   <- lubridate::round_date(rt, "hour")
      rt_unique <- sort(unique(rt_hour))

      actual_block <- terra::nlyr(r) / length(rt_unique)
      if (actual_block %% 1 != 0 || actual_block != n_var) {
        stop("Raster for ", ym_i, ": ", terra::nlyr(r), " layers / ", length(rt_unique),
             " timestamps = ", actual_block, " layers/hour, but expected ", n_var,
             " (", paste(var_cols, collapse = ", "), "). ",
             "This file's variable layout no longer matches .era5_single_var_positions().")
      }

      sub_idx  <- which(sub_tbl$ym == ym_i)
      t_round  <- lubridate::round_date(as.POSIXct(sub_tbl$target_time[sub_idx], tz = tz), "hour")
      hour_idx <- match(as.numeric(t_round), as.numeric(rt_unique))

      ok <- which(!is.na(hour_idx))
      if (length(ok) == 0L) {
        if (verbose) message("  No matching raster hours for ", ym_i)
        next
      }

      sub_ok      <- sub_idx[ok]
      hour_idx_ok <- hour_idx[ok]
      uniq_hours  <- sort(unique(hour_idx_ok))

      pts <- terra::vect(
        data.frame(x = sub_tbl$lon[sub_ok], y = sub_tbl$lat[sub_ok]),
        geom = c("x", "y"), crs = coord_crs
      )
      r_crs <- terra::crs(r, proj = TRUE)
      if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) pts <- terra::project(pts, r_crs)

      layer_blocks <- lapply(uniq_hours, function(h) ((h - 1L) * n_var + 1L):(h * n_var))
      all_layers   <- unlist(layer_blocks)

      if (verbose) {
        message("  ", ym_i, ": rows=", length(sub_ok), ", unique hours=", length(uniq_hours),
                ", vars/hour=", n_var)
      }

      r_sub <- r[[all_layers]]
      vals  <- terra::extract(r_sub, pts, ID = FALSE)
      if (ncol(vals) != length(all_layers)) {
        stop("Extraction returned ", ncol(vals), " columns, expected ", length(all_layers), ".")
      }

      hour_block_idx <- match(hour_idx_ok, uniq_hours)
      out_mat <- matrix(NA_real_, nrow = length(sub_ok), ncol = n_var)
      for (v in seq_len(n_var)) {
        col_in_vals <- (hour_block_idx - 1L) * n_var + v
        out_mat[, v] <- vals[cbind(seq_along(sub_ok), col_in_vals)]
      }
      sub_tbl[sub_ok, var_cols] <- out_mat

      rm(r); gc(verbose = FALSE)
    }

    suffix <- paste0("_", shift, "h")
    m <- match(seq_len(n), sub_tbl$.row_id)
    for (nm in var_cols) {
      base_name <- sub("^era5_", "", nm)
      out[[paste0(base_name, suffix)]] <- sub_tbl[[nm]][m]
    }
    if (keep_debug_cols) {
      out[[paste0("sunset_time", suffix)]] <- df$sunset_time
      out[[paste0("target_time", suffix)]] <- sub_tbl$target_time[m]
      out[[paste0("use_prev",    suffix)]] <- df$use_prev
    }
  }

  out
}


# ─────────────────────────────────────────────────────────────────────────────
# Usage example (commented out)
# ─────────────────────────────────────────────────────────────────────────────
# source("./R/annotate_era5.R")
# source("./R/annotate_era5_sunset.R")
#
# bats_daily_sunset <- annotate_era5_sunset_many_offsets(
#   data          = bats_daily,          # sf/move2 object with lon_prev/lat_prev cols
#   era5_dir      = "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData",
#   shift_hours   = seq(-48, 48, by = 24),
#   lon_prev_col  = "lon_prev",
#   lat_prev_col  = "lat_prev"
# )
#
# source("./R/calculate_wind_features.R")
# bats_daily_sunset <- calculate_wind_features(
#   data            = bats_daily_sunset,
#   u_col_base      = "u10",
#   v_col_base      = "v10",
#   distance_col    = "distance",
#   time_diff_col   = "dt_prev",
#   bearing_col     = "azimuth_prev",
#   offsets         = -2:2,
#   offset_units    = "days",
#   time_diff_units = "seconds"
# )
