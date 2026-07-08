# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5_avg_night.R
# ─────────────────────────────────────────────────────────────────────────────
# Night-averaged counterpart to annotate_era5_many_offsets(): instead of the
# single ERA5 step nearest each fix, averages every hourly ERA5 step across
# the preceding night (suncalc sunset->sunrise, spanning up to 24h back) for
# each of offsets_days, using the SAME single-level variable/position table
# as annotate_era5() (.era5_single_var_positions(), incl. cbh) rather than a
# hand-maintained var_names list — so it can't drift out of sync with the
# monthly single_levels/ GRIB layout the way extract_avg_night_env_from_year_
# stacks()'s var_names did.
#
# Depends on annotate_era5.R being sourced first (.era5_single_var_positions).
# ─────────────────────────────────────────────────────────────────────────────

# ── Night-hour sequence helper (suncalc-based) ──────────────────────────────
# Self-contained copy of extract_nighttime_hours_24h() from
# extract_avg_night_env.R — duplicated (matching that file's own duplication
# precedent) so this file has no dependency beyond annotate_era5.R.
.era5_night_hours_24h <- function(timestamps, latitudes, longitudes, tz = "UTC") {
  requireNamespace("lubridate", quietly = TRUE)
  requireNamespace("suncalc",   quietly = TRUE)

  n   <- length(timestamps)
  ts  <- as.POSIXct(timestamps, tz = tz)
  dt  <- as.Date(ts, tz = tz)
  dt1 <- dt - 1L

  combos_today <- unique(data.frame(date = dt,  lat = latitudes, lon = longitudes))
  combos_yday  <- unique(data.frame(date = dt1, lat = latitudes, lon = longitudes))
  all_combos   <- unique(rbind(combos_today, combos_yday))

  sun_all <- suncalc::getSunlightTimes(data = all_combos, tz = tz, keep = c("sunrise", "sunset"))
  sun_all$.key <- paste(sun_all$date, sun_all$lat, sun_all$lon, sep = "|")

  .lookup <- function(d, lat, lon, field) {
    idx <- match(paste(d, lat, lon, sep = "|"), sun_all$.key)
    sun_all[[field]][idx]
  }

  lapply(seq_len(n), function(i) {
    t0    <- ts[i]
    start <- t0 - lubridate::hours(24)

    sr_today <- .lookup(dt[i],  latitudes[i], longitudes[i], "sunrise")
    ss_today <- .lookup(dt[i],  latitudes[i], longitudes[i], "sunset")
    ss_yday  <- .lookup(dt1[i], latitudes[i], longitudes[i], "sunset")

    night1_start <- max(ss_yday,  start, na.rm = FALSE)
    night1_end   <- min(sr_today, t0,    na.rm = FALSE)
    night2_start <- max(ss_today, start, na.rm = FALSE)
    night2_end   <- t0

    hours_seq <- seq(from = start, to = t0, by = "hour")
    keep <- rep(FALSE, length(hours_seq))
    if (!is.na(night1_start) && !is.na(night1_end) && night1_start < night1_end)
      keep <- keep | (hours_seq >= night1_start & hours_seq <= night1_end)
    if (!is.na(night2_start) && !is.na(night2_end) && night2_start < night2_end)
      keep <- keep | (hours_seq >= night2_start & hours_seq <= night2_end)

    lubridate::round_date(hours_seq[keep], unit = "hour")
  })
}


#' Annotate a move2/sf object with night-averaged ERA5 single-level data
#'
#' For each offset in \code{offsets_days}, computes the suncalc-derived
#' nighttime hour sequence ending at (fix time + offset), looks up every
#' hourly ERA5 single-level step in that window from the monthly
#' \code{era5_dir/single_levels/} GRIB files, and averages back to one row
#' per fix (total precipitation is summed, not averaged, since it's additive).
#'
#' @param data         A \code{move2} or \code{sf} object with timestamps.
#' @param era5_dir     Path to the EnvData directory (parent of
#'   \code{single_levels/}).
#' @param offsets_days Numeric vector of day offsets, e.g. \code{-2:2}.
#' @param lon_col,lat_col Optional column names to use as the extraction
#'   location instead of \code{data}'s own geometry (e.g. \code{"lon_prev"},
#'   \code{"lat_prev"} to annotate the *preceding* night's location rather
#'   than the current fix).
#' @param tz           Timezone for timestamp handling.
#' @param coord_crs    CRS of the extraction coordinates.
#' @param verbose      Logical; print progress messages?
#' @return \code{data} with per-offset night-averaged columns
#'   (\code{u10_-48h}, ..., \code{cbh_48h}) plus \code{night_n_-48h} etc.
#'   (number of night-hours averaged into that offset).
#' @export
annotate_era5_avg_night_many_offsets <- function(
    data,
    era5_dir,
    offsets_days,
    lon_col = NULL,
    lat_col = NULL,
    tz = "UTC",
    coord_crs = "EPSG:4326",
    verbose = TRUE
) {
  require(terra)
  require(lubridate)
  require(dplyr)
  require(sf)

  stopifnot(inherits(data, "sf"))

  single_dir <- file.path(era5_dir, "single_levels")
  if (!dir.exists(single_dir)) stop("No single_levels/ folder found under ", era5_dir)

  base_time <- if (inherits(data, "move2")) move2::mt_time(data) else data$timestamp
  if (is.null(base_time)) stop("Cannot find timestamps in data.")

  attr_df <- sf::st_drop_geometry(data)
  if (!is.null(lon_col) && !is.null(lat_col)) {
    stopifnot(lon_col %in% names(attr_df), lat_col %in% names(attr_df))
    lon <- as.numeric(attr_df[[lon_col]])
    lat <- as.numeric(attr_df[[lat_col]])
  } else {
    coords <- sf::st_coordinates(data)
    lon <- coords[, 1]
    lat <- coords[, 2]
  }
  n   <- nrow(data)

  var_positions <- .era5_single_var_positions()
  n_var    <- length(var_positions)
  var_cols <- names(var_positions)

  out <- data

  for (d in offsets_days) {
    if (verbose) message("Night offset days: ", d)
    shifted_time <- as.POSIXct(base_time, tz = tz) + d * 86400

    night_hours <- .era5_night_hours_24h(shifted_time, lat, lon, tz = tz)

    rows <- lapply(seq_len(n), function(i) {
      nh <- night_hours[[i]]
      if (length(nh) == 0L) return(NULL)
      data.frame(
        .row_id    = i,
        night_hour = nh,
        lon        = lon[i],
        lat        = lat[i],
        ym         = format(nh, "%Y-%m"),
        stringsAsFactors = FALSE
      )
    })
    long_tbl <- dplyr::bind_rows(rows)

    if (nrow(long_tbl) == 0L) {
      if (verbose) message("  No night hours found for any fix at this offset.")
      next
    }

    for (nm in var_cols) long_tbl[[nm]] <- NA_real_

    yms_needed <- sort(unique(long_tbl$ym))
    if (verbose) message("  Year-months needed: ", paste(yms_needed, collapse = ", "))

    for (ym in yms_needed) {
      f <- file.path(single_dir, paste0("era5_single_", gsub("-", "_", ym), ".grib"))
      if (!file.exists(f)) {
        if (verbose) message("  [skip] no file for ", ym)
        next
      }

      r  <- terra::rast(f)
      rt <- as.POSIXct(terra::time(r), tz = tz)
      if (is.null(rt) || length(rt) == 0) {
        if (verbose) message("  [skip] ", ym, " has no time vector.")
        next
      }
      rt_hour   <- lubridate::round_date(rt, "hour")
      rt_unique <- sort(unique(rt_hour))

      actual_block <- terra::nlyr(r) / length(rt_unique)
      if (actual_block %% 1 != 0 || actual_block != n_var) {
        stop("Raster for ", ym, ": ", terra::nlyr(r), " layers / ", length(rt_unique),
             " timestamps = ", actual_block, " layers/hour, but expected ", n_var,
             " (u10,v10,u100,v100,t2m,msl,sp,tp,i10fg,tcc,cbh). ",
             "This file's variable layout no longer matches .era5_single_var_positions().")
      }

      sub_idx <- which(long_tbl$ym == ym)
      t_round <- lubridate::round_date(as.POSIXct(long_tbl$night_hour[sub_idx], tz = tz), "hour")
      hour_idx <- match(as.numeric(t_round), as.numeric(rt_unique))

      ok <- which(!is.na(hour_idx))
      if (length(ok) == 0L) {
        if (verbose) message("  No matching raster hours for ", ym)
        next
      }

      sub_ok      <- sub_idx[ok]
      hour_idx_ok <- hour_idx[ok]
      uniq_hours  <- sort(unique(hour_idx_ok))

      pts <- terra::vect(
        data.frame(x = long_tbl$lon[sub_ok], y = long_tbl$lat[sub_ok]),
        geom = c("x", "y"), crs = coord_crs
      )
      r_crs <- terra::crs(r, proj = TRUE)
      if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) pts <- terra::project(pts, r_crs)

      layer_blocks <- lapply(uniq_hours, function(h) ((h - 1L) * n_var + 1L):(h * n_var))
      all_layers   <- unlist(layer_blocks)

      if (verbose) {
        message("  ", ym, ": rows=", length(sub_ok), ", unique hours=", length(uniq_hours),
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
      long_tbl[sub_ok, var_cols] <- out_mat

      rm(r); gc(verbose = FALSE)
    }

    # aggregate to one row per fix: mean for most vars, sum for precipitation
    avg_tbl <- long_tbl |>
      dplyr::group_by(.row_id) |>
      dplyr::summarise(
        night_n = dplyr::n(),
        dplyr::across(dplyr::all_of(setdiff(var_cols, "era5_tp")), ~ mean(.x, na.rm = TRUE)),
        era5_tp = sum(era5_tp, na.rm = TRUE),
        .groups = "drop"
      )

    m <- match(seq_len(n), avg_tbl$.row_id)
    suffix <- paste0("_", d * 24, "h")
    for (nm in var_cols) {
      base_name <- sub("^era5_", "", nm)
      out[[paste0(base_name, suffix)]] <- avg_tbl[[nm]][m]
    }
    out[[paste0("night_n", suffix)]] <- avg_tbl$night_n[m]
  }

  out
}
