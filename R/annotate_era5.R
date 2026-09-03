# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5.R
# ─────────────────────────────────────────────────────────────────────────────
# Annotate move2/sf objects with ERA5 reanalysis weather data. Consolidates
# every ERA5 annotation mode into one file, all sharing the same internal
# helpers (.era5_detect_col, .era5_single_var_positions, .era5_wind_support,
# .era5_flight_pressure, ...) so they can't drift out of sync with each other
# or with the monthly file layout produced by inst/python/download_era5.py.
#
# Public functions, by use case:
#
#   annotate_era5(data, era5_dir, ...)
#     Instantaneous, single fix: nearest ERA5 step to the fix's own timestamp.
#     Single-level + pressure-level extraction, wind support / crosswind /
#     airspeed / flight-pressure matching. The engine every other function
#     below wraps.
#
#   annotate_era5_offsets(data, era5_dir, offsets_days, ...)
#     Instantaneous, at multiple day offsets from the fix (e.g. u10_-48h,
#     u10_-24h, u10_0h, u10_24h, u10_48h for offsets_days = -2:2). Re-runs
#     annotate_era5() at each shifted timestamp. Only the day-0 pass computes
#     wind_support/crosswind/airspeed/flight-pressure (heading and ground
#     speed come from fix-to-fix geometry and are invariant to a uniform
#     timestamp shift).
#
#   annotate_era5_night_avg(data, era5_dir, offsets_days, ...)
#     Night-averaged, at multiple day offsets: for each offset, averages every
#     hourly ERA5 single-level step across the preceding night (suncalc
#     sunset->sunrise, up to 24h back) into one row per fix. Replaces
#     extract_avg_night_env_from_year_stacks().
#
#   annotate_era5_sunset(data, era5_dir, shift_hours, ...)
#     Beginning-of-night: looks up the ERA5 step nearest sunset (+1h +
#     shift_hours) at the fix's own location, falling back to the *previous*
#     fix's location when that sunset has already passed. Replaces
#     extract_sunset_env().
#
#   annotate_era5_gee(data, ...)
#     Same variables as annotate_era5(), but from the Google Earth Engine
#     catalog via python/annotate_era5_gee.py (system2()) instead of local
#     monthly GRIB files. Use when there's no local EnvData folder.
#
# Reuses existing SigfoxTagPrep functions:
#   - wind_support(), cross_wind(), airspeed()  (calculate_wind_features.R)
#   - pressure_to_altitude_m()                  (pressure_to_altitude_m.R)
# ─────────────────────────────────────────────────────────────────────────────


# ── Inverse of pressure_to_altitude_m() ──────────────────────────────────────

#' Convert altitude to atmospheric pressure using the ISA hypsometric formula
#'
#' Inverse of \code{\link{pressure_to_altitude_m}}.
#' \deqn{P = P_0 \times (1 - z / 44330)^{5.2558}}
#'
#' @param alt_m   Numeric vector of altitudes in metres.
#' @param p0_hpa  Reference sea-level pressure in hPa.  When ERA5 MSLP is
#'   available this should be a per-location vector; otherwise it defaults to
#'   ISA standard 1013.25 hPa.
#' @return Numeric vector of pressures in hPa.
#' @seealso \code{\link{pressure_to_altitude_m}}
#' @export
altitude_to_pressure_hPa <- function(alt_m, p0_hpa = 1013.25) {
  alt_m  <- as.numeric(alt_m)
  p0_hpa <- as.numeric(p0_hpa)
  out <- rep(NA_real_, length(alt_m))
  ok  <- which(is.finite(alt_m) & is.finite(p0_hpa) & p0_hpa > 0)
  out[ok] <- p0_hpa[ok] * (1 - alt_m[ok] / 44330)^5.2558
  out
}


# ═══════════════════════════════════════════════════════════════════════════
# annotate_era5() — instantaneous, single fix
# ═══════════════════════════════════════════════════════════════════════════

#' Annotate a move2 object with ERA5 reanalysis weather data
#'
#' Extracts ERA5 single-level and pressure-level variables at each animal
#' location and nearest timestep from the NetCDF folder structure produced by
#' \code{inst/python/download_era5.py}.  Optionally computes wind support,
#' crosswind, airspeed, and altitude-matched wind at the flight pressure level.
#'
#' @section Sensor compatibility:
#' The function auto-detects the altitude source per row:
#' \itemize{
#'   \item \strong{GPS tags}: uses altitude column + actual ERA5 MSLP in the
#'     barometric formula (no ISA standard-atmosphere assumption).
#'   \item \strong{Sigfox/TinyFox tags}: uses barometric pressure from the tag
#'     directly — no altitude conversion needed.
#'   \item \strong{Mixed datasets}: handles both per-row via the priority
#'     tag-pressure > GPS-altitude.
#' }
#'
#' @section Wind support convention:
#' Positive = tailwind (wind pushing in the direction of travel).
#' Negative = headwind.
#' Crosswind positive = wind from the left.
#' Heading is the azimuth of the \emph{preceding} segment (lagged within
#' each track).
#'
#' @param data           A \code{move2} or \code{sf} object with timestamps.
#' @param era5_dir       Path to directory with ERA5 NetCDF files (e.g.
#'   \code{"//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"}).
#' @param pressure_levels Numeric vector of pressure levels (hPa) to extract.
#'   Must match the levels present in \code{era5_dir/pressure_levels/}.
#' @param altitude_col   Column name holding altitude in metres
#'   (GPS data).  \code{NULL} = auto-detect.
#' @param tag_pressure_col Column name holding barometric pressure from the tag
#'   in hPa (Sigfox data).  \code{NULL} = auto-detect.
#' @param compute_wind_support Logical; compute tailwind, crosswind, airspeed?
#' @param max_time_gap_hours Warn when nearest ERA5 timestep exceeds this
#'   distance from the animal fix.
#' @param verbose Logical; print progress messages?
#'
#' @return The input object with new columns appended (see README for full list).
#'
#' @examples
#' \dontrun{
#'   library(move2)
#'   source("R/calculate_wind_features.R")
#'   source("R/pressure_to_altitude_m.R")
#'
#'   dat <- movebank_download_study(study_id = 12345)
#'   dat <- annotate_era5(
#'     dat,
#'     era5_dir = "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"
#'   )
#'
#'   # Mixed GPS + Sigfox
#'   combined <- mt_stack(gps_data, sigfox_data)
#'   combined <- annotate_era5(combined, era5_dir = "path/to/era5_data")
#' }
#'
#' @importFrom move2 mt_time mt_azimuth mt_track_id_column mt_speed
#' @importFrom sf st_coordinates st_is_empty
#' @importFrom terra rast time extract nlyr
#' @importFrom dplyr mutate group_by ungroup lag across all_of
#' @export
annotate_era5 <- function(
    data,
    era5_dir,
    pressure_levels  = c(500, 600, 700, 800, 850, 900, 925, 950, 1000),
    altitude_col     = NULL,
    tag_pressure_col = NULL,
    compute_wind_support = TRUE,
    max_time_gap_hours   = 3,
    verbose = TRUE
) {

  require(terra)

  # ── Validate ──────────────────────────────────────────────────────────────
  era5_dir <- gsub("\\\\", "/", era5_dir)   # UNC paths need forward slashes for GDAL
  stopifnot(inherits(data, "sf"))
  stopifnot(dir.exists(era5_dir))
  n <- nrow(data)
  if (n == 0) { warning("Input data has 0 rows."); return(data) }

  # ── Auto-detect columns ──────────────────────────────────────────────────
  altitude_col <- .era5_detect_col(
    data, altitude_col,
    c("height_raw", "altitude", "altitude_m", "altitude_sea",
      "altitude.sea", "height_above_msl"),
    "altitude", verbose
  )
  tag_pressure_col <- .era5_detect_col(
    data, tag_pressure_col,
    c("tinyfox_pressure_min_last_24h", "min_3h_pressure",
      "tag_pressure", "barometric_pressure", "pressure_hpa_used"),
    "tag pressure", verbose
  )

  # ── Timestamps ────────────────────────────────────────────────────────────
  timestamps <- if (inherits(data, "move2")) mt_time(data) else data$timestamp
  if (is.null(timestamps)) stop("Cannot find timestamps in data.")
  coords_mat <- sf::st_coordinates(data)

  # ── Single-level extraction ───────────────────────────────────────────────
  single_dir <- file.path(era5_dir, "single_levels")
  if (dir.exists(single_dir)) {
    grib_files <- sort(list.files(single_dir, "\\.grib$", full.names = TRUE))
    if (length(grib_files) > 0) {
      if (verbose) message("Extracting single-level variables ...")
      data <- .era5_extract_single(data, grib_files, timestamps,
                                    coords_mat, max_time_gap_hours, verbose)
    }
  } else if (verbose) {
    message("No single_levels/ folder found — skipping.")
  }

  # ── Pressure-level extraction ─────────────────────────────────────────────
  pressure_dir <- file.path(era5_dir, "pressure_levels")
  if (dir.exists(pressure_dir)) {
    if (verbose) message("Extracting pressure-level wind ...")
    data <- .era5_extract_pressure(data, pressure_dir, timestamps,
                                    coords_mat, pressure_levels,
                                    max_time_gap_hours, verbose)
  } else if (verbose) {
    message("No pressure_levels/ folder found — skipping.")
  }

  # ── Wind support ──────────────────────────────────────────────────────────
  if (compute_wind_support) {
    if (verbose) message("Computing wind support ...")
    data <- .era5_wind_support(data, pressure_levels,
                                altitude_col, tag_pressure_col)
  }

  if (verbose) message("Annotation complete.")
  return(data)
}


# ═══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS (prefixed .era5_ to avoid collisions) — shared by every
# public function in this file
# ═══════════════════════════════════════════════════════════════════════════

.era5_detect_col <- function(data, user_choice, candidates, label, verbose) {
  if (!is.null(user_choice)) {
    if (user_choice %in% names(data)) return(user_choice)
    if (verbose) message("  Column '", user_choice, "' not found.")
    return(NULL)
  }
  hit <- intersect(candidates, names(data))
  if (length(hit) > 0) {
    if (verbose) message("  Auto-detected ", label, " column: ", hit[1])
    return(hit[1])
  }
  NULL
}


# Haversine bearing (degrees, 0–360) from (lon1, lat1) to (lon2, lat2).
# Returns NA wherever either endpoint is missing.
.era5_bearing <- function(lon1, lat1, lon2, lat2) {
  to_rad <- pi / 180
  d_lon  <- (lon2 - lon1) * to_rad
  lat1r  <- lat1 * to_rad
  lat2r  <- lat2 * to_rad
  y   <- sin(d_lon) * cos(lat2r)
  x   <- cos(lat1r) * sin(lat2r) - sin(lat1r) * cos(lat2r) * cos(d_lon)
  out <- (atan2(y, x) / to_rad + 360) %% 360
  out[is.na(lon1) | is.na(lat1) | is.na(lon2) | is.na(lat2)] <- NA_real_
  out
}


# ── Single-level extraction ──────────────────────────────────────────────────

#' GRIB layer positions within each time-step block for single-level files.
#'
#' Order confirmed via eccodes against era5_single_YYYY_MM.grib — positions
#' 3, 4, 11 (u100, v100, cbh) come back as "undefined" from GDAL's grib
#' driver, so they must be identified by position, not name.
#' @keywords internal
.era5_single_var_positions <- function() {
  c(
    era5_u10   = 1L,  # 10 metre u wind component
    era5_v10   = 2L,  # 10 metre v wind component
    era5_u100  = 3L,  # 100 metre u wind component (listed as "undefined")
    era5_v100  = 4L,  # 100 metre v wind component (listed as "undefined")
    era5_t2m   = 5L,  # 2 metre temperature
    era5_msl   = 6L,  # Mean sea level pressure
    era5_sp    = 7L,  # Surface pressure
    era5_tp    = 8L,  # Total precipitation
    era5_i10fg = 9L,  # Instantaneous 10 metre wind gust
    era5_tcc   = 10L, # Total cloud cover
    era5_cbh   = 11L  # Cloud base height (listed as "undefined")
  )
}

.era5_extract_single <- function(data, grib_files, timestamps,
                                  coords_mat, max_gap, verbose) {
  var_positions <- .era5_single_var_positions()

  grib_files <- gsub("\\\\", "/", grib_files)
  coords_xy  <- coords_mat[, 1:2, drop = FALSE]

  # ── Step 1: metadata scan — one open per file, keep only timestamps ─────────
  if (verbose) message("  Scanning ", length(grib_files), " single-level file(s) ...")
  catalog <- vector("list", length(grib_files))
  for (fi in seq_along(grib_files)) {
    r_tmp <- tryCatch(rast(grib_files[fi]), error = function(e) NULL)
    if (is.null(r_tmp)) next
    rt <- time(r_tmp)
    rm(r_tmp); gc(verbose = FALSE)
    if (length(rt) == 0) next
    n_v <- sum(rt == rt[1])
    ut  <- rt[seq(1L, length(rt), by = n_v)]
    catalog[[fi]] <- list(file = grib_files[fi], n_vars = n_v, unique_times = ut)
  }
  catalog <- Filter(Negate(is.null), catalog)
  if (length(catalog) == 0) return(data)

  # ── Step 2: build global time index across all files ─────────────────────────
  all_times_num      <- as.numeric(do.call(c, lapply(catalog, `[[`, "unique_times")))
  file_for_global    <- rep(seq_along(catalog),
                            vapply(catalog, function(x) length(x$unique_times), integer(1)))
  local_t_for_global <- unlist(lapply(catalog, function(x) seq_along(x$unique_times)),
                                use.names = FALSE)

  obs_num    <- as.numeric(timestamps)
  nearest_gi <- vapply(obs_num, function(t) which.min(abs(all_times_num - t)), integer(1))

  obs_file_idx <- file_for_global[nearest_gi]
  obs_time_idx <- local_t_for_global[nearest_gi]   # 1-based within that file

  # ── Step 3: time-gap warning ──────────────────────────────────────────────────
  gaps_h <- abs(all_times_num[nearest_gi] - obs_num) / 3600
  bad <- which(gaps_h > max_gap)
  if (length(bad) > 0 && verbose) {
    message("    [warn] ", length(bad), " fixes >", max_gap,
            "h from nearest ERA5 step (max gap: ",
            round(max(gaps_h[bad]), 1), "h)")
  }

  # ── Step 4: extract one file at a time ───────────────────────────────────────
  for (fi in seq_along(catalog)) {
    obs_mask <- obs_file_idx == fi
    if (!any(obs_mask)) next

    cat_entry <- catalog[[fi]]
    r <- tryCatch(rast(cat_entry$file), error = function(e) {
      if (verbose) message("    [skip] Could not load: ", basename(cat_entry$file))
      NULL
    })
    if (is.null(r)) next

    n_vars     <- cat_entry$n_vars
    obs_here   <- which(obs_mask)
    t_idx_here <- obs_time_idx[obs_mask]

    for (col_nm in names(var_positions)) {
      pos <- var_positions[[col_nm]]
      if (pos > n_vars) next
      if (!col_nm %in% names(data)) data[[col_nm]] <- rep(NA_real_, nrow(data))

      layer_idx <- (t_idx_here - 1L) * n_vars + pos
      for (li in unique(layer_idx)) {
        rows_li <- obs_here[layer_idx == li]
        ex <- tryCatch(
          terra::extract(r[[li]], coords_xy[rows_li, , drop = FALSE]),
          error = function(e) {
            if (verbose) message("    [warn] layer ", li, " unreadable — skipping (", conditionMessage(e), ")")
            NULL
          }
        )
        if (!is.null(ex) && ncol(ex) >= 1) data[[col_nm]][rows_li] <- ex[[1]]
      }
    }

    if (verbose) message("    [single] file ", fi, "/", length(catalog), ": ",
                         basename(cat_entry$file), " (", sum(obs_mask), " obs)")
    rm(r); gc(verbose = FALSE)
  }

  if (verbose) {
    for (col_nm in names(var_positions)) {
      if (col_nm %in% names(data)) message("    + ", col_nm)
    }
  }

  data
}


# ── Pressure-level extraction ────────────────────────────────────────────────

.era5_extract_pressure <- function(data, pressure_dir, timestamps,
                                    coords_mat, pressure_levels,
                                    max_gap, verbose) {

  coords_xy    <- coords_mat[, 1:2, drop = FALSE]
  obs_num      <- as.numeric(timestamps)
  reported_gap <- FALSE

  if (verbose) message("  Pressure levels: ", paste(pressure_levels, collapse = ", "), " hPa")

  for (level in pressure_levels) {
    pattern     <- paste0("era5_wind_", level, "hPa_.*\\.grib$")
    level_files <- gsub("\\\\", "/",
                        sort(list.files(pressure_dir, pattern, full.names = TRUE)))

    if (length(level_files) == 0) {
      if (verbose) message("    [skip] No files for ", level, " hPa")
      next
    }

    # ── Metadata scan for this level ───────────────────────────────────────────
    catalog <- vector("list", length(level_files))
    for (fi in seq_along(level_files)) {
      r_tmp <- tryCatch(rast(level_files[fi]), error = function(e) NULL)
      if (is.null(r_tmp)) next
      rt  <- time(r_tmp)
      lnm <- names(r_tmp)
      rm(r_tmp); gc(verbose = FALSE)
      if (length(rt) == 0) next
      n_v <- sum(rt == rt[1])
      ut  <- rt[seq(1L, length(rt), by = n_v)]
      catalog[[fi]] <- list(file = level_files[fi], n_vars = n_v,
                             unique_times = ut, layer_names = lnm[seq_len(n_v)])
    }
    catalog <- Filter(Negate(is.null), catalog)
    if (length(catalog) == 0) {
      if (verbose) message("    [skip] Could not read any files for ", level, " hPa")
      next
    }

    # ── Global time index for this level ───────────────────────────────────────
    all_times_num      <- as.numeric(do.call(c, lapply(catalog, `[[`, "unique_times")))
    file_for_global    <- rep(seq_along(catalog),
                              vapply(catalog, function(x) length(x$unique_times), integer(1)))
    local_t_for_global <- unlist(lapply(catalog, function(x) seq_along(x$unique_times)),
                                  use.names = FALSE)

    nearest_gi   <- vapply(obs_num, function(t) which.min(abs(all_times_num - t)), integer(1))
    obs_file_idx <- file_for_global[nearest_gi]
    obs_time_idx <- local_t_for_global[nearest_gi]

    if (!reported_gap) {
      gaps_h <- abs(all_times_num[nearest_gi] - obs_num) / 3600
      bad <- which(gaps_h > max_gap)
      if (length(bad) > 0 && verbose) {
        message("    [warn] ", length(bad), " fixes >", max_gap,
                "h from nearest ERA5 step (max gap: ",
                round(max(gaps_h[bad]), 1), "h)")
      }
      reported_gap <- TRUE
    }

    # u/v positions from first file's layer names (consistent across files)
    layer_names <- catalog[[1]]$layer_names
    u_pos <- grep("(?i)(^u$|U-velocity|U_velocity|\\bu.{0,10}wind|u.{0,5}component)",
                  layer_names, perl = TRUE)[1]
    v_pos <- grep("(?i)(^v$|V-velocity|V_velocity|\\bv.{0,10}wind|v.{0,5}component)",
                  layer_names, perl = TRUE)[1]

    if (verbose && (is.na(u_pos) || is.na(v_pos)))
      message("    [debug] layer names: ", paste(layer_names, collapse = ", "))

    # ── Extract one file at a time ─────────────────────────────────────────────
    for (fi in seq_along(catalog)) {
      obs_mask <- obs_file_idx == fi
      if (!any(obs_mask)) next

      cat_entry <- catalog[[fi]]
      r <- tryCatch(rast(cat_entry$file), error = function(e) {
        if (verbose) message("    [skip] Could not load: ", basename(cat_entry$file))
        NULL
      })
      if (is.null(r)) next

      n_vars     <- cat_entry$n_vars
      obs_here   <- which(obs_mask)
      t_idx_here <- obs_time_idx[obs_mask]

      for (var_info in list(list(pos = u_pos, nm = "u"),
                            list(pos = v_pos, nm = "v"))) {
        pos    <- var_info$pos
        col_nm <- paste0("era5_", var_info$nm, level)

        if (is.na(pos)) {
          if (verbose && fi == 1L)
            message("    [skip] ", var_info$nm, " at ", level, " hPa — layer not found")
          next
        }
        if (!col_nm %in% names(data)) data[[col_nm]] <- rep(NA_real_, nrow(data))

        layer_idx <- (t_idx_here - 1L) * n_vars + pos
        for (li in unique(layer_idx)) {
          rows_li <- obs_here[layer_idx == li]
          ex <- tryCatch(
            terra::extract(r[[li]], coords_xy[rows_li, , drop = FALSE]),
            error = function(e) {
              if (verbose) message("    [warn] layer ", li, " unreadable — skipping (", conditionMessage(e), ")")
              NULL
            }
          )
          if (!is.null(ex) && ncol(ex) >= 1) data[[col_nm]][rows_li] <- ex[[1]]
        }
      }

      if (verbose) message("    [", level, " hPa] file ", fi, "/", length(catalog),
                           ": ", basename(cat_entry$file), " (", sum(obs_mask), " obs)")
      rm(r); gc(verbose = FALSE)
    }

    if (verbose) {
      u_nm <- paste0("era5_u", level); v_nm <- paste0("era5_v", level)
      if (u_nm %in% names(data)) message("    + ", u_nm)
      if (v_nm %in% names(data)) message("    + ", v_nm)
    }
  }

  data
}


# ── Wind support & altitude matching ─────────────────────────────────────────

.era5_wind_support <- function(data, pressure_levels,
                                altitude_col, tag_pressure_col) {

  n <- nrow(data)

  # ── Heading (bearing of preceding segment, degrees 0–360) ──────────────
  # Prefer lon_prev/lat_prev → lon/lat (daily solar-noon data): direct
  # bearing with no lag needed and robust to empty geometries.
  # Falls back to mt_azimuth() on the non-empty subset only.
  coords <- sf::st_coordinates(data)
  lon    <- coords[, 1]
  lat    <- coords[, 2]

  if (all(c("lon_prev", "lat_prev") %in% names(data))) {
    heading <- .era5_bearing(
      as.numeric(data$lon_prev), as.numeric(data$lat_prev), lon, lat
    )
  } else if (inherits(data, "move2")) {
    non_empty <- !sf::st_is_empty(data)
    heading   <- rep(NA_real_, n)
    if (any(non_empty)) {
      sub    <- data[non_empty, ]
      tid_col <- mt_track_id_column(sub)
      az <- tryCatch(as.numeric(mt_azimuth(sub)),
                     error = function(e) rep(NA_real_, sum(non_empty)))
      sub$.az <- az
      sub <- sub %>%
        dplyr::group_by(dplyr::across(dplyr::all_of(tid_col))) %>%
        dplyr::mutate(.az = dplyr::lag(.az)) %>%
        dplyr::ungroup()
      heading[non_empty] <- as.numeric(sub$.az)
    }
  } else {
    heading <- rep(NA_real_, n)
  }

  # ── Ground speed (m/s, preceding segment) ──────────────────────────────
  # Prefer dist_prev (km) / diff_date (days): directly encodes the overnight
  # displacement without being contaminated by empty-point gaps.
  if (all(c("dist_prev", "diff_date") %in% names(data))) {
    dist_m       <- as.numeric(data$dist_prev) * 1000   # km → m
    time_s       <- as.numeric(data$diff_date) * 86400  # days → s
    ground_speed <- ifelse(!is.na(dist_m) & time_s > 0,
                           dist_m / time_s, NA_real_)
  } else if (inherits(data, "move2")) {
    non_empty    <- !sf::st_is_empty(data)
    ground_speed <- rep(NA_real_, n)
    if (any(non_empty)) {
      sub    <- data[non_empty, ]
      tid_col <- mt_track_id_column(sub)
      gs <- tryCatch(as.numeric(mt_speed(sub)),
                     error = function(e) rep(NA_real_, sum(non_empty)))
      sub$.gs <- gs
      sub <- sub %>%
        dplyr::group_by(dplyr::across(dplyr::all_of(tid_col))) %>%
        dplyr::mutate(.gs = dplyr::lag(.gs)) %>%
        dplyr::ungroup()
      ground_speed[non_empty] <- as.numeric(sub$.gs)
    }
  } else {
    ground_speed <- rep(NA_real_, n)
  }

  # ── Per-level wind support (reuses package functions) ──────────────────
  for (level in pressure_levels) {
    u_col <- paste0("era5_u", level)
    v_col <- paste0("era5_v", level)
    if (!all(c(u_col, v_col) %in% names(data))) next

    u <- as.numeric(data[[u_col]])
    v <- as.numeric(data[[v_col]])

    ws  <- wind_support(u, v, heading)
    cw  <- cross_wind(u, v, heading)
    spd <- sqrt(u^2 + v^2)

    data[[paste0("wind_speed_",   level)]] <- spd
    data[[paste0("wind_support_", level)]] <- ws
    data[[paste0("crosswind_",    level)]] <- cw
    data[[paste0("airspeed_",     level)]] <- airspeed(as.numeric(ground_speed), ws, cw)
  }

  # Surface winds (10 m / 100 m)
  for (h in c("10", "100")) {
    u_col <- paste0("era5_u", h)
    v_col <- paste0("era5_v", h)
    if (!all(c(u_col, v_col) %in% names(data))) next

    u   <- as.numeric(data[[u_col]])
    v   <- as.numeric(data[[v_col]])
    sfx <- paste0(h, "m")
    data[[paste0("wind_speed_",   sfx)]] <- sqrt(u^2 + v^2)
    data[[paste0("wind_support_", sfx)]] <- wind_support(u, v, heading)
    data[[paste0("crosswind_",    sfx)]] <- cross_wind(u, v, heading)
  }

  # ── Flight-level matching ──────────────────────────────────────────────
  flight_p <- .era5_flight_pressure(data, altitude_col, tag_pressure_col)

  if (!is.null(flight_p)) {
    data$flight_pressure_hPa <- flight_p

    # Nearest standard pressure level.
    # vapply handles NA flight_p correctly: which.min(all-NA) returns integer(0),
    # which makes apply() return a list and crash with 'invalid subscript type'.
    nearest_idx <- vapply(flight_p, function(fp) {
      if (is.na(fp)) NA_integer_ else which.min(abs(pressure_levels - fp))
    }, integer(1))
    nearest_level <- ifelse(is.na(nearest_idx), NA_real_, pressure_levels[nearest_idx])
    data$matched_pressure_level <- nearest_level

    # Collect wind columns across available levels
    ws_cols  <- paste0("wind_support_", pressure_levels)
    cs_cols  <- paste0("crosswind_",    pressure_levels)
    spd_cols <- paste0("wind_speed_",   pressure_levels)
    avail    <- ws_cols %in% names(data)

    if (any(avail)) {
      # Drop sf geometry before matrix-building: the sticky geometry column
      # (or any non-double column, e.g. class "units") would otherwise make
      # as.matrix() fall back to format()-based stringification, turning the
      # whole matrix to character and crashing the vapply() calls below.
      # Coerce column-by-column with as.numeric() instead of as.matrix() so a
      # single contaminated column can't silently poison the others.
      .data_df <- sf::st_drop_geometry(data)
      .num_mat <- function(df, cols) {
        m <- vapply(cols, function(cn) as.numeric(df[[cn]]), numeric(nrow(df)))
        matrix(m, nrow = nrow(df), ncol = length(cols), dimnames = list(NULL, cols))
      }
      ws_mat  <- .num_mat(.data_df, ws_cols[avail])
      cs_mat  <- .num_mat(.data_df, cs_cols[avail])
      spd_mat <- .num_mat(.data_df, spd_cols[avail])

      matched_col <- match(nearest_level, pressure_levels[avail])
      idx <- cbind(seq_len(n), matched_col)

      # Extract to plain numeric vectors before assigning to sf/move2 columns.
      # Accessing the column back through `data$` after assignment can silently
      # coerce to character via the sticky geometry mechanism; using local
      # variables avoids the round-trip entirely.
      ws_flight  <- as.numeric(ws_mat[idx])
      cs_flight  <- as.numeric(cs_mat[idx])
      spd_flight <- as.numeric(spd_mat[idx])

      data$wind_support_flight <- ws_flight
      data$crosswind_flight    <- cs_flight
      data$wind_speed_flight   <- spd_flight
      data$airspeed_flight     <- airspeed(
        as.numeric(ground_speed),
        ws_flight,
        cs_flight
      )

      # Best available wind level — vapply ensures length-1 integer per row even
      # when all ws values are NA (apply() would return integer(0) and produce a
      # list, causing list subscript crash / garbage memory values).
      best_col <- vapply(seq_len(nrow(ws_mat)), function(i) {
        row <- ws_mat[i, ]
        if (all(is.na(row))) NA_integer_ else which.max(row)
      }, integer(1))
      data$best_wind_level   <- pressure_levels[avail][best_col]
      data$best_wind_support <- vapply(seq_len(n), function(i) {
        j <- best_col[i]
        if (is.na(j)) NA_real_ else ws_mat[i, j]
      }, numeric(1))
      data$at_best_wind      <- nearest_level == data$best_wind_level
    }
  }

  data
}


# ── Flight pressure estimation ───────────────────────────────────────────────

.era5_flight_pressure <- function(data, altitude_col, tag_pressure_col) {
  n <- nrow(data)
  flight_p  <- rep(NA_real_, n)
  has_value <- rep(FALSE, n)

  # Priority 1: tag barometric pressure (Sigfox / NanoFox)
  if (!is.null(tag_pressure_col) && tag_pressure_col %in% names(data)) {
    tag_p <- as.numeric(data[[tag_pressure_col]])
    valid <- !is.na(tag_p) & tag_p > 0
    flight_p[valid]  <- tag_p[valid]
    has_value[valid]  <- TRUE
  }

  # Priority 2: GPS altitude → barometric formula with actual MSLP
  if (!is.null(altitude_col) && altitude_col %in% names(data)) {
    alt_m <- as.numeric(data[[altitude_col]])
    need  <- !has_value & !is.na(alt_m)

    if (any(need)) {
      # Use ERA5 MSLP if available (Pa → hPa), else ISA standard
      if ("era5_msl" %in% names(data)) {
        p0 <- as.numeric(data$era5_msl) / 100
      } else {
        p0 <- rep(1013.25, n)
      }

      p_hPa <- altitude_to_pressure_hPa(alt_m, p0_hpa = p0)
      flight_p[need]  <- p_hPa[need]
      has_value[need]  <- TRUE
    }
  }

  if (!any(has_value)) return(NULL)
  flight_p
}


# ═══════════════════════════════════════════════════════════════════════════
# annotate_era5_offsets() — instantaneous, at multiple day offsets
# ═══════════════════════════════════════════════════════════════════════════

#' Annotate a move2/sf object with ERA5 data at multiple day offsets
#'
#' Wraps \code{\link{annotate_era5}} to support the same ±N-day offset
#' workflow that \code{add_env_many_offsets()} provided for the old yearly
#' GRIB stacks — e.g. \code{u10_-48h}, \code{u10_-24h}, \code{u10_0h},
#' \code{u10_24h}, \code{u10_48h} for \code{offsets_days = -2:2}, ready to feed
#' into \code{\link{calculate_wind_features}}.
#'
#' For each offset, the animal's timestamp is shifted by \code{offset_days}
#' and \code{\link{annotate_era5}} is re-run against that shifted time (same
#' location) to look up the ERA5 step nearest the shifted time. Only the
#' offset (day = 0) pass computes wind_support/crosswind/airspeed/
#' flight-pressure matching, since heading and ground speed are derived from
#' fix-to-fix geometry and are invariant to a uniform timestamp shift —
#' recomputing them per offset would be redundant.
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
annotate_era5_offsets <- function(
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


# ═══════════════════════════════════════════════════════════════════════════
# annotate_era5_night_avg() — night-averaged, at multiple day offsets
# ═══════════════════════════════════════════════════════════════════════════

# ── Night-hour sequence helper (suncalc-based) ──────────────────────────────
# Self-contained copy of extract_nighttime_hours_24h() from
# extract_avg_night_env.R — duplicated (matching that file's own duplication
# precedent) so this function has no dependency beyond what's in this file.
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
#' Night-averaged counterpart to \code{\link{annotate_era5_offsets}}: instead
#' of the single ERA5 step nearest each fix, averages every hourly ERA5 step
#' across the preceding night (suncalc sunset->sunrise, spanning up to 24h
#' back) for each of \code{offsets_days}, using the same single-level
#' variable/position table as \code{\link{annotate_era5}}
#' (\code{.era5_single_var_positions()}, incl. cbh) rather than a
#' hand-maintained var_names list — so it can't drift out of sync with the
#' monthly \code{single_levels/} GRIB layout the way
#' \code{extract_avg_night_env_from_year_stacks()}'s var_names did.
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
annotate_era5_night_avg <- function(
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


# ═══════════════════════════════════════════════════════════════════════════
# annotate_era5_sunset() — beginning-of-night, sunset-referenced
# ═══════════════════════════════════════════════════════════════════════════

#' Annotate a move2/sf object with ERA5 single-level data at sunset (+ offset)
#'
#' Sunset-referenced counterpart to \code{\link{annotate_era5_offsets}}:
#' instead of looking up the ERA5 step nearest the fix time, looks up the
#' step nearest sunset (+1h + \code{shift_hours}) at the fix's own location,
#' falling back to the \emph{previous} fix's location (\code{lon_prev_col}/
#' \code{lat_prev_col}) when that sunset has already passed by the time of
#' the current fix. Uses the same single-level variable/position table as
#' \code{\link{annotate_era5}} (\code{.era5_single_var_positions()}, incl.
#' cbh) rather than a hand-maintained var_names list, and reads from the
#' monthly \code{era5_dir/single_levels/} GRIB files instead of a yearly
#' raster_by_year stack — replaces \code{extract_sunset_env()}.
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
annotate_era5_sunset <- function(
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


# ═══════════════════════════════════════════════════════════════════════════
# annotate_era5_gee() — same variables, sourced from Google Earth Engine
# ═══════════════════════════════════════════════════════════════════════════

#' Annotate a move2/sf object with ERA5 data via Google Earth Engine
#'
#' Calls \code{python/annotate_era5_gee.py} using \code{system2()} to extract
#' ERA5 variables from the GEE data catalog.  No local ERA5 files or rgee
#' required — only \pkg{earthengine-api} and \pkg{pandas} in a Python
#' environment.
#'
#' @section Extracted variables:
#' \describe{
#'   \item{era5_u10, era5_v10}{10 m zonal / meridional wind (m/s)}
#'   \item{era5_u100, era5_v100}{100 m zonal / meridional wind (m/s)}
#'   \item{era5_u500, era5_v500}{500 hPa zonal / meridional wind (m/s)}
#'   \item{era5_u850, era5_v850}{850 hPa zonal / meridional wind (m/s)}
#'   \item{era5_t2m}{2 m temperature (K)}
#'   \item{era5_msl}{Mean sea-level pressure (Pa)}
#'   \item{era5_sp}{Surface pressure (Pa)}
#'   \item{era5_tp}{Total precipitation (m)}
#'   \item{era5_i10fg}{Instantaneous 10 m wind gust (m/s)}
#'   \item{era5_tcc}{Total cloud cover (fraction 0–1)}
#'   \item{era5_cbh}{Cloud base height (m)}
#' }
#'
#' @section Python setup:
#' \preformatted{
#' conda activate your_env
#' pip install earthengine-api pandas
#' python -c "import ee; ee.Authenticate()"
#' }
#'
#' @param data             A \code{move2} or \code{sf} object with timestamps.
#' @param python           Path to the Python executable that has
#'   \pkg{earthengine-api} installed.  \code{NULL} = auto-detect from PATH.
#' @param script           Path to \code{annotate_era5_gee.py}.  Defaults to
#'   \code{python/annotate_era5_gee.py} relative to \code{getwd()}.
#' @param gee_project      GEE Cloud project ID (required by newer API
#'   versions).  \code{NULL} = let the script use the stored default.
#' @param altitude_col     Column name with altitude in metres (GPS data).
#' @param tag_pressure_col Column name with barometric pressure from tag (hPa).
#' @param compute_wind_support Logical; compute tailwind/crosswind/airspeed?
#' @param pressure_levels  Pressure levels (hPa) to include in wind support.
#'   Only 500 and 850 are available from GEE; others will be ignored unless
#'   their \code{era5_uXXX}/\code{era5_vXXX} columns are already present.
#' @param max_time_gap_hours Warn when nearest ERA5 hour exceeds this gap.
#' @param verbose          Logical; show Python script output?
#'
#' @return The input object with ERA5 columns appended.
#'
#' @seealso \code{\link{annotate_era5}} for the local GRIB-file version.
#'
#' @examples
#' \dontrun{
#' source("R/annotate_era5.R")
#' source("R/calculate_wind_features.R")
#' source("R/pressure_to_altitude_m.R")
#'
#' dat <- annotate_era5_gee(
#'   data    = leisler,
#'   python  = "C:/Users/Edward/anaconda3/envs/rgee311/python.exe",
#'   verbose = TRUE
#' )
#' }
#'
#' @export
annotate_era5_gee <- function(
    data,
    python              = NULL,
    script              = file.path("python", "annotate_era5_gee.py"),
    gee_project         = NULL,
    altitude_col        = NULL,
    tag_pressure_col    = NULL,
    compute_wind_support = TRUE,
    pressure_levels     = c(500, 850),
    max_time_gap_hours  = 3,
    verbose             = TRUE
) {

  stopifnot(inherits(data, "sf"))
  n <- nrow(data)
  if (n == 0L) { warning("Input has 0 rows."); return(data) }

  # ── Find Python ───────────────────────────────────────────────────────────
  if (is.null(python)) {
    python <- Sys.which("python")
    if (python == "") python <- Sys.which("python3")
    if (python == "")
      stop(
        "Python not found on PATH.\n",
        "Set python = 'C:/Users/Edward/anaconda3/envs/rgee311/python.exe'"
      )
  }
  if (!file.exists(python))
    stop("Python executable not found: ", python)

  if (!file.exists(script))
    stop("Python script not found: ", script,
         "\n  (run from project root, or set script = full path)")

  # ── Column detection ──────────────────────────────────────────────────────
  altitude_col <- .era5_detect_col(
    data, altitude_col,
    c("height_raw", "altitude", "altitude_m", "altitude_sea",
      "altitude.sea", "height_above_msl"),
    "altitude", verbose
  )
  tag_pressure_col <- .era5_detect_col(
    data, tag_pressure_col,
    c("tinyfox_pressure_min_last_24h", "min_3h_pressure",
      "tag_pressure", "barometric_pressure", "pressure_hpa_used"),
    "tag pressure", verbose
  )

  # ── Build a flat CSV for Python ───────────────────────────────────────────
  coords  <- sf::st_coordinates(sf::st_transform(data, 4326))
  df_flat <- sf::st_drop_geometry(data)

  timestamps <- if (inherits(data, "move2")) move2::mt_time(data) else data$timestamp
  if (is.null(timestamps)) stop("Cannot find timestamps in data.")

  df_flat$.longitude <- coords[, 1]
  df_flat$.latitude  <- coords[, 2]
  df_flat$.timestamp <- format(as.POSIXct(timestamps, tz = "UTC"),
                                "%Y-%m-%dT%H:%M:%SZ")
  df_flat$.row_id    <- seq_len(n)

  # Mask empty geometries so Python doesn't try to extract from (NA, NA)
  empty <- sf::st_is_empty(data)
  if (any(empty)) {
    df_flat$.longitude[empty] <- NA_real_
    df_flat$.latitude[empty]  <- NA_real_
  }

  tmp_in  <- tempfile(fileext = ".csv")
  tmp_out <- tempfile(fileext = ".csv")
  on.exit({ unlink(tmp_in); unlink(tmp_out) }, add = TRUE)

  utils::write.csv(df_flat, tmp_in, row.names = FALSE, na = "")

  # ── Call Python script ────────────────────────────────────────────────────
  args <- c(
    normalizePath(script, mustWork = FALSE),
    normalizePath(tmp_in,  mustWork = TRUE),
    normalizePath(tmp_out, mustWork = FALSE),
    "--lon",  ".longitude",
    "--lat",  ".latitude",
    "--time", ".timestamp"
  )
  if (!is.null(gee_project)) args <- c(args, "--project", gee_project)

  if (verbose) message("Running Python ERA5 extraction ...")
  ret <- system2(python, args = args, wait = TRUE,
                 stdout = if (verbose) "" else FALSE,
                 stderr = if (verbose) "" else FALSE)

  if (ret != 0L)
    stop("Python script exited with code ", ret,
         ".\n  Check output above for error details.")

  if (!file.exists(tmp_out))
    stop("Python script did not produce output file.")

  # ── Read results and attach to original data ──────────────────────────────
  result <- utils::read.csv(tmp_out, stringsAsFactors = FALSE)

  era5_cols <- grep("^era5_", names(result), value = TRUE)
  if (length(era5_cols) == 0L)
    warning("No era5_* columns found in Python output — check script output above.")

  # Match by row order (.row_id is just seq_len so index directly)
  for (col in era5_cols) {
    data[[col]] <- result[[col]]
  }

  if (verbose)
    message("  ERA5 columns added: ",
            paste(era5_cols, collapse = ", "))

  # ── Wind support ──────────────────────────────────────────────────────────
  if (compute_wind_support) {
    if (verbose) message("Computing wind support ...")
    data <- .era5_wind_support(data, pressure_levels,
                                altitude_col, tag_pressure_col)
  }

  if (verbose) message("GEE annotation complete.")
  data
}


# ═══════════════════════════════════════════════════════════════════════════
# correct_altitude_era5() — local-pressure/temperature altitude correction
# ═══════════════════════════════════════════════════════════════════════════

#' Correct tag-barometric altitude using local ERA5 surface pressure/temperature
#'
#' \code{\link{pressure_to_altitude_m}} (used by the default import pipeline)
#' assumes the International Standard Atmosphere: fixed sea-level reference
#' pressure (1013.25 hPa) and a fixed lapse-rate temperature profile. This
#' function instead uses the actual local surface pressure and 2 m
#' temperature from ERA5 reanalysis (nearest grid cell/timestep) in the full
#' hypsometric formula, which corrects for real synoptic pressure variation
#' and temperature — the two biggest sources of error in barometric altitude
#' at a fixed location.
#'
#' This is \strong{not} run automatically by \code{\link{import_nanofox_movebank}}
#' or any other pipeline function — call it explicitly as a post-processing
#' step on \code{location}/\code{daily}/\code{full} output.
#'
#' @section Reference frame caveat:
#' \code{era5_sp} is the modelled pressure at ERA5's own grid-cell orography
#' height, not true sea level and not the animal's ground-truth elevation.
#' \code{altitude_m_era5} is therefore "height above the local ERA5 surface,"
#' not directly the same reference frame as the ISA-based \code{altitude_m}.
#' Treat differences between the two as approximate, not exact.
#'
#' @param data             A \code{move2} or \code{sf} object with timestamps.
#' @param era5_dir         Path to the EnvData directory (parent of
#'   \code{single_levels/}), e.g.
#'   \code{"//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"}.
#' @param tag_pressure_col Column name holding the tag's own barometric
#'   pressure in hPa. \code{NULL} = auto-detect (\code{pressure_hpa_used},
#'   \code{tinyfox_pressure_min_last_24h}, \code{min_3h_pressure},
#'   \code{tag_pressure}, \code{barometric_pressure}).
#' @param max_time_gap_hours Warn when the nearest ERA5 timestep exceeds this
#'   distance from the animal fix.
#' @param verbose Logical; print progress messages?
#'
#' @return \code{data} with three new columns:
#'   \code{altitude_m_era5} (corrected altitude, m), \code{era5_sp_hpa}
#'   (local ERA5 surface pressure used, hPa), \code{era5_t2m_c} (local ERA5
#'   2 m temperature used, \eqn{^\circ}C). \code{NA} where the tag pressure
#'   or matching ERA5 values are unavailable. The existing \code{altitude_m}
#'   column is never modified.
#'
#' @examples
#' \dontrun{
#'   out <- import_nanofox_movebank(study_id = 123456789)
#'   out$location <- correct_altitude_era5(
#'     out$location,
#'     era5_dir = "//10.0.16.7/grpdechmann/Postdoc-EdwardHurme/EnvData"
#'   )
#' }
#'
#' @importFrom move2 mt_time
#' @importFrom sf st_coordinates
#' @export
correct_altitude_era5 <- function(
    data,
    era5_dir,
    tag_pressure_col   = NULL,
    max_time_gap_hours = 3,
    verbose             = TRUE
) {
  require(terra)

  era5_dir <- gsub("\\\\", "/", era5_dir)  # UNC paths need forward slashes for GDAL
  stopifnot(inherits(data, "sf"))

  single_dir <- file.path(era5_dir, "single_levels")
  if (!dir.exists(single_dir)) stop("No single_levels/ folder found under ", era5_dir)

  n <- nrow(data)
  if (n == 0) { warning("Input data has 0 rows."); return(data) }

  tag_pressure_col <- .era5_detect_col(
    data, tag_pressure_col,
    c("pressure_hpa_used", "tinyfox_pressure_min_last_24h",
      "min_3h_pressure", "tag_pressure", "barometric_pressure"),
    "tag pressure", verbose
  )
  if (is.null(tag_pressure_col)) {
    warning("correct_altitude_era5: no tag pressure column found/detected; skipping.")
    return(data)
  }

  timestamps <- if (inherits(data, "move2")) mt_time(data) else data$timestamp
  if (is.null(timestamps)) stop("Cannot find timestamps in data.")
  coords_mat <- sf::st_coordinates(data)

  grib_files <- sort(list.files(single_dir, "\\.grib$", full.names = TRUE))
  if (length(grib_files) == 0) {
    warning("correct_altitude_era5: no .grib files found in ", single_dir, "; skipping.")
    return(data)
  }

  if (verbose) message("Extracting local surface pressure/temperature ...")
  data <- .era5_extract_single(data, grib_files, timestamps,
                                coords_mat, max_time_gap_hours, verbose)

  # Hypsometric formula with local (not ISA-standard) pressure and temperature:
  #   z = (R * T0 / g) * ln(P0 / P)
  # R = specific gas constant for dry air (J / (kg K)); g = standard gravity (m/s^2)
  R_dry <- 287.053
  g_std <- 9.80665

  p0_hpa  <- as.numeric(data$era5_sp) / 100   # Pa -> hPa
  t0_k    <- as.numeric(data$era5_t2m)        # already Kelvin
  p_tag   <- as.numeric(data[[tag_pressure_col]])

  ok  <- is.finite(p0_hpa) & p0_hpa > 0 &
    is.finite(t0_k) & t0_k > 0 &
    is.finite(p_tag) & p_tag > 0

  alt <- rep(NA_real_, n)
  alt[ok] <- (R_dry * t0_k[ok] / g_std) * log(p0_hpa[ok] / p_tag[ok])

  data$altitude_m_era5 <- alt
  data$era5_sp_hpa     <- p0_hpa
  data$era5_t2m_c      <- t0_k - 273.15

  if (verbose) {
    message("  altitude_m_era5: ", sum(ok), " / ", n, " rows corrected using local ERA5 pressure/temperature.")
  }

  data
}
