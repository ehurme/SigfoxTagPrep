# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5.R
# ─────────────────────────────────────────────────────────────────────────────
# Annotate move2 objects with ERA5 reanalysis weather data extracted from the
# folder structure produced by inst/python/download_era5.py.
#
# Complements add_env_to_move2() (which works with yearly GRIB stacks) by
# adding pressure-level wind extraction, altitude-matched wind support, and
# NetCDF-based extraction from the standardised monthly file layout.
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


# ── Main annotation function ─────────────────────────────────────────────────

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


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS (prefixed .era5_ to avoid collisions)
# ═══════════════════════════════════════════════════════════════════════════════

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


.era5_nearest_layer <- function(raster_times, obs_times, max_gap_h, verbose) {
  obs_num  <- as.numeric(obs_times)
  rast_num <- as.numeric(raster_times)
  idx <- vapply(obs_num, function(t) which.min(abs(rast_num - t)), integer(1))

  gaps_h <- abs(rast_num[idx] - obs_num) / 3600
  bad <- which(gaps_h > max_gap_h)
  if (length(bad) > 0 && verbose) {
    message("    [warn] ", length(bad), " fixes >", max_gap_h,
            "h from nearest ERA5 step (max gap: ",
            round(max(gaps_h[bad]), 1), "h)")
  }
  idx
}


# ── Single-level extraction ──────────────────────────────────────────────────

.era5_extract_single <- function(data, grib_files, timestamps,
                                  coords_mat, max_gap, verbose) {
  # GRIB layer positions within each time-step block.
  # Order confirmed from terra output — positions 3, 4, 11 are "undefined" in
  # GRIB names so must be identified by position, not name.
  var_positions <- c(
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
      # Drop sf geometry before as.matrix(): the sticky geometry column would
      # otherwise coerce the entire matrix to character.
      .data_df <- sf::st_drop_geometry(data)
      ws_mat  <- as.matrix(.data_df[, ws_cols[avail],  drop = FALSE])
      cs_mat  <- as.matrix(.data_df[, cs_cols[avail],  drop = FALSE])
      spd_mat <- as.matrix(.data_df[, spd_cols[avail], drop = FALSE])

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
