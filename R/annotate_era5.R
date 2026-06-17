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
  r <- tryCatch(rast(grib_files), error = function(e) NULL)
  if (is.null(r)) return(data)

  rt <- time(r)
  if (is.null(rt) || length(rt) == 0) return(data)

  # GRIB layers are interleaved: [var1_t1, var2_t1, ..., varN_t1, var1_t2, ...]
  n_vars       <- sum(rt == rt[1])
  unique_times <- rt[seq(1L, length(rt), by = n_vars)]

  obs_num  <- as.numeric(timestamps)
  time_num <- as.numeric(unique_times)
  nearest_t <- vapply(obs_num, function(t) which.min(abs(time_num - t)), integer(1))

  gaps_h <- abs(time_num[nearest_t] - obs_num) / 3600
  bad <- which(gaps_h > max_gap)
  if (length(bad) > 0 && verbose) {
    message("    [warn] ", length(bad), " fixes >", max_gap,
            "h from nearest ERA5 step (max gap: ",
            round(max(gaps_h[bad]), 1), "h)")
  }

  coords_xy <- coords_mat[, 1:2, drop = FALSE]

  for (col_nm in names(var_positions)) {
    pos <- var_positions[[col_nm]]
    if (pos > n_vars) next

    layer_idx <- (nearest_t - 1L) * n_vars + pos
    out_vals  <- rep(NA_real_, length(layer_idx))
    for (li in unique(layer_idx)) {
      idx <- which(layer_idx == li)
      ex  <- tryCatch(
        terra::extract(r[[li]], coords_xy[idx, , drop = FALSE]),
        error = function(e) {
          if (verbose) message("    [warn] layer ", li, " unreadable — skipping (", conditionMessage(e), ")")
          NULL
        }
      )
      if (!is.null(ex) && ncol(ex) >= 1) out_vals[idx] <- ex[[1]]
    }
    data[[col_nm]] <- out_vals
    if (verbose) message("    + ", col_nm)
  }

  rm(r)
  gc(verbose = FALSE)

  data
}


# ── Pressure-level extraction ────────────────────────────────────────────────

.era5_extract_pressure <- function(data, pressure_dir, timestamps,
                                    coords_mat, pressure_levels,
                                    max_gap, verbose) {

  coords_xy <- coords_mat[, 1:2, drop = FALSE]
  reported_gap <- FALSE

  if (verbose) {
    cat("  Pressure levels: ")
    pb <- utils::txtProgressBar(min = 0, max = length(pressure_levels), style = 3)
    pb_i <- 0L
  }

  for (level in pressure_levels) {
    pattern <- paste0("era5_wind_", level, "hPa_.*\\.grib$")
    level_files <- gsub("\\\\", "/", sort(list.files(pressure_dir, pattern, full.names = TRUE)))

    if (length(level_files) == 0) {
      if (verbose) message("    [skip] No files for ", level, " hPa")
      next
    }

    r <- tryCatch(rast(level_files), error = function(e) NULL)
    if (is.null(r)) {
      if (verbose) message("    [skip] Could not load ", level, " hPa")
      next
    }

    rt <- time(r)
    if (is.null(rt) || length(rt) == 0) next

    # GRIB interleaved structure: n_vars layers per time step
    n_vars       <- sum(rt == rt[1])
    unique_times <- rt[seq(1L, length(rt), by = n_vars)]
    layer_names  <- names(r)[seq_len(n_vars)]

    obs_num  <- as.numeric(timestamps)
    time_num <- as.numeric(unique_times)
    nearest_t <- vapply(obs_num, function(t) which.min(abs(time_num - t)), integer(1))

    if (!reported_gap) {
      gaps_h <- abs(time_num[nearest_t] - obs_num) / 3600
      bad <- which(gaps_h > max_gap)
      if (length(bad) > 0 && verbose) {
        message("    [warn] ", length(bad), " fixes >", max_gap,
                "h from nearest ERA5 step (max gap: ",
                round(max(gaps_h[bad]), 1), "h)")
      }
      reported_gap <- TRUE
    }

    # Find u and v layer positions by name matching
    # ERA5 GRIB short names are often bare "u" / "v"; NetCDF uses "u_component_of_wind"
    u_pos <- grep("(?i)(^u$|U-velocity|U_velocity|\\bu.{0,10}wind|u.{0,5}component)", layer_names, perl = TRUE)[1]
    v_pos <- grep("(?i)(^v$|V-velocity|V_velocity|\\bv.{0,10}wind|v.{0,5}component)", layer_names, perl = TRUE)[1]

    if (verbose && (is.na(u_pos) || is.na(v_pos)))
      message("    [debug] layer names: ", paste(layer_names, collapse = ", "))

    for (var_info in list(list(pos = u_pos, nm = "u"),
                          list(pos = v_pos, nm = "v"))) {
      pos <- var_info$pos
      var <- var_info$nm
      if (is.na(pos)) {
        if (verbose) message("    [skip] ", var, " at ", level, " hPa — layer not found")
        next
      }

      layer_idx <- (nearest_t - 1L) * n_vars + pos
      out_vals  <- rep(NA_real_, length(layer_idx))
      for (li in unique(layer_idx)) {
        idx <- which(layer_idx == li)
        ex  <- tryCatch(
          terra::extract(r[[li]], coords_xy[idx, , drop = FALSE]),
          error = function(e) {
            if (verbose) message("    [warn] layer ", li, " unreadable — skipping (", conditionMessage(e), ")")
            NULL
          }
        )
        if (!is.null(ex) && ncol(ex) >= 1) out_vals[idx] <- ex[[1]]
      }
      col_nm <- paste0("era5_", var, level)
      data[[col_nm]] <- out_vals
      if (verbose) message("    + ", col_nm)
    }

    rm(r)
    gc(verbose = FALSE)

    if (verbose) { pb_i <- pb_i + 1L; utils::setTxtProgressBar(pb, pb_i) }
  }

  if (verbose) { close(pb); cat("\n") }

  data
}


# ── Wind support & altitude matching ─────────────────────────────────────────

.era5_wind_support <- function(data, pressure_levels,
                                altitude_col, tag_pressure_col) {

  n <- nrow(data)

  # ── Heading (degrees, lagged to preceding segment) ─────────────────────
  if (inherits(data, "move2")) {
    heading_deg <- as.numeric(mt_azimuth(data))
    tid_col <- mt_track_id_column(data)
    data$.heading_deg <- heading_deg
    data <- data %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(tid_col))) %>%
      dplyr::mutate(.heading_deg = dplyr::lag(.heading_deg)) %>%
      dplyr::ungroup()
    heading <- data$.heading_deg

    # Ground speed for airspeed calc (m/s → m/h not needed; wind_support is m/s)
    ground_speed <- tryCatch(
      as.numeric(mt_speed(data)),
      error = function(e) rep(NA_real_, n)
    )
    data$.gs <- ground_speed
    data <- data %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(tid_col))) %>%
      dplyr::mutate(.gs = dplyr::lag(.gs)) %>%
      dplyr::ungroup()
    ground_speed <- data$.gs
    data$.gs <- NULL
  } else {
    heading <- rep(NA_real_, n)
    ground_speed <- rep(NA_real_, n)
  }

  # ── Per-level wind support (reuses package functions) ──────────────────
  for (level in pressure_levels) {
    u_col <- paste0("era5_u", level)
    v_col <- paste0("era5_v", level)
    if (!all(c(u_col, v_col) %in% names(data))) next

    u <- data[[u_col]];  v <- data[[v_col]]

    data[[paste0("wind_speed_",   level)]] <- sqrt(u^2 + v^2)
    data[[paste0("wind_support_", level)]] <- wind_support(u, v, heading)
    data[[paste0("crosswind_",    level)]] <- cross_wind(u, v, heading)
    data[[paste0("airspeed_",     level)]] <- airspeed(
      as.numeric(ground_speed),
      wind_support(u, v, heading),
      cross_wind(u, v, heading)
    )
  }

  # Surface winds (10 m / 100 m)
  for (h in c("10", "100")) {
    u_col <- paste0("era5_u", h)
    v_col <- paste0("era5_v", h)
    if (!all(c(u_col, v_col) %in% names(data))) next

    u <- data[[u_col]];  v <- data[[v_col]]
    sfx <- paste0(h, "m")
    data[[paste0("wind_speed_",   sfx)]] <- sqrt(u^2 + v^2)
    data[[paste0("wind_support_", sfx)]] <- wind_support(u, v, heading)
    data[[paste0("crosswind_",    sfx)]] <- cross_wind(u, v, heading)
  }

  # ── Flight-level matching ──────────────────────────────────────────────
  flight_p <- .era5_flight_pressure(data, altitude_col, tag_pressure_col)

  if (!is.null(flight_p)) {
    data$flight_pressure_hPa <- flight_p

    # Nearest standard pressure level
    level_mat <- outer(flight_p, pressure_levels,
                       function(fp, pl) abs(fp - pl))
    nearest_idx   <- apply(level_mat, 1, which.min)
    nearest_level <- pressure_levels[nearest_idx]
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

      data$wind_support_flight <- ws_mat[idx]
      data$crosswind_flight    <- cs_mat[idx]
      data$wind_speed_flight   <- spd_mat[idx]
      data$airspeed_flight     <- airspeed(
        as.numeric(ground_speed),
        data$wind_support_flight,
        data$crosswind_flight
      )

      # Best available wind level
      best_col <- apply(ws_mat, 1, function(row) {
        if (all(is.na(row))) NA_integer_ else which.max(row)
      })
      data$best_wind_level   <- pressure_levels[avail][best_col]
      data$best_wind_support <- ws_mat[cbind(seq_len(n), best_col)]
      data$at_best_wind      <- nearest_level == data$best_wind_level
    }
  }

  data$.heading_deg <- NULL
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
