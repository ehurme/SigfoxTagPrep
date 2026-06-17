# pressure_wind_profile.R
# ─────────────────────────────────────────────────────────────────────────────
# Generic wind-column / altitude profile analysis for Sigfox wildlife tags.
# Works for any species (bats, swifts, raptors) with daily or nightly windows.
#
# Workflow:
#   cfg     <- setup_pressure_wind_config()
#   periods <- find_complete_periods(data, cfg)
#   result  <- pressure_wind_profile(data, cfg,
#                individual_id = "TAG001",
#                t_start = periods$t_start[1],
#                t_end   = periods$t_end[1])
#
# Dependencies (sourced automatically if found):
#   pressure_to_altitude_m()     — R/pressure_to_altitude_m.R
#   extract_elevation_segments() — R/extract_elevation_segments.R
# ─────────────────────────────────────────────────────────────────────────────


# ── Internal helpers ──────────────────────────────────────────────────────────
`%||%`     <- function(a, b) if (!is.null(a)) a else b
`%||_plt%` <- function(a, b) if (!is.null(a)) a else b


# ── setup_pressure_wind_config() ──────────────────────────────────────────────
#'
#' Interactive configuration wizard for pressure_wind_profile()
#'
#' Prompts the user for tag firmware, pressure measurement type, column names,
#' time window mode, location interpolation method, and optional VeDBA /
#' temperature settings.  Returns a named list passed to
#' \code{find_complete_periods()} and \code{pressure_wind_profile()}.
#'
#' @return Named list with all configuration fields (invisibly).
#' @export
setup_pressure_wind_config <- function() {

  .menu <- function(prompt, choices, default_idx = 1L) {
    cat("\n", prompt, "\n", sep = "")
    for (i in seq_along(choices))
      cat(sprintf("  [%d] %s\n", i, choices[i]))
    cat(sprintf("Enter number [default %d]: ", default_idx))
    ans <- trimws(readline())
    idx <- suppressWarnings(as.integer(ans))
    if (is.na(idx) || idx < 1L || idx > length(choices)) {
      message("  → using default: ", choices[default_idx])
      return(choices[default_idx])
    }
    choices[idx]
  }

  .txt <- function(prompt, default = "") {
    cat(prompt)
    if (nchar(default) > 0) cat("[", default, "] ")
    ans <- trimws(readline())
    if (ans == "") default else ans
  }

  .num <- function(prompt, default) {
    cat(prompt, "[", default, "]: ")
    ans <- trimws(readline())
    v   <- suppressWarnings(as.numeric(ans))
    if (is.na(v)) { message("  → using default: ", default); return(default) }
    v
  }

  .yn <- function(prompt, default_yes = FALSE) {
    cat(prompt, if (default_yes) "[Y/n]: " else "[y/N]: ")
    ans <- tolower(trimws(readline()))
    if (ans == "") return(default_yes)
    ans %in% c("y", "yes")
  }

  cfg <- list()
  cat("\n══════════════════════════════════════════════════\n")
  cat("   Pressure–Wind Profile  —  Configuration Wizard  \n")
  cat("══════════════════════════════════════════════════\n")

  # ── 1. Firmware ─────────────────────────────────────────────────────────
  cat("\n── 1. Tag firmware ─────────────────────────────────────────────────\n")
  cfg$firmware <- .txt(
    "Firmware string (e.g. 'NanoFox_finescale', 'TinyFox_v2', 'uWasp'): "
  )

  # ── 2. Pressure measurement type ─────────────────────────────────────────
  cat("\n── 2. Pressure measurement type ────────────────────────────────────\n")
  cat("  Instantaneous: one reading per fix; finescale tags store up to 5\n")
  cat("  retrospective readings ~36 min apart spanning a ~3 h window.\n")
  cat("  Minimum: lowest pressure (= highest altitude) in the window.\n")
  press_choice <- .menu(
    "Select pressure type:",
    choices = c(
      "instantaneous  (single reading / retrospective every ~36 min)",
      "minimum over 1-hour window",
      "minimum over 3-hour window",
      "minimum over 6-hour window"
    )
  )
  cfg$pressure_type <- switch(press_choice,
    "instantaneous  (single reading / retrospective every ~36 min)" = "instantaneous",
    "minimum over 1-hour window" = "min_1h",
    "minimum over 3-hour window" = "min_3h",
    "minimum over 6-hour window" = "min_6h"
  )

  # ── 3. Column names ───────────────────────────────────────────────────────
  cat("\n── 3. Column names ─────────────────────────────────────────────────\n")
  cfg$timestamp_col  <- .txt("Timestamp column:                          ", "timestamp")
  cfg$individual_col <- .txt("Individual/animal ID column:               ", "individual_local_identifier")
  cfg$pressure_col   <- .txt("Pressure column (hPa, press Enter to skip):", "barometric_pressure")
  if (cfg$pressure_col == "") cfg$pressure_col <- NULL

  cat("Heading/bearing column (press Enter to skip):               ")
  ans <- trimws(readline())
  cfg$heading_col <- if (nchar(ans) > 0) ans else NULL

  # ── 4. Time window ────────────────────────────────────────────────────────
  cat("\n── 4. Analysis time window ─────────────────────────────────────────\n")
  win_choice <- .menu(
    "Window type:",
    choices = c(
      "nightly  — last evening fix to first morning fix (displacement-based)",
      "daily    — full calendar day (all fixes within one UTC date)"
    )
  )
  cfg$window_type <- if (grepl("^nightly", win_choice)) "nightly" else "daily"

  # ── 5. Location interpolation ─────────────────────────────────────────────
  cat("\n── 5. Location interpolation ───────────────────────────────────────\n")
  interp_choice <- .menu(
    "Method for filling gaps between fixes:",
    choices = c(
      "none    — use raw fixes only",
      "linear  — linear interpolation between consecutive fixes",
      "crawl   — continuous-time correlated random walk (momentuHMM)"
    )
  )
  cfg$interpolation <- switch(interp_choice,
    "none    — use raw fixes only"                                  = "none",
    "linear  — linear interpolation between consecutive fixes"       = "linear",
    "crawl   — continuous-time correlated random walk (momentuHMM)" = "crawl"
  )
  if (cfg$interpolation != "none") {
    cfg$interp_timestep_min <- .num("  Interpolation time step (minutes)", 10)
  } else {
    cfg$interp_timestep_min <- NA_real_
  }

  # ── 6. VeDBA ─────────────────────────────────────────────────────────────
  cat("\n── 6. VeDBA (optional) ─────────────────────────────────────────────\n")
  if (.yn("Include VeDBA in plots?")) {
    cfg$vedba_col <- .txt("  VeDBA column name: ", "vedba_sum")
    agg_choice <- .menu(
      "  VeDBA values are:",
      choices = c(
        "already summed / aggregated over a measurement window",
        "raw instantaneous values (will be summed per window in plot)"
      )
    )
    cfg$vedba_aggregation <- if (grepl("already", agg_choice)) "precomputed" else "sum"
    if (cfg$vedba_aggregation == "sum")
      cfg$vedba_window_min <- .num("  Summation window (minutes)", 30)
  } else {
    cfg$vedba_col         <- NULL
    cfg$vedba_aggregation <- NULL
  }

  # ── 7. Temperature ────────────────────────────────────────────────────────
  cat("\n── 7. Temperature (optional) ───────────────────────────────────────\n")
  if (.yn("Include temperature in plots?")) {
    cfg$temp_col <- .txt("  Temperature column name: ", "external_temperature")
    temp_choice <- .menu(
      "  Temperature column represents:",
      choices = c(
        "mean over measurement window",
        "minimum over measurement window",
        "maximum over measurement window",
        "instantaneous single reading"
      )
    )
    cfg$temp_aggregation <- switch(temp_choice,
      "mean over measurement window"    = "mean",
      "minimum over measurement window" = "min",
      "maximum over measurement window" = "max",
      "instantaneous single reading"    = "instant"
    )
  } else {
    cfg$temp_col         <- NULL
    cfg$temp_aggregation <- NULL
  }

  # ── 8. ERA5 pressure levels ───────────────────────────────────────────────
  cat("\n── 8. ERA5 pressure levels ─────────────────────────────────────────\n")
  cat("Comma-separated hPa values\n[500,600,700,800,850,900,925,950,1000]: ")
  ans <- trimws(readline())
  if (ans == "") {
    cfg$pressure_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)
  } else {
    cfg$pressure_levels <- sort(na.omit(
      as.numeric(strsplit(gsub(" ", "", ans), ",")[[1]])
    ))
  }

  # ── 9. Displacement threshold ─────────────────────────────────────────────
  if (cfg$window_type == "nightly") {
    cat("\n── 9. Migration displacement threshold ────────────────────────────\n")
    cfg$min_displacement_km <- .num("Minimum displacement (km) to qualify", 50)
  } else {
    cfg$min_displacement_km <- 0
  }

  # ── 10. Output path ───────────────────────────────────────────────────────
  cat("\n── 10. Output ──────────────────────────────────────────────────────\n")
  cfg$out_path <- .txt("Directory for PNG + RDS output: ", "output/wind_profiles")

  # ── Summary ───────────────────────────────────────────────────────────────
  cat("\n══ Configuration summary ═══════════════════════════════════════════\n")
  for (nm in names(cfg))
    cat(sprintf("  %-28s %s\n", nm, paste(cfg[[nm]], collapse = ", ")))

  if (.yn("\nSave config as .rds for reuse?", default_yes = TRUE)) {
    prefix <- .txt("Config filename prefix: ", "wind_profile_config")
    fname  <- paste0(prefix, ".rds")
    saveRDS(cfg, fname)
    message("Config saved: ", fname)
  }

  invisible(cfg)
}


# ── find_complete_periods() ───────────────────────────────────────────────────
#'
#' Identify analysis periods suitable for wind-profile plotting
#'
#' Scans an annotated move2/sf object for time windows that meet ERA5 coverage
#' and, in nightly mode, displacement criteria.  Works for both nocturnal
#' migrants (bats) and continuously-flying species (swifts).
#'
#' @param data        sf/move2 object with ERA5 \code{wind_support_*} columns.
#' @param cfg         Config list from \code{setup_pressure_wind_config()}.
#' @param min_coverage Fraction of rows in the window that must have non-NA
#'   ERA5 wind at all \code{cfg$pressure_levels}. Default 0.3.
#' @param verbose     Logical; print progress. Default \code{TRUE}.
#'
#' @return Data frame: \code{individual_id}, \code{t_start}, \code{t_end},
#'   \code{displacement_km}, \code{n_fixes}, \code{era5_coverage}.
#' @export
find_complete_periods <- function(
    data,
    cfg,
    min_coverage = 0.3,
    verbose      = TRUE
) {
  suppressPackageStartupMessages({ library(dplyr); library(sf) })

  stopifnot(inherits(data, "sf"))
  ts_col <- cfg$timestamp_col
  id_col <- cfg$individual_col
  stopifnot(all(c(ts_col, id_col) %in% names(data)))

  data   <- data[!sf::st_is_empty(data), ]
  coords <- sf::st_coordinates(data)
  df <- data %>%
    sf::st_drop_geometry() %>%
    mutate(
      .ts   = .data[[ts_col]],
      .id   = as.character(.data[[id_col]]),
      .lon  = coords[, "X"],
      .lat  = coords[, "Y"],
      .date = as.Date(.data[[ts_col]])
    ) %>%
    arrange(.id, .ts)

  ws_cols  <- paste0("wind_support_", cfg$pressure_levels)
  ws_avail <- ws_cols[ws_cols %in% names(df)]
  if (length(ws_avail) == 0)
    stop("No wind_support_* columns found. Run annotate_era5() first.")

  .era5_cov <- function(win_df) {
    if (nrow(win_df) == 0 || length(ws_avail) == 0) return(0)
    mean(rowSums(is.na(win_df[, ws_avail, drop = FALSE])) == 0)
  }

  .dist_km <- function(lon1, lat1, lon2, lat2) {
    tryCatch(
      as.numeric(sf::st_distance(
        sf::st_sfc(sf::st_point(c(lon1, lat1)), crs = 4326),
        sf::st_sfc(sf::st_point(c(lon2, lat2)), crs = 4326)
      )) / 1000,
      error = function(e) NA_real_
    )
  }

  results <- list()
  indivs  <- unique(df$.id)

  if (verbose) {
    cat("Scanning", length(indivs), "individual(s) [mode:", cfg$window_type, "]\n")
    pb <- utils::txtProgressBar(0, length(indivs), style = 3)
  }

  for (i_ind in seq_along(indivs)) {
    if (verbose) utils::setTxtProgressBar(pb, i_ind)
    ind  <- indivs[i_ind]
    idf  <- df %>% filter(.id == ind) %>% arrange(.ts)
    dates <- sort(unique(idf$.date))

    if (cfg$window_type == "nightly") {
      for (d_idx in seq_len(length(dates) - 1L)) {
        d0 <- dates[d_idx]; d1 <- dates[d_idx + 1L]
        if (as.integer(d1 - d0) != 1L) next

        dep <- idf %>% filter(.date == d0) %>% slice_max(.ts, n = 1)
        arr <- idf %>% filter(.date == d1) %>% slice_min(.ts, n = 1)
        if (nrow(dep) == 0 || nrow(arr) == 0) next

        dk <- .dist_km(dep$.lon, dep$.lat, arr$.lon, arr$.lat)
        if (is.na(dk) || dk < cfg$min_displacement_km) next

        win <- idf %>% filter(.ts >= dep$.ts[1], .ts <= arr$.ts[1])
        if (.era5_cov(win) < min_coverage) next

        results[[length(results) + 1L]] <- data.frame(
          individual_id   = ind,
          t_start         = dep$.ts[1],
          t_end           = arr$.ts[1],
          displacement_km = round(dk, 1),
          n_fixes         = nrow(win),
          era5_coverage   = round(.era5_cov(win), 3),
          stringsAsFactors = FALSE
        )
      }

    } else {
      # Daily windows
      for (d in dates) {
        win <- idf %>% filter(.date == d)
        if (nrow(win) < 2) next
        if (.era5_cov(win) < min_coverage) next

        dep <- win %>% slice_min(.ts, n = 1)
        arr <- win %>% slice_max(.ts, n = 1)
        dk  <- .dist_km(dep$.lon, dep$.lat, arr$.lon, arr$.lat)

        results[[length(results) + 1L]] <- data.frame(
          individual_id   = ind,
          t_start         = dep$.ts[1],
          t_end           = arr$.ts[1],
          displacement_km = round(if (!is.na(dk)) dk else 0, 1),
          n_fixes         = nrow(win),
          era5_coverage   = round(.era5_cov(win), 3),
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (verbose) { close(pb); cat("\n") }
  if (length(results) == 0) {
    if (verbose) message("No qualifying periods found.")
    return(data.frame())
  }

  out <- do.call(rbind, results)
  out$t_start  <- as.POSIXct(out$t_start, tz = "UTC", origin = "1970-01-01")
  out$t_end    <- as.POSIXct(out$t_end,   tz = "UTC", origin = "1970-01-01")
  rownames(out) <- NULL
  if (verbose)
    message("find_complete_periods: ", nrow(out), " qualifying periods across ",
            length(unique(out$individual_id)), " individuals.")
  out
}


# ── .interpolate_track() ─────────────────────────────────────────────────────
# Internal: interpolate a track data frame to regular time steps.
# Input df must have columns: timestamp, .lon, .lat
# Returns a data frame with those columns plus .interpolated (logical).
.interpolate_track <- function(df, cfg) {
  if (cfg$interpolation == "none" || nrow(df) < 2) {
    df$.interpolated <- FALSE
    return(df)
  }

  t_seq <- seq(min(df$timestamp), max(df$timestamp),
               by = cfg$interp_timestep_min * 60)
  t_num <- as.numeric(df$timestamp)

  if (cfg$interpolation == "linear") {
    return(data.frame(
      timestamp     = t_seq,
      .lon          = approx(t_num, df$.lon, xout = as.numeric(t_seq), rule = 2)$y,
      .lat          = approx(t_num, df$.lat, xout = as.numeric(t_seq), rule = 2)$y,
      .interpolated = TRUE
    ))
  }

  if (cfg$interpolation == "crawl") {
    if (!requireNamespace("momentuHMM", quietly = TRUE))
      stop("Package 'momentuHMM' required for CRAWL interpolation.")

    # momentuHMM::crawlWrap works in projected space; use Web Mercator
    obs_sf <- sf::st_as_sf(df, coords = c(".lon", ".lat"), crs = 4326) %>%
      sf::st_transform(3857)
    obs_xy <- sf::st_coordinates(obs_sf)

    crawl_in <- data.frame(
      ID   = "animal",
      Time = as.numeric(df$timestamp),
      x    = obs_xy[, "X"],
      y    = obs_xy[, "Y"]
    )

    crw_fit <- tryCatch(
      momentuHMM::crawlWrap(
        obsData   = crawl_in,
        timeStep  = cfg$interp_timestep_min * 60,
        theta     = c(6, -0.1),
        fixPar    = c(NA, NA),
        coord     = c("x", "y"),
        Time.name = "Time",
        ID.name   = "ID"
      ),
      error = function(e) {
        warning("CRAWL fit failed (", conditionMessage(e),
                "); falling back to linear interpolation.")
        NULL
      }
    )

    if (!is.null(crw_fit)) {
      pred <- crw_fit$crwPredict
      pred_sf <- sf::st_as_sf(
        data.frame(x = pred$mu.x, y = pred$mu.y),
        coords = c("x", "y"), crs = 3857
      ) %>% sf::st_transform(4326)
      pred_ll <- sf::st_coordinates(pred_sf)
      return(data.frame(
        timestamp     = as.POSIXct(pred$Time, tz = "UTC", origin = "1970-01-01"),
        .lon          = pred_ll[, "X"],
        .lat          = pred_ll[, "Y"],
        .interpolated = TRUE
      ))
    }

    # Fall back to linear
    return(data.frame(
      timestamp     = t_seq,
      .lon          = approx(t_num, df$.lon, xout = as.numeric(t_seq), rule = 2)$y,
      .lat          = approx(t_num, df$.lat, xout = as.numeric(t_seq), rule = 2)$y,
      .interpolated = TRUE
    ))
  }
}


# ── .assign_nearest_wind() ────────────────────────────────────────────────────
# Assign ERA5 wind columns from the temporally nearest original fix.
# Used to propagate annotated wind values to interpolated locations.
.assign_nearest_wind <- function(interp_df, raw_df, pressure_levels) {
  take_cols <- c(
    paste0("wind_support_", pressure_levels),
    paste0("wind_speed_",   pressure_levels),
    "best_wind_level", "best_wind_support",
    "wind_support_flight", "wind_support_10m",
    "matched_pressure_level", "airspeed_flight"
  )
  take_cols <- take_cols[take_cols %in% names(raw_df)]
  if (length(take_cols) == 0) return(interp_df)

  t_raw    <- as.numeric(raw_df$timestamp)
  t_interp <- as.numeric(interp_df$timestamp)
  nn_idx   <- sapply(t_interp, function(t) which.min(abs(t_raw - t)))

  for (col in take_cols)
    interp_df[[col]] <- raw_df[[col]][nn_idx]

  interp_df
}


# ── pressure_wind_profile() ───────────────────────────────────────────────────
#'
#' Multi-panel wind-column and flight-altitude diagnostic plot
#'
#' Produces a figure with up to eight panels:
#' \enumerate{
#'   \item \strong{Map} — full track (grey), window fixes coloured by time,
#'     departure (green circle), arrival (orange triangle), terrain elevation.
#'   \item \strong{Elevation profile} — ground elevation along the flight path.
#'   \item \strong{Wind heatmap} — time × pressure level, fill = wind support.
#'     Animal's tag pressure shown as a white path; best-wind level in yellow.
#'   \item \strong{Wind per level} — one coloured line per ERA5 pressure level.
#'   \item \strong{Tag pressure / altitude} — barometric pressure over time,
#'     drawn as a path (instantaneous) or step function (windowed minimum).
#'   \item \strong{Wind comparison} — flight-level vs best-available vs 10 m.
#'   \item \strong{VeDBA} — (optional) activity index over time.
#'   \item \strong{Temperature} — (optional) external temperature over time.
#' }
#'
#' All data used to build the figure are saved as an .rds file alongside the PNG.
#'
#' @param data          sf/move2 object annotated with ERA5 wind columns
#'   (run \code{annotate_era5()} first).
#' @param cfg           Config list from \code{setup_pressure_wind_config()}.
#' @param individual_id Character; individual to plot.
#' @param t_start       POSIXct start of the analysis window.
#' @param t_end         POSIXct end of the analysis window.
#' @param context_min   Minutes of context beyond the window shown in the map
#'   track. Default 15.
#' @param elev_z        Zoom level for \code{elevatr::get_elev_raster}. Default 6.
#' @param buffer_deg    Map padding in degrees. Default 1.5.
#' @param theme_dark    Logical; dark-background theme. Default \code{FALSE}.
#' @param verbose       Logical; print progress. Default \code{TRUE}.
#'
#' @return Invisibly: named list containing \code{config}, \code{raw_data},
#'   \code{interp_track}, \code{plot_data}, \code{wind_long},
#'   \code{elev_profile}, and \code{plot} (the assembled ggplot).  The same
#'   list is written to \code{<cfg$out_path>/<id>_<date>_wind_profile.rds}.
#' @export
pressure_wind_profile <- function(
    data,
    cfg,
    individual_id,
    t_start,
    t_end,
    context_min = 15,
    elev_z      = 6,
    buffer_deg  = 1.5,
    theme_dark  = FALSE,
    verbose     = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr); library(tidyr); library(sf)
    library(ggplot2); library(ggpubr)
    library(terra); library(tidyterra)
    library(rnaturalearth); library(elevatr)
  })

  for (.src in c("R/pressure_to_altitude_m.R",
                 "../SigfoxTagPrep/R/pressure_to_altitude_m.R"))
    if (!exists("pressure_to_altitude_m", mode = "function") && file.exists(.src))
      source(.src)

  for (.src in c("R/extract_elevation_segments.R",
                 "../SigfoxTagPrep/R/extract_elevation_segments.R"))
    if (!exists("extract_elevation_segments", mode = "function") && file.exists(.src))
      source(.src)

  .has <- function(df, col)
    !is.null(col) && nchar(col) > 0 && col %in% names(df) && any(!is.na(df[[col]]))

  .pt <- function(base_size = 9) {
    bg <- if (theme_dark) "black" else "white"
    fg <- if (theme_dark) "white" else "black"
    theme_minimal(base_size = base_size) +
      theme(
        panel.background  = element_rect(fill = bg, color = NA),
        plot.background   = element_rect(fill = bg, color = NA),
        text              = element_text(color = fg),
        axis.text         = element_text(color = fg),
        axis.title        = element_text(color = fg),
        legend.background = element_rect(fill = bg, color = NA),
        legend.text       = element_text(color = fg),
        legend.title      = element_text(color = fg),
        panel.grid        = element_line(color = if (theme_dark) "grey25" else "grey90")
      )
  }

  ts_col <- cfg$timestamp_col
  id_col <- cfg$individual_col
  pr_col <- cfg$pressure_col

  # ── Filter to individual ─────────────────────────────────────────────────
  stopifnot(inherits(data, "sf"))
  indiv_chr <- as.character(individual_id)
  data_i    <- data[as.character(data[[id_col]]) == indiv_chr, ]
  data_i    <- data_i[!sf::st_is_empty(data_i), ]
  if (nrow(data_i) < 2)
    stop("Fewer than 2 valid rows for individual '", indiv_chr, "'.")

  coords_i <- sf::st_coordinates(data_i)
  df_i <- data_i %>%
    sf::st_drop_geometry() %>%
    mutate(
      timestamp = .data[[ts_col]],
      .lon = coords_i[, "X"],
      .lat = coords_i[, "Y"]
    ) %>%
    arrange(timestamp)

  t_start <- as.POSIXct(t_start, tz = "UTC")
  t_end   <- as.POSIXct(t_end,   tz = "UTC")

  # ── Subset window (with context for map) ────────────────────────────────
  t_ctx_s <- t_start - context_min * 60
  t_ctx_e <- t_end   + context_min * 60
  core_df <- df_i %>% filter(timestamp >= t_start, timestamp <= t_end)

  if (nrow(core_df) < 2)
    stop("Fewer than 2 rows in window for '", indiv_chr, "'.")

  if (verbose)
    message("Window: ", format(t_start, "%Y-%m-%d %H:%M"), " – ",
            format(t_end, "%H:%M UTC"),
            " | ", nrow(core_df), " fixes | mode=", cfg$window_type,
            " | firmware=", cfg$firmware,
            " | pressure_type=", cfg$pressure_type)

  # ── Location interpolation ───────────────────────────────────────────────
  interp_df <- .interpolate_track(core_df, cfg)
  if (cfg$interpolation != "none" && nrow(interp_df) > 0) {
    interp_df <- .assign_nearest_wind(interp_df, core_df, cfg$pressure_levels)
    plot_df   <- interp_df
    if (verbose)
      message("Interpolated: ", nrow(interp_df), " rows @ ",
              cfg$interp_timestep_min, " min (", cfg$interpolation, ")")
  } else {
    plot_df <- core_df
    plot_df$.interpolated <- FALSE
  }

  # ── Pressure labels (hPa + approx altitude) ──────────────────────────────
  press_breaks <- sort(cfg$pressure_levels)
  alt_fn <- if (exists("pressure_to_altitude_m", mode = "function"))
    pressure_to_altitude_m
  else
    function(p) 44330 * (1 - (p / 1013.25)^0.1903)

  press_labels <- paste0(press_breaks, "\n(",
                         round(alt_fn(press_breaks) / 100) * 100, " m)")

  press_y_label <- switch(cfg$pressure_type,
    instantaneous = "Pressure (hPa) — instantaneous",
    min_1h        = "Min pressure (hPa) — 1 h window",
    min_3h        = "Min pressure (hPa) — 3 h window",
    min_6h        = "Min pressure (hPa) — 6 h window",
    "Pressure (hPa)"
  )

  # ── Wind reshape (long) ───────────────────────────────────────────────────
  ws_cols   <- paste0("wind_support_", cfg$pressure_levels)
  ws_avail  <- ws_cols[ws_cols %in% names(plot_df)]
  spd_cols  <- paste0("wind_speed_",   cfg$pressure_levels)
  spd_avail <- spd_cols[spd_cols %in% names(plot_df)]

  has_ws    <- length(ws_avail) > 0
  fill_var  <- if (has_ws) "wind_support" else "wind_speed"
  fill_cols <- if (has_ws) ws_avail else spd_avail
  fill_lbl  <- if (has_ws) "Wind support\n(m/s)" else "Wind speed\n(m/s)"

  wind_long <- NULL
  if (length(fill_cols) > 0) {
    wind_long <- plot_df %>%
      select(timestamp, all_of(fill_cols)) %>%
      pivot_longer(
        cols      = all_of(fill_cols),
        names_to  = "level_col",
        values_to = fill_var
      ) %>%
      mutate(pressure_hPa = as.numeric(
        gsub("wind_support_|wind_speed_", "", level_col)))
  }

  # ── Map preparation ───────────────────────────────────────────────────────
  dep_row <- core_df %>% slice_min(timestamp, n = 1)
  arr_row <- core_df %>% slice_max(timestamp, n = 1)

  xlims <- c(min(dep_row$.lon, arr_row$.lon) - buffer_deg,
             max(dep_row$.lon, arr_row$.lon) + buffer_deg)
  ylims <- c(min(dep_row$.lat, arr_row$.lat) - buffer_deg,
             max(dep_row$.lat, arr_row$.lat) + buffer_deg)

  extent_pts <- tryCatch(
    sf::st_as_sf(
      data.frame(lon = c(dep_row$.lon, arr_row$.lon),
                 lat = c(dep_row$.lat, arr_row$.lat)),
      coords = c("lon", "lat"), crs = 4326
    ), error = function(e) NULL
  )

  elev_rast <- NULL
  if (!is.null(extent_pts)) {
    elev_rast <- tryCatch({
      r <- terra::rast(elevatr::get_elev_raster(extent_pts, z = elev_z, expand = 1))
      terra::values(r)[terra::values(r) < 0] <- 0
      r
    }, error = function(e) { if (verbose) message("  Elevation raster failed: ", conditionMessage(e)); NULL })
  }

  elev_profile <- NULL
  if (!is.null(elev_rast) && !is.null(extent_pts) &&
      nrow(extent_pts) >= 2 &&
      exists("extract_elevation_segments", mode = "function")) {
    elev_profile <- tryCatch(
      extract_elevation_segments(raster = elev_rast, tag = extent_pts),
      error = function(e) NULL
    )
  }

  dist_km <- tryCatch(
    round(as.numeric(sf::st_distance(
      sf::st_sfc(sf::st_point(c(dep_row$.lon, dep_row$.lat)), crs = 4326),
      sf::st_sfc(sf::st_point(c(arr_row$.lon, arr_row$.lat)), crs = 4326)
    )) / 1000, 1),
    error = function(e) NA_real_
  )

  countries <- tryCatch(rnaturalearth::ne_countries(scale = 10),
                        error = function(e) NULL)

  title_str <- paste0(
    indiv_chr, "  |  ", format(t_start, "%Y-%m-%d %H:%M"), " – ",
    format(t_end, "%H:%M UTC"),
    "  |  fw: ", cfg$firmware,
    "  |  ", cfg$pressure_type,
    if (!is.na(dist_km)) paste0("  |  ", dist_km, " km") else "",
    if (cfg$interpolation != "none") paste0("  |  interp: ", cfg$interpolation) else ""
  )

  # ─ Panel 1: Map ────────────────────────────────────────────────────────────
  p_map <- ggplot() +
    { if (!is.null(elev_rast)) geom_spatraster(data = elev_rast) } +
    { if (!is.null(elev_rast))
        scale_fill_hypso_c(name = "Elevation\n(m)", palette = "arctic_bathy",
                           limits = c(0, 4000), na.value = NA) } +
    { if (!is.null(countries))
        geom_sf(data = countries, fill = NA,
                color = if (theme_dark) "grey70" else "grey80",
                linewidth = 0.3) } +
    # Full individual track for context
    geom_path(data = df_i %>% filter(timestamp >= t_ctx_s, timestamp <= t_ctx_e),
              aes(.lon, .lat), col = "grey55", linewidth = 0.3, alpha = 0.6) +
    # Interpolated or straight-line flight segment
    { if (cfg$interpolation != "none" && nrow(interp_df) > 1)
        geom_path(data = interp_df, aes(.lon, .lat),
                  col = "white", linewidth = 0.6, linetype = "dashed",
                  inherit.aes = FALSE)
      else
        geom_segment(aes(x = dep_row$.lon, y = dep_row$.lat,
                         xend = arr_row$.lon, yend = arr_row$.lat),
                     col = "white", linewidth = 1, linetype = "dashed") } +
    # Window fixes coloured by time
    geom_point(data = core_df, aes(.lon, .lat, col = as.numeric(timestamp)),
               size = 1.5, alpha = 0.85) +
    scale_color_viridis_c(option = "plasma", guide = "none") +
    geom_point(data = dep_row, aes(.lon, .lat), col = "#4DAF4A", size = 4, shape = 16) +
    geom_point(data = arr_row, aes(.lon, .lat), col = "#FF7F00", size = 4, shape = 17) +
    coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
    labs(title = title_str, x = "Longitude", y = "Latitude") +
    .pt(9) +
    theme(plot.title = element_text(size = 7, face = "bold"))

  # ─ Panel 2: Ground elevation profile ───────────────────────────────────────
  p_elev <- NULL
  if (!is.null(elev_profile) && nrow(elev_profile) > 0) {
    max_km <- max(elev_profile$distance_from_origin, na.rm = TRUE)
    p_elev <- ggplot(elev_profile, aes(distance_from_origin, elevation)) +
      geom_ribbon(aes(ymin = 0, ymax = pmax(elevation, 0)),
                  fill = if (theme_dark) "#5C4033" else "#D2B48C", alpha = 0.55) +
      geom_line(col = if (theme_dark) "#FFD700" else "#8B4513", linewidth = 0.8) +
      labs(x = "Distance from start (km)", y = "Ground elevation (m)",
           subtitle = paste0("Terrain along flight path  |  ", round(max_km, 0), " km total")) +
      .pt(9) +
      theme(plot.subtitle = element_text(size = 6, color = "grey60"))
  }

  # ─ Panel 3: Wind-support heatmap ───────────────────────────────────────────
  p_heatmap <- NULL
  if (!is.null(wind_long)) {
    level_gaps <- diff(sort(unique(wind_long$pressure_hPa)))
    tile_h     <- if (length(level_gaps) > 0) min(level_gaps) else 50

    p_heatmap <- ggplot(wind_long,
                        aes(timestamp, pressure_hPa, fill = .data[[fill_var]])) +
      geom_tile(height = tile_h) +
      { if (has_ws)
          scale_fill_gradient2(low = "#2166AC", mid = "white", high = "#D73027",
                               midpoint = 0, na.value = "grey50", name = fill_lbl)
        else
          scale_fill_viridis_c(na.value = "grey50", option = "viridis",
                               name = fill_lbl) } +
      scale_y_reverse(breaks = press_breaks, labels = press_labels,
                      name = "Pressure level") +
      scale_x_datetime(expand = expansion(0)) +
      { if (.has(plot_df, pr_col))
          geom_path(data = plot_df %>% filter(!is.na(.data[[pr_col]])),
                    aes(timestamp, .data[[pr_col]]),
                    col = "white", linewidth = 1.8, inherit.aes = FALSE) } +
      { if (.has(plot_df, "best_wind_level"))
          geom_path(data = plot_df %>% filter(!is.na(best_wind_level)),
                    aes(timestamp, as.numeric(best_wind_level)),
                    col = "yellow", linetype = "dashed", linewidth = 1.1,
                    inherit.aes = FALSE) } +
      geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.7) +
      geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.7) +
      labs(x = "Time (UTC)",
           subtitle = if (has_ws)
             "White path = tag pressure  |  Yellow dashed = best-wind level"
           else
             "White path = tag pressure  (wind speed shown — heading unavailable)") +
      .pt(9) +
      theme(legend.position = "right",
            plot.subtitle = element_text(size = 6, color = "grey60"))
  }

  # ─ Panel 4: Wind support per level (lines) ─────────────────────────────────
  p_ws_lines <- NULL
  if (!is.null(wind_long) && has_ws) {
    lvl_pal <- colorRampPalette(c("#08306B", "#41AE76", "#FEC44F", "#D73027"))(
      length(press_breaks))
    names(lvl_pal) <- as.character(press_breaks)

    # Thicken lines at the animal's matched level (if available)
    matched_levels <- if (.has(plot_df, "matched_pressure_level"))
      as.character(unique(na.omit(plot_df$matched_pressure_level)))
    else character(0)

    lw_vals <- setNames(
      ifelse(names(lvl_pal) %in% matched_levels, 2, 0.65),
      names(lvl_pal)
    )

    p_ws_lines <- ggplot(
      wind_long %>% filter(!is.na(wind_support)),
      aes(timestamp, wind_support,
          group     = level_col,
          col       = as.character(pressure_hPa),
          linewidth = as.character(pressure_hPa))
    ) +
      geom_hline(yintercept = 0, col = "grey50", linetype = "dotted") +
      geom_path(alpha = 0.85) +
      scale_color_manual(values = lvl_pal, name = "Level\n(hPa)") +
      scale_linewidth_manual(values = lw_vals, guide = "none") +
      geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.7) +
      geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.7) +
      scale_x_datetime(expand = expansion(0)) +
      labs(x = "Time (UTC)", y = "Wind support (m/s)",
           subtitle = if (length(matched_levels) > 0)
             "Thick line = animal's matched pressure level" else NULL) +
      .pt(9) +
      theme(legend.position = "right",
            plot.subtitle = element_text(size = 6, color = "grey60"))
  }

  # ─ Panel 5: Tag pressure / flight altitude over time ───────────────────────
  p_pressure <- ggplot() +
    geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
               linetype = "dashed", linewidth = 0.7) +
    geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
               linetype = "dashed", linewidth = 0.7)

  for (lv in press_breaks)
    p_pressure <- p_pressure +
      geom_hline(yintercept = lv, col = "grey70", linetype = "dotted",
                 linewidth = 0.35)

  if (.has(plot_df, pr_col)) {
    # Instantaneous → path; windowed minimum → step function
    geom_fn <- if (cfg$pressure_type == "instantaneous") geom_path else geom_step
    p_pressure <- p_pressure +
      geom_fn(data  = plot_df %>% filter(!is.na(.data[[pr_col]])),
              aes(timestamp, .data[[pr_col]]),
              col = "black", linewidth = 1) +
      geom_point(data = plot_df %>% filter(!is.na(.data[[pr_col]])),
                 aes(timestamp, .data[[pr_col]], col = as.numeric(timestamp)),
                 size = 1.5)
  }

  p_pressure <- p_pressure +
    scale_y_reverse(breaks = press_breaks, labels = press_labels,
                    name   = press_y_label) +
    scale_color_viridis_c(option = "plasma", guide = "none") +
    scale_x_datetime(expand = expansion(0)) +
    labs(x = "Time (UTC)") +
    .pt(9)

  # ─ Panel 6: Flight-level vs best-wind comparison ───────────────────────────
  p_comparison <- ggplot() +
    geom_hline(yintercept = 0, col = "grey50", linetype = "dotted") +
    geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
               linetype = "dashed", linewidth = 0.7) +
    geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
               linetype = "dashed", linewidth = 0.7)

  if (.has(plot_df, "wind_support_flight"))
    p_comparison <- p_comparison +
      geom_path(data  = plot_df %>% filter(!is.na(wind_support_flight)),
                aes(timestamp, wind_support_flight),
                col = "black", linewidth = 1) +
      geom_point(data = plot_df %>% filter(!is.na(wind_support_flight)),
                 aes(timestamp, wind_support_flight),
                 col = "black", size = 1.2)

  if (.has(plot_df, "best_wind_support"))
    p_comparison <- p_comparison +
      geom_path(data  = plot_df %>% filter(!is.na(best_wind_support)),
                aes(timestamp, best_wind_support),
                col = "#FF7F00", linewidth = 1, linetype = "dashed") +
      geom_point(data = plot_df %>% filter(!is.na(best_wind_support)),
                 aes(timestamp, best_wind_support),
                 col = "#FF7F00", size = 1.2)

  if (.has(plot_df, "wind_support_10m"))
    p_comparison <- p_comparison +
      geom_path(data  = plot_df %>% filter(!is.na(wind_support_10m)),
                aes(timestamp, wind_support_10m),
                col = "grey55", linewidth = 0.7, linetype = "dashed")

  p_comparison <- p_comparison +
    scale_x_datetime(expand = expansion(0)) +
    labs(x = "Time (UTC)", y = "Wind support (m/s)",
         subtitle = "Black = flight level  |  Orange dashed = best available  |  Grey = 10 m surface") +
    .pt(9) +
    theme(plot.subtitle = element_text(size = 6, color = "grey60"))

  # ─ Panel 7: VeDBA ─────────────────────────────────────────────────────────
  p_vedba <- NULL
  if (.has(plot_df, cfg$vedba_col)) {
    vedba_y_lbl <- switch(cfg$vedba_aggregation %||% "precomputed",
      precomputed = "VeDBA (summed over window)",
      sum         = paste0("VeDBA (sum / ", cfg$vedba_window_min %||% "?", " min)"),
      "VeDBA"
    )
    p_vedba <- ggplot(plot_df %>% filter(!is.na(.data[[cfg$vedba_col]])),
                      aes(timestamp, .data[[cfg$vedba_col]])) +
      geom_path(col = "#E41A1C", linewidth = 0.9) +
      geom_point(col = "#E41A1C", size = 1.2) +
      geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.7) +
      geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.7) +
      scale_x_datetime(expand = expansion(0)) +
      labs(x = "Time (UTC)", y = vedba_y_lbl) +
      .pt(9)
  }

  # ─ Panel 8: Temperature ─────────────────────────────────────────────────────
  p_temp <- NULL
  if (.has(plot_df, cfg$temp_col)) {
    temp_y_lbl <- switch(cfg$temp_aggregation %||% "mean",
      mean    = "Temperature — mean (°C)",
      min     = "Temperature — min (°C)",
      max     = "Temperature — max (°C)",
      instant = "Temperature (°C)",
      "Temperature (°C)"
    )
    p_temp <- ggplot(plot_df %>% filter(!is.na(.data[[cfg$temp_col]])),
                     aes(timestamp, .data[[cfg$temp_col]])) +
      geom_path(col = "#377EB8", linewidth = 0.9) +
      geom_point(col = "#377EB8", size = 1.2) +
      geom_vline(xintercept = as.numeric(t_start), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.7) +
      geom_vline(xintercept = as.numeric(t_end),   col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.7) +
      scale_x_datetime(expand = expansion(0)) +
      labs(x = "Time (UTC)", y = temp_y_lbl) +
      .pt(9)
  }

  # ─ Assemble panels ──────────────────────────────────────────────────────────
  # Row 1: map  |  elevation profile  (elevation omitted if unavailable)
  p_top <- if (!is.null(p_elev))
    ggarrange(p_map, p_elev, ncol = 2, widths = c(1.3, 1))
  else
    ggarrange(p_map, ncol = 1)

  # Row 2: heatmap  |  tag pressure
  p_mid <- ggarrange(
    p_heatmap %||_plt% (ggplot() + .pt() +
                          labs(title = "Wind heatmap\n(no wind data available)")),
    p_pressure,
    ncol = 2, widths = c(1.4, 1)
  )

  # Row 3: wind lines per level  |  comparison
  p_lower <- ggarrange(
    p_ws_lines %||_plt% (ggplot() + .pt() +
                           labs(title = "Wind per level\n(heading required for wind support)")),
    p_comparison,
    ncol = 2
  )

  rows    <- list(p_top, p_mid, p_lower)
  heights <- c(1.5, 1.4, 1.2)

  # Optional row: VeDBA and/or temperature
  extra <- Filter(Negate(is.null), list(p_vedba, p_temp))
  if (length(extra) > 0) {
    rows    <- c(rows, list(ggarrange(plotlist = extra, ncol = length(extra))))
    heights <- c(heights, 0.9)
  }

  p_all  <- ggarrange(plotlist = rows, ncol = 1, heights = heights)
  fig_h  <- sum(heights) * 2.0

  # ─ Save ─────────────────────────────────────────────────────────────────────
  out_obj <- list(
    config        = cfg,
    individual_id = indiv_chr,
    t_start       = t_start,
    t_end         = t_end,
    raw_data      = core_df,
    interp_track  = if (cfg$interpolation != "none") interp_df else NULL,
    plot_data     = plot_df,
    wind_long     = wind_long,
    elev_profile  = elev_profile,
    plot          = p_all
  )

  if (!is.null(cfg$out_path) && nchar(cfg$out_path) > 0) {
    if (!dir.exists(cfg$out_path))
      dir.create(cfg$out_path, recursive = TRUE)
    fstem <- file.path(
      cfg$out_path,
      paste0(indiv_chr, "_", format(as.Date(t_start), "%Y-%m-%d"),
             "_wind_profile")
    )
    ggplot2::ggsave(paste0(fstem, ".png"), p_all,
                    width = 14, height = fig_h, dpi = 200,
                    bg = if (theme_dark) "black" else "white")
    saveRDS(out_obj, paste0(fstem, ".rds"))
    if (verbose) {
      message("Saved PNG: ", basename(paste0(fstem, ".png")))
      message("Saved RDS: ", basename(paste0(fstem, ".rds")))
    }
  }

  invisible(out_obj)
}
