# scan_night_wind_profile.R
# ─────────────────────────────────────────────────────────────────────────────
# Wind-column analysis for individual migration nights.
#
# Workflow:
#   1. Run annotate_era5() on your move2/sf track data to add wind columns at
#      all pressure levels plus flight-level matching.
#   2. Call find_complete_nights() to get a table of nights that have both
#      sufficient displacement and ERA5 pressure coverage.
#   3. Call scan_night_wind_profile() for each qualifying night to produce a
#      multi-panel diagnostic PNG.
#
# Designed primarily for 2026 finescale NanoFox data (barometric_pressure =
# instantaneous pressure every ~36 min).  The pressure-sensor logic mirrors
# scan_migration_nights.R: finescale NanoFox is detected when tag_firmware
# contains "finescale" or barometric_pressure is present and not aggregated.
#
# Dependencies (must be sourced before use):
#   pressure_to_altitude_m()   — pressure_to_altitude_m.R
#   extract_elevation_segments() — extract_elevation_segments.R
# ─────────────────────────────────────────────────────────────────────────────


# ── find_complete_nights() ────────────────────────────────────────────────────
#
#' Identify migration nights suitable for wind-column analysis
#'
#' Scans an annotated move2/sf location object for consecutive-day pairs where:
#'   (a) displacement between last evening fix and first morning fix exceeds
#'       \code{min_displacement_km}, and
#'   (b) ERA5 wind-support columns at \code{pressure_levels} have at least
#'       \code{min_coverage} non-NA values in the night window.
#'
#' @param data       sf/move2 object output from \code{annotate_era5()}.
#'   Should contain location-only rows (sensor_type == "location") or at
#'   minimum have non-empty geometries.
#' @param pressure_levels Numeric vector of hPa levels to check for ERA5
#'   coverage. Default \code{c(500,600,700,800,850,900,925,950,1000)}.
#' @param min_displacement_km Minimum straight-line displacement (km) between
#'   the last fix of night N and the first fix of morning N+1. Default 100.
#' @param min_coverage Fraction (0–1) of night-window rows that must have
#'   non-NA wind-support at every requested pressure level. Default 0.5.
#' @param pressure_col Column with tag barometric pressure (hPa).
#'   \code{NULL} = auto-detect (\code{barometric_pressure},
#'   \code{min_3h_pressure}, \code{tinyfox_pressure_min_last_24h}).
#' @param individual_col Column identifying individuals.
#'   Default \code{"individual_local_identifier"}.
#' @param verbose Logical; print progress. Default \code{TRUE}.
#'
#' @return A data frame with one row per qualifying (individual, night) pair:
#'   \describe{
#'     \item{individual_local_identifier}{Animal ID}
#'     \item{night_date}{Date of the night (Date of departure fix)}
#'     \item{t_dep}{POSIXct — last fix before midnight of night_date}
#'     \item{t_arr}{POSIXct — first fix on the morning after}
#'     \item{displacement_km}{Straight-line km between dep and arr fixes}
#'     \item{n_night_fixes}{Number of location fixes in the night window}
#'     \item{n_pressure_fixes}{Fixes with non-NA tag pressure in window}
#'     \item{era5_coverage}{Fraction of night fixes with complete wind data}
#'     \item{pressure_col_used}{Name of the tag pressure column detected}
#'   }
#'
#' @export
find_complete_nights <- function(
    data,
    pressure_levels      = c(500, 600, 700, 800, 850, 900, 925, 950, 1000),
    min_displacement_km  = 100,
    min_coverage         = 0.5,
    pressure_col         = NULL,
    individual_col       = "individual_local_identifier",
    verbose              = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(sf)
  })

  stopifnot(inherits(data, "sf"))
  stopifnot(individual_col %in% names(data))

  # ── Auto-detect tag pressure column ───────────────────────────────────────
  if (is.null(pressure_col)) {
    candidates <- c("barometric_pressure", "min_3h_pressure",
                    "tinyfox_pressure_min_last_24h")
    pressure_col <- candidates[candidates %in% names(data)][1]
    if (!is.na(pressure_col) && verbose)
      message("find_complete_nights: using pressure column '", pressure_col, "'")
    if (is.na(pressure_col)) pressure_col <- NULL
  }

  # ── Drop empty geometries; extract coordinates ─────────────────────────────
  data <- data[!sf::st_is_empty(data), ]
  if (nrow(data) == 0) stop("No non-empty geometries in data.")

  coords <- sf::st_coordinates(data)
  df <- data %>%
    sf::st_drop_geometry() %>%
    mutate(
      .lon      = coords[, "X"],
      .lat      = coords[, "Y"],
      .date     = as.Date(timestamp),
      .individual = as.character(.data[[individual_col]])
    ) %>%
    arrange(.individual, timestamp)

  # ── ERA5 wind-support columns present ────────────────────────────────────
  ws_cols <- paste0("wind_support_", pressure_levels)
  ws_avail <- ws_cols[ws_cols %in% names(df)]
  if (length(ws_avail) == 0) {
    stop("No wind_support_* columns found. Run annotate_era5() first.")
  }

  # ── Per-individual, per-consecutive-date-pair assessment ─────────────────
  results  <- list()
  indivs   <- unique(df$.individual)

  if (verbose) {
    cat("  Scanning individuals: ")
    pb <- utils::txtProgressBar(min = 0, max = length(indivs), style = 3)
  }

  for (i_ind in seq_along(indivs)) {
    indiv <- indivs[i_ind]
    if (verbose) utils::setTxtProgressBar(pb, i_ind)
    idf <- df %>% filter(.individual == indiv) %>% arrange(timestamp)

    dates <- sort(unique(idf$.date))
    if (length(dates) < 2) next

    for (d_idx in seq_len(length(dates) - 1)) {
      d_night <- dates[d_idx]
      d_morn  <- dates[d_idx + 1L]

      # Only consider date pairs that are exactly 1 day apart (consecutive nights)
      if (as.integer(d_morn - d_night) != 1L) next

      # Departure = last fix on d_night
      dep_row <- idf %>% filter(.date == d_night) %>% slice_max(timestamp, n = 1)
      # Arrival = first fix on d_morn
      arr_row <- idf %>% filter(.date == d_morn)  %>% slice_min(timestamp, n = 1)

      if (nrow(dep_row) == 0 || nrow(arr_row) == 0) next

      # Displacement via Haversine
      dist_km <- tryCatch({
        as.numeric(sf::st_distance(
          sf::st_sfc(sf::st_point(c(dep_row$.lon, dep_row$.lat)), crs = 4326),
          sf::st_sfc(sf::st_point(c(arr_row$.lon, arr_row$.lat)), crs = 4326)
        )) / 1000
      }, error = function(e) NA_real_)

      if (is.na(dist_km) || dist_km < min_displacement_km) next

      # Night window: departure timestamp → arrival timestamp
      t_dep <- dep_row$timestamp
      t_arr <- arr_row$timestamp

      # All fixes strictly inside the night window
      night_fixes <- idf %>%
        filter(timestamp >= t_dep, timestamp <= t_arr)

      n_night <- nrow(night_fixes)

      # ERA5 coverage: fraction of night rows with all requested wind cols non-NA
      if (n_night > 0 && length(ws_avail) > 0) {
        complete_rows <- rowSums(is.na(night_fixes[, ws_avail, drop = FALSE])) == 0
        era5_cov <- mean(complete_rows)
      } else {
        era5_cov <- 0
      }

      if (era5_cov < min_coverage) next

      # Tag pressure coverage in window
      n_press <- if (!is.null(pressure_col) && pressure_col %in% names(night_fixes))
        sum(!is.na(night_fixes[[pressure_col]]))
      else 0L

      results[[length(results) + 1L]] <- data.frame(
        individual_local_identifier = indiv,
        night_date      = d_night,
        t_dep           = t_dep,
        t_arr           = t_arr,
        displacement_km = round(dist_km, 1),
        n_night_fixes   = n_night,
        n_pressure_fixes = n_press,
        era5_coverage   = round(era5_cov, 3),
        pressure_col_used = if (!is.null(pressure_col)) pressure_col else NA_character_,
        stringsAsFactors = FALSE
      )
    }
  }

  if (verbose) { close(pb); cat("\n") }

  if (length(results) == 0) {
    if (verbose) message("No qualifying nights found.")
    return(data.frame())
  }

  out <- do.call(rbind, results)
  out$t_dep <- as.POSIXct(out$t_dep, tz = "UTC", origin = "1970-01-01")
  out$t_arr <- as.POSIXct(out$t_arr, tz = "UTC", origin = "1970-01-01")
  rownames(out) <- NULL

  if (verbose)
    message("find_complete_nights: ", nrow(out), " qualifying nights across ",
            length(unique(out$individual_local_identifier)), " individuals.")
  out
}


# ── scan_night_wind_profile() ─────────────────────────────────────────────────
#
#' Multi-panel wind-column diagnostic plot for a single migration night
#'
#' Produces a figure with six panels:
#' \enumerate{
#'   \item \strong{Map} — full track (grey), departure (green circle) and
#'     arrival (orange triangle), terrain elevation.
#'   \item \strong{Wind support heatmap} — x = time, y = pressure level
#'     (reversed so altitude increases upward), fill = wind support (m/s,
#'     diverging blue–white–red).  Animal's flight pressure overlaid as a
#'     thick white line; best-wind level as a yellow dashed line.
#'   \item \strong{Wind support per level} — one coloured line per pressure
#'     level over time; animal's matched level drawn thicker.
#'   \item \strong{Tag pressure / flight altitude} — animal's barometric
#'     pressure (black) and derived altitude (purple) over time; ERA5 pressure
#'     level reference lines (grey horizontal dashes).
#'   \item \strong{Wind support comparison} — flight-level wind support
#'     (black) vs. best-available wind support (orange) vs. 10m surface wind
#'     support (grey dashed), if columns are present.
#'   \item \strong{Estimated airspeed} — airspeed at the matched pressure
#'     level over time (red), if available.
#' }
#'
#' @param data       sf/move2 object output from \code{annotate_era5()}.
#' @param individual_id Character; individual to plot.
#' @param night_date  Date or character \code{"YYYY-MM-DD"}; the departure
#'   night.  If \code{NULL}, \code{t_dep}/\code{t_arr} must be supplied.
#' @param t_dep      POSIXct departure time (overrides \code{night_date}).
#' @param t_arr      POSIXct arrival time   (overrides \code{night_date}).
#' @param pressure_levels Numeric vector of hPa levels. Default
#'   \code{c(500,600,700,800,850,900,925,950,1000)}.
#' @param pressure_col Tag pressure column (hPa). \code{NULL} = auto-detect.
#' @param elev_z     Zoom level for \code{elevatr::get_elev_raster}. Default 6.
#' @param buffer_deg Map padding in degrees. Default 1.5.
#' @param out_path   Directory to save PNG, or \code{NULL} to return plot only.
#' @param theme_dark Logical; dark background. Default \code{FALSE}.
#' @param verbose    Logical. Default \code{TRUE}.
#'
#' @return Invisibly returns the ggplot object.
#'
#' @export
scan_night_wind_profile <- function(
    data,
    individual_id,
    night_date   = NULL,
    t_dep        = NULL,
    t_arr        = NULL,
    pressure_levels = c(500, 600, 700, 800, 850, 900, 925, 950, 1000),
    pressure_col = NULL,
    # ── Lagerveld flight-profile parameters ─────────────────────────────────
    vmp_sl_ms    = 7.5,    # Sea-level minimum power speed (m/s)
    vmr_sl_ms    = NULL,   # Sea-level max range speed; NULL → 3^0.25 × Vmp
    vmp_sd_ms    = 1.0,    # SD for Vmp reference band
    vmr_sd_ms    = 1.1,    # SD for Vmr reference band
    species_label = NULL,  # e.g. "Nyctalus leisleri"
    # ────────────────────────────────────────────────────────────────────────
    elev_z       = 6,
    buffer_deg   = 1.5,
    out_path     = NULL,
    theme_dark   = FALSE,
    verbose      = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(sf)
    library(ggplot2)
    library(ggpubr)
    library(terra)
    library(tidyterra)
    library(rnaturalearth)
    library(elevatr)
  })

  # Source helpers if not loaded
  for (.src in c("R/pressure_to_altitude_m.R",
                 "../SigfoxTagPrep/R/pressure_to_altitude_m.R")) {
    if (!exists("pressure_to_altitude_m", mode = "function") && file.exists(.src))
      source(.src)
  }
  for (.src in c("R/extract_elevation_segments.R",
                 "../SigfoxTagPrep/R/extract_elevation_segments.R")) {
    if (!exists("extract_elevation_segments", mode = "function") && file.exists(.src))
      source(.src)
  }
  for (.src in c("R/lagerveld_flight_profile.R",
                 "../SigfoxTagPrep/R/lagerveld_flight_profile.R")) {
    if (!exists("compute_night_profiles", mode = "function") && file.exists(.src))
      source(.src)
  }
  if (is.null(vmr_sl_ms)) vmr_sl_ms <- vmp_sl_ms * 3^(1/4)

  # ── Helpers ────────────────────────────────────────────────────────────────
  .has <- function(df, col) !is.null(col) && col %in% names(df) &&
                              any(!is.na(df[[col]]))

  .pt <- function(base_size = 9) {
    bg <- if (theme_dark) "black" else "white"
    fg <- if (theme_dark) "white" else "black"
    theme_minimal(base_size = base_size) +
      theme(
        panel.background = element_rect(fill = bg, color = NA),
        plot.background  = element_rect(fill = bg, color = NA),
        text             = element_text(color = fg),
        axis.text        = element_text(color = fg),
        axis.title       = element_text(color = fg),
        legend.background = element_rect(fill = bg, color = NA),
        legend.text       = element_text(color = fg),
        legend.title      = element_text(color = fg),
        panel.grid        = element_line(
          color = if (theme_dark) "grey25" else "grey90")
      )
  }

  # ── Validate / filter individual ──────────────────────────────────────────
  stopifnot(inherits(data, "sf"))
  indiv_chr <- as.character(individual_id)
  id_col <- if ("individual_local_identifier" %in% names(data))
    "individual_local_identifier"
  else if ("deployment_id" %in% names(data)) "deployment_id"
  else stop("Cannot find individual ID column in data.")

  data_i <- data[as.character(data[[id_col]]) == indiv_chr, ]
  data_i <- data_i[!sf::st_is_empty(data_i), ]
  if (nrow(data_i) < 2)
    stop("Fewer than 2 valid rows for individual '", indiv_chr, "'.")

  # ── Determine night window ─────────────────────────────────────────────────
  coords_i  <- sf::st_coordinates(data_i)
  df_i <- data_i %>%
    sf::st_drop_geometry() %>%
    mutate(.lon = coords_i[, "X"], .lat = coords_i[, "Y"],
           .date = as.Date(timestamp)) %>%
    arrange(timestamp)

  if (!is.null(t_dep) && !is.null(t_arr)) {
    # Explicit times supplied
    t_dep <- as.POSIXct(t_dep, tz = "UTC")
    t_arr <- as.POSIXct(t_arr, tz = "UTC")
  } else {
    if (is.null(night_date))
      stop("Supply either night_date or both t_dep and t_arr.")
    nd <- as.Date(night_date)
    # Departure = last fix on night_date
    dep_row <- df_i %>% filter(.date == nd) %>% slice_max(timestamp, n = 1)
    # Arrival  = first fix the morning after
    arr_row <- df_i %>% filter(.date == nd + 1L) %>% slice_min(timestamp, n = 1)
    if (nrow(dep_row) == 0)
      stop("No fixes found on night_date=", nd, " for individual '", indiv_chr, "'.")
    if (nrow(arr_row) == 0)
      stop("No fixes found on ", nd + 1L, " (arrival day) for '", indiv_chr, "'.")
    t_dep <- dep_row$timestamp[1]
    t_arr <- arr_row$timestamp[1]
  }

  # ── Auto-detect pressure column ────────────────────────────────────────────
  if (is.null(pressure_col)) {
    cands <- c("barometric_pressure", "min_3h_pressure",
               "tinyfox_pressure_min_last_24h")
    pressure_col <- cands[cands %in% names(df_i)][1]
    if (is.na(pressure_col)) pressure_col <- NULL
    else if (verbose) message("Using pressure column: ", pressure_col)
  }

  # ── Night-window data ──────────────────────────────────────────────────────
  # Expand window slightly for context (15 min before dep, 15 min after arr)
  t_ctx_start <- t_dep - 15 * 60
  t_ctx_end   <- t_arr + 15 * 60

  night_df <- df_i %>%
    filter(timestamp >= t_ctx_start, timestamp <= t_ctx_end) %>%
    arrange(timestamp)

  if (nrow(night_df) < 2) {
    stop("Fewer than 2 rows in night window for '", indiv_chr, "' on ",
         as.Date(t_dep), ".")
  }

  # Detect tag type
  is_finescale <- (.has(night_df, "tag_firmware") &&
    any(grepl("finescale", night_df$tag_firmware, ignore.case = TRUE),
        na.rm = TRUE)) ||
    (!is.null(pressure_col) && pressure_col == "barometric_pressure")

  if (verbose)
    message("Night window: ", format(t_dep, "%Y-%m-%d %H:%M"),
            " \u2013 ", format(t_arr, "%Y-%m-%d %H:%M"),
            " | ", nrow(night_df), " rows | finescale=", is_finescale)

  # ── Wind-support reshape (long format for heatmap) ─────────────────────────
  ws_cols   <- paste0("wind_support_", pressure_levels)
  ws_avail  <- ws_cols[ws_cols %in% names(night_df)]
  spd_cols  <- paste0("wind_speed_",   pressure_levels)
  spd_avail <- spd_cols[spd_cols %in% names(night_df)]

  has_ws <- length(ws_avail) > 0

  # Choose fill variable: wind_support if available, else wind_speed
  fill_var  <- if (has_ws) "wind_support" else "wind_speed"
  fill_cols <- if (has_ws) ws_avail else spd_avail[spd_avail %in% names(night_df)]
  fill_label <- if (has_ws) "Wind support\n(m/s)" else "Wind speed\n(m/s)"

  wind_long <- NULL
  if (length(fill_cols) > 0) {
    pivot_cols <- fill_cols
    wind_long <- night_df %>%
      select(timestamp, all_of(pivot_cols)) %>%
      pivot_longer(
        cols      = all_of(pivot_cols),
        names_to  = "level_col",
        values_to = fill_var
      ) %>%
      mutate(
        pressure_hPa = as.numeric(
          gsub("wind_support_|wind_speed_", "", level_col))
      )
  }

  # ── Pressure-level breaks with altitude labels ─────────────────────────────
  press_breaks <- sort(pressure_levels)
  alt_approx   <- if (exists("pressure_to_altitude_m", mode = "function"))
    round(pressure_to_altitude_m(press_breaks) / 100) * 100
  else
    round(44330 * (1 - (press_breaks / 1013.25)^0.1903) / 100) * 100

  press_labels <- paste0(press_breaks, "\n(", alt_approx, "m)")

  # ── Map preparation ────────────────────────────────────────────────────────
  dep_row_map <- df_i %>% filter(timestamp == t_dep)
  arr_row_map <- df_i %>% filter(timestamp == t_arr)
  if (nrow(dep_row_map) == 0) dep_row_map <- df_i %>% slice_min(abs(as.numeric(timestamp - t_dep)), n = 1)
  if (nrow(arr_row_map) == 0) arr_row_map <- df_i %>% slice_min(abs(as.numeric(timestamp - t_arr)), n = 1)

  xlims <- c(min(dep_row_map$.lon, arr_row_map$.lon) - buffer_deg,
             max(dep_row_map$.lon, arr_row_map$.lon) + buffer_deg)
  ylims <- c(min(dep_row_map$.lat, arr_row_map$.lat) - buffer_deg,
             max(dep_row_map$.lat, arr_row_map$.lat) + buffer_deg)

  # Elevation raster
  elev_rast <- NULL
  extent_pts <- tryCatch(
    sf::st_as_sf(data.frame(lon = c(dep_row_map$.lon, arr_row_map$.lon),
                            lat = c(dep_row_map$.lat, arr_row_map$.lat)),
                 coords = c("lon", "lat"), crs = 4326),
    error = function(e) NULL
  )
  if (!is.null(extent_pts)) {
    elev_rast <- tryCatch(
      terra::rast(elevatr::get_elev_raster(extent_pts, z = elev_z, expand = 1)),
      error = function(e) NULL
    )
    if (!is.null(elev_rast))
      terra::values(elev_rast)[terra::values(elev_rast) < 0] <- 0
  }

  # Elevation profile along flight path
  elev_profile <- NULL
  if (!is.null(elev_rast) && !is.null(extent_pts) &&
      exists("extract_elevation_segments", mode = "function")) {
    elev_profile <- tryCatch(
      extract_elevation_segments(raster = elev_rast, tag = extent_pts),
      error = function(e) NULL
    )
  }

  dist_km <- tryCatch(
    round(as.numeric(sf::st_distance(
      sf::st_sfc(sf::st_point(c(dep_row_map$.lon, dep_row_map$.lat)), crs = 4326),
      sf::st_sfc(sf::st_point(c(arr_row_map$.lon, arr_row_map$.lat)), crs = 4326)
    )) / 1000, 1),
    error = function(e) NA_real_
  )

  countries <- tryCatch(rnaturalearth::ne_countries(scale = 10),
                        error = function(e) NULL)

  title_str <- paste0(
    indiv_chr, "  |  ",
    format(t_dep, "%Y-%m-%d %H:%M"), " \u2013 ", format(t_arr, "%H:%M UTC"),
    if (!is.na(dist_km)) paste0("  |  ", dist_km, " km") else ""
  )

  # ─ Panel 1: Map ────────────────────────────────────────────────────────────
  p_map <- ggplot() +
    { if (!is.null(elev_rast))
        geom_spatraster(data = elev_rast) } +
    { if (!is.null(elev_rast))
        scale_fill_hypso_c(name = "Elevation (m)", palette = "arctic_bathy",
                           limits = c(0, 4000), na.value = NA) } +
    { if (!is.null(countries))
        geom_sf(data = countries, fill = NA,
                color = if (theme_dark) "grey70" else "grey80") } +
    # Full individual track — thin grey context
    geom_path(data = df_i, aes(.lon, .lat),
              col = "grey60", linewidth = 0.35, alpha = 0.5) +
    # Flight segment
    geom_segment(
      aes(x = dep_row_map$.lon, y = dep_row_map$.lat,
          xend = arr_row_map$.lon, yend = arr_row_map$.lat),
      col = "white", linewidth = 1.1, linetype = "dashed"
    ) +
    # Night-window fixes coloured by time
    geom_point(
      data = night_df,
      aes(.lon, .lat, col = as.numeric(timestamp)),
      size = 1.2, alpha = 0.8
    ) +
    scale_color_viridis_c(option = "plasma", guide = "none") +
    # Departure (green) and arrival (orange)
    geom_point(data = dep_row_map, aes(.lon, .lat),
               col = "#4DAF4A", size = 4, shape = 16) +
    geom_point(data = arr_row_map, aes(.lon, .lat),
               col = "#FF7F00", size = 4, shape = 17) +
    coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
    labs(title = title_str, x = "Longitude", y = "Latitude") +
    .pt(9) +
    theme(plot.title = element_text(size = 7.5, face = "bold"))

  # ─ Panel 2: Wind-support heatmap ───────────────────────────────────────────
  p_heatmap <- NULL
  if (!is.null(wind_long)) {

    # Tile height = gap between adjacent levels (or uniform spacing)
    level_gaps <- diff(sort(unique(wind_long$pressure_hPa)))
    tile_h     <- if (length(level_gaps) > 0) min(level_gaps) else 50

    p_heatmap <- ggplot(wind_long,
                        aes(timestamp, pressure_hPa, fill = .data[[fill_var]])) +
      geom_tile(height = tile_h) +
      { if (has_ws)
          scale_fill_gradient2(low  = "#2166AC", mid = "white", high = "#D73027",
                               midpoint = 0, na.value = "grey50",
                               name = fill_label)
        else
          scale_fill_viridis_c(na.value = "grey50", option = "viridis",
                               name = fill_label) } +
      scale_y_reverse(
        breaks = press_breaks,
        labels = press_labels,
        name   = "Pressure (hPa)\n[approx. altitude]"
      ) +
      scale_x_datetime(expand = expansion(0)) +
      # Animal's flight pressure (thick white line)
      { if (!is.null(pressure_col) && .has(night_df, pressure_col))
          geom_path(data   = night_df %>% filter(!is.na(.data[[pressure_col]])),
                    aes(timestamp, .data[[pressure_col]]),
                    col = "white", linewidth = 1.8, inherit.aes = FALSE) } +
      # Best wind level (yellow dashed)
      { if (.has(night_df, "best_wind_level"))
          geom_path(data   = night_df %>% filter(!is.na(best_wind_level)),
                    aes(timestamp, as.numeric(best_wind_level)),
                    col = "yellow", linetype = "dashed", linewidth = 1.2,
                    inherit.aes = FALSE) } +
      # Departure / arrival vlines
      geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.8) +
      geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.8) +
      labs(x = "Time (UTC)", y = "Pressure (hPa)\n[approx. altitude]",
           subtitle = if (has_ws) "White path = animal flight pressure; yellow dashed = best wind level"
                      else "White path = animal flight pressure (wind speed shown — heading unavailable)") +
      .pt(9) +
      theme(legend.position = "right",
            plot.subtitle = element_text(size = 6, color = "grey60"))
  }

  # ─ Panel 3: Wind support per level over time (line per level) ──────────────
  p_ws_lines <- NULL
  if (!is.null(wind_long) && has_ws) {
    # Colour scale: levels mapped from low (500 hPa = highest altitude, cold)
    # to high (1000 hPa = surface, warm)
    lvl_pal <- colorRampPalette(c("#08306B", "#41AE76", "#FEC44F", "#D73027"))(
      length(pressure_levels))
    names(lvl_pal) <- as.character(sort(pressure_levels))

    # Animal's matched level per row
    matched_df <- if (.has(night_df, "matched_pressure_level"))
      night_df %>%
        filter(!is.na(matched_pressure_level)) %>%
        select(timestamp, matched_pressure_level)
    else NULL

    p_ws_lines <- ggplot(wind_long %>% filter(!is.na(wind_support)),
                         aes(timestamp, wind_support,
                             group = level_col,
                             col   = as.character(pressure_hPa),
                             linewidth = as.character(pressure_hPa))) +
      geom_hline(yintercept = 0, col = "grey50", linetype = "dotted") +
      geom_path(alpha = 0.85) +
      scale_color_manual(values = lvl_pal, name = "Level\n(hPa)") +
      scale_linewidth_manual(
        values = setNames(
          ifelse(names(lvl_pal) %in%
                   as.character(if (!is.null(matched_df))
                     unique(matched_df$matched_pressure_level) else ""), 2, 0.6),
          names(lvl_pal)
        ),
        guide = "none"
      ) +
      geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
                 linetype = "dashed", linewidth = 0.7) +
      geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
                 linetype = "dashed", linewidth = 0.7) +
      scale_x_datetime(expand = expansion(0)) +
      labs(x = "Time (UTC)", y = "Wind support (m/s)",
           subtitle = "Thick line = animal's matched pressure level") +
      .pt(9) +
      theme(legend.position = "right",
            plot.subtitle = element_text(size = 6, color = "grey60"))
  }

  # ─ Panel 4: Tag pressure / flight altitude over time ───────────────────────
  p_pressure <- ggplot() +
    geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
               linetype = "dashed", linewidth = 0.7) +
    geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
               linetype = "dashed", linewidth = 0.7)

  # ERA5 pressure level reference lines
  for (lv in pressure_levels) {
    p_pressure <- p_pressure +
      geom_hline(yintercept = lv, col = "grey70", linetype = "dotted",
                 linewidth = 0.4)
  }

  if (!is.null(pressure_col) && .has(night_df, pressure_col)) {
    p_pressure <- p_pressure +
      geom_path(data  = night_df %>% filter(!is.na(.data[[pressure_col]])),
                aes(timestamp, .data[[pressure_col]]), col = "black", linewidth = 1) +
      geom_point(data = night_df %>% filter(!is.na(.data[[pressure_col]])),
                 aes(timestamp, .data[[pressure_col]],
                     col = as.numeric(timestamp)), size = 1.5)
  }

  # Pressure-derived altitude (right-side annotation via secondary axis)
  # Use breaks at standard levels
  p_pressure <- p_pressure +
    scale_y_reverse(
      breaks = press_breaks,
      labels = press_labels,
      name   = "Tag pressure (hPa)\n[approx. altitude]"
    ) +
    scale_color_viridis_c(option = "plasma", guide = "none") +
    scale_x_datetime(expand = expansion(0)) +
    labs(x = "Time (UTC)") +
    .pt(9)

  # ─ Panel 5: Flight-level vs best-wind support comparison ───────────────────
  p_comparison <- ggplot() +
    geom_hline(yintercept = 0, col = "grey50", linetype = "dotted") +
    geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
               linetype = "dashed", linewidth = 0.7) +
    geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
               linetype = "dashed", linewidth = 0.7)

  has_comparison_data <- FALSE

  if (.has(night_df, "wind_support_flight")) {
    p_comparison <- p_comparison +
      geom_path(data  = night_df %>% filter(!is.na(wind_support_flight)),
                aes(timestamp, wind_support_flight), col = "black", linewidth = 1) +
      geom_point(data = night_df %>% filter(!is.na(wind_support_flight)),
                 aes(timestamp, wind_support_flight), col = "black", size = 1.2)
    has_comparison_data <- TRUE
  }

  if (.has(night_df, "best_wind_support")) {
    p_comparison <- p_comparison +
      geom_path(data  = night_df %>% filter(!is.na(best_wind_support)),
                aes(timestamp, best_wind_support), col = "#FF7F00",
                linewidth = 1, linetype = "dashed") +
      geom_point(data = night_df %>% filter(!is.na(best_wind_support)),
                 aes(timestamp, best_wind_support), col = "#FF7F00", size = 1.2)
    has_comparison_data <- TRUE
  }

  if (.has(night_df, "wind_support_10m")) {
    p_comparison <- p_comparison +
      geom_path(data  = night_df %>% filter(!is.na(wind_support_10m)),
                aes(timestamp, wind_support_10m), col = "grey60",
                linewidth = 0.7, linetype = "dashed")
    has_comparison_data <- TRUE
  }

  p_comparison <- p_comparison +
    scale_x_datetime(expand = expansion(0)) +
    labs(x = "Time (UTC)", y = "Wind support (m/s)",
         subtitle = "Black = flight level  |  Orange dashed = best available  |  Grey = 10 m surface") +
    .pt(9) +
    theme(plot.subtitle = element_text(size = 6, color = "grey60"))

  # ─ Panel 6: Airspeed at flight level ──────────────────────────────────────
  p_airspeed <- ggplot() +
    geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
               linetype = "dashed", linewidth = 0.7) +
    geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
               linetype = "dashed", linewidth = 0.7)

  has_airspeed <- FALSE

  if (.has(night_df, "airspeed_flight")) {
    p_airspeed <- p_airspeed +
      geom_path(data  = night_df %>% filter(!is.na(airspeed_flight)),
                aes(timestamp, airspeed_flight), col = "#D73027", linewidth = 1) +
      geom_point(data = night_df %>% filter(!is.na(airspeed_flight)),
                 aes(timestamp, airspeed_flight), col = "#D73027", size = 1.2)
    has_airspeed <- TRUE
  } else {
    # Fall back to airspeed at the most common matched level
    as_cols <- paste0("airspeed_", pressure_levels)
    as_avail <- as_cols[as_cols %in% names(night_df)]
    if (length(as_avail) > 0) {
      # Pick median level
      mid_col <- as_avail[ceiling(length(as_avail) / 2)]
      lvl_nm  <- gsub("airspeed_", "", mid_col)
      if (.has(night_df, mid_col)) {
        p_airspeed <- p_airspeed +
          geom_path(data  = night_df %>% filter(!is.na(.data[[mid_col]])),
                    aes(timestamp, .data[[mid_col]]),
                    col = "#D73027", linewidth = 1) +
          geom_point(data = night_df %>% filter(!is.na(.data[[mid_col]])),
                     aes(timestamp, .data[[mid_col]]),
                     col = "#D73027", size = 1.2)
        has_airspeed <- TRUE
      }
    }
  }

  p_airspeed <- p_airspeed +
    scale_x_datetime(expand = expansion(0)) +
    labs(x = "Time (UTC)", y = "Estimated airspeed (m/s)",
         subtitle = if (has_airspeed) NULL else "No airspeed data (heading required)") +
    .pt(9)

  # ─ Lagerveld airspeed-altitude profile ────────────────────────────────────
  p_lagerveld   <- NULL
  p_opt_alt_time <- NULL
  night_profiles <- NULL

  if (exists("compute_night_profiles", mode = "function") &&
      any(paste0("era5_u", pressure_levels) %in% names(night_df))) {

    night_sf <- tryCatch({
      coords_night <- cbind(night_df$.lon, night_df$.lat)
      sf::st_as_sf(night_df, coords = c(".lon", ".lat"), crs = 4326)
    }, error = function(e) NULL)

    if (!is.null(night_sf)) {
      night_profiles <- tryCatch(
        compute_night_profiles(
          night_df        = night_sf,
          pressure_levels = pressure_levels,
          vmp_sl_ms       = vmp_sl_ms,
          vmr_sl_ms       = vmr_sl_ms,
          min_speed_ms    = 0.5
        ),
        error = function(e) {
          if (verbose) message("  [Lagerveld] compute_night_profiles failed: ", e$message)
          NULL
        }
      )
    }

    if (!is.null(night_profiles) && nrow(night_profiles) > 0) {
      # Observed altitude from tag pressure (ISA, for comparison line)
      obs_alt_m <- NULL
      if (!is.null(pressure_col) && .has(night_df, pressure_col)) {
        p_obs <- as.numeric(night_df[[pressure_col]])
        obs_alt_m <- 44330 * (1 - (p_obs / 1013.25)^0.1903)
        obs_alt_m[!is.finite(obs_alt_m)] <- NA
      }

      # Panel 7: Lagerveld airspeed-altitude profile (Fig. 3 style)
      p_lagerveld <- tryCatch(
        plot_lagerveld_profile(
          profiles        = night_profiles,
          vmp_sl_ms       = vmp_sl_ms,
          vmr_sl_ms       = vmr_sl_ms,
          vmp_sd_ms       = vmp_sd_ms,
          vmr_sd_ms       = vmr_sd_ms,
          observed_alt_m  = obs_alt_m,
          species_label   = species_label,
          theme_dark      = theme_dark
        ),
        error = function(e) {
          if (verbose) message("  [Lagerveld] plot_lagerveld_profile failed: ", e$message)
          NULL
        }
      )

      # Panel 8: Predicted optimal altitude over time vs observed altitude
      opt_summary <- tryCatch(
        summarise_optimal_altitudes(night_profiles, vmp_sl_ms, vmr_sl_ms),
        error = function(e) NULL
      )

      if (!is.null(opt_summary) && nrow(opt_summary) > 0) {
        p_opt_alt_time <- ggplot() +
          geom_vline(xintercept = as.numeric(t_dep), col = "#4DAF4A",
                     linetype = "dashed", linewidth = 0.7) +
          geom_vline(xintercept = as.numeric(t_arr), col = "#FF7F00",
                     linetype = "dashed", linewidth = 0.7) +
          # Feasible altitude range (ribbon)
          geom_ribbon(data = opt_summary %>%
                        filter(!is.na(feasible_alt_min_m), !is.na(feasible_alt_max_m)),
                      aes(t_mid,
                          ymin = feasible_alt_min_m,
                          ymax = feasible_alt_max_m),
                      fill = "grey60", alpha = 0.25) +
          # Predicted optimal altitude (coloured by min required airspeed)
          geom_path(data  = opt_summary,
                    aes(t_mid, optimal_alt_m, col = min_va_ms),
                    linewidth = 1.4) +
          geom_point(data = opt_summary,
                     aes(t_mid, optimal_alt_m, col = min_va_ms),
                     size = 2) +
          scale_color_gradient2(
            low = "#762A83", mid = "#35978F", high = "#D73027",
            midpoint = (vmp_sl_ms + vmr_sl_ms) / 2,
            name = "Min req.\nairspeed\n(m/s)"
          ) +
          # Observed altitude (black) if available
          { if (!is.null(obs_alt_m) && any(!is.na(obs_alt_m))) {
              obs_df <- night_df %>%
                filter(!is.na(.data[[if (!is.null(pressure_col) && pressure_col %in% names(night_df)) pressure_col else "timestamp"]])) %>%
                mutate(.alt_obs = 44330 * (1 - (.data[[pressure_col]] / 1013.25)^0.1903))
              list(
                geom_path(data  = obs_df %>% filter(!is.na(.alt_obs)),
                          aes(timestamp, .alt_obs),
                          col = "black", linewidth = 1.2, inherit.aes = FALSE),
                geom_point(data = obs_df %>% filter(!is.na(.alt_obs)),
                           aes(timestamp, .alt_obs),
                           col = "black", size = 1.2, inherit.aes = FALSE)
              )
            } } +
          # Vmp / Vmr reference lines (altitude-adjusted at median night altitude)
          {
            med_alt <- median(opt_summary$optimal_alt_m, na.rm = TRUE)
            list(
              geom_hline(yintercept = 0, col = "grey50", linetype = "dotted"),
              annotate("text", x = t_dep + 300, y = 100, label = "0 m (sea level)",
                       col = "grey50", size = 2, hjust = 0)
            )
          } +
          scale_x_datetime(expand = expansion(0)) +
          scale_y_continuous(name = "Altitude (m)", limits = c(0, NA)) +
          labs(x = "Time (UTC)",
               subtitle = paste0(
                 "Colour = min required airspeed  |  ",
                 "Black = observed (tag pressure)  |  ",
                 "Grey ribbon = feasible altitude range\n",
                 "Purple = Vmp (", round(vmp_sl_ms, 1),
                 " m/s)  |  Teal = Vmr (", round(vmr_sl_ms, 1), " m/s) at sea level"
               )) +
          .pt(9) +
          theme(legend.position = "right",
                plot.subtitle = element_text(size = 6, color = "grey60"))
      }
    }
  }

  # ─ Assemble panels ─────────────────────────────────────────────────────────
  # Row 1 (tall): map | heatmap
  # Row 2: wind-support lines | tag pressure
  # Row 3: flight vs best comparison | airspeed
  # Row 4 (Lagerveld): airspeed-altitude profile | predicted optimal alt over time

  bottom_panels <- list(
    p_ws_lines   %||_plt% ggplot() + .pt() + labs(title = "Wind support per level\n(heading required)"),
    p_pressure,
    p_comparison,
    p_airspeed
  )

  if (!is.null(p_heatmap)) {
    top_right <- p_heatmap
  } else {
    top_right <- ggplot() + .pt() +
      labs(title = "Wind support heatmap\n(no wind data available)")
  }

  has_lagerveld <- !is.null(p_lagerveld)

  p_top <- ggarrange(p_map, top_right, ncol = 2, widths = c(1, 1.4))
  p_bot <- ggarrange(plotlist = bottom_panels, ncol = 2, nrow = 2)

  if (has_lagerveld) {
    p_lag_row <- ggarrange(
      p_lagerveld,
      p_opt_alt_time %||_plt% ggplot() + .pt() + labs(title = "Optimal altitude over time\n(insufficient data)"),
      ncol = 2, widths = c(1, 1)
    )
    p_all <- ggarrange(p_top, p_bot, p_lag_row, ncol = 1,
                       heights = c(1.6, 2, 1.4))
    fig_h <- 14
  } else {
    p_all <- ggarrange(p_top, p_bot, ncol = 1, heights = c(1.6, 2))
    fig_h <- 10
  }

  # ─ Save ───────────────────────────────────────────────────────────────────
  if (!is.null(out_path)) {
    if (!dir.exists(out_path)) dir.create(out_path, recursive = TRUE)
    fname <- file.path(out_path, paste0(
      indiv_chr, "_", format(as.Date(t_dep), "%Y-%m-%d"), "_wind_profile.png"
    ))
    ggplot2::ggsave(fname, p_all, width = 12, height = fig_h, dpi = 200,
                    bg = if (theme_dark) "black" else "white")
    if (verbose) message("Saved: ", basename(fname))
  }

  invisible(p_all)
}


# ── Null-coalescing helper for ggplot objects (local to this file) ────────────
`%||_plt%` <- function(a, b) if (!is.null(a)) a else b
