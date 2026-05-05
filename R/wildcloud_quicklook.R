# wildcloud_quicklook.R
# Process movement data exported from WildCloud (Sigfox bat tags)
# Extracts step distance, speed, VeDBA; produces diagnostic plots
# Edward Hurme | MPI-AB

library(data.table)
library(tidyverse)
library(geosphere)  # distHaversine
library(ggforce)    # facet_wrap_paginate

# ─────────────────────────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────────────────────────

wildcloud_quicklook <- function(
    data,                        # data.frame/data.table or file path (.csv)
    datetime_tz  = "UTC",        # timezone of the Time (UTC) column
    start_time   = NULL,         # POSIXct or "YYYY-MM-DD" / "YYYY-MM-DD HH:MM:SS"
    end_time     = NULL,         # POSIXct or "YYYY-MM-DD" / "YYYY-MM-DD HH:MM:SS"
    min_quality  = NULL,         # "Poor","Average","Good","Excellent"
    max_radius   = NULL,         # max position radius (m)
    gap_hours    = 3,            # path segment break threshold (hours)
    # ── plot controls ──────────────────────────────────────────
    plot         = TRUE,         # master switch
    plot_daily   = TRUE,         # P1 daily distance + P2 daily VeDBA
    plot_raw     = TRUE,         # P3 speed / P5 VeDBA / P6 altitude / P4 timeline
    plot_temp    = TRUE,         # P7 temperature (subset of raw)
    free_y_raw   = TRUE,         # TRUE = each facet has its own y scale in raw plots
    # FALSE = shared y axis across all tag facets
    facets_per_page = 20,        # max facets per page; >20 devices splits into pages
    plot_dir     = NULL          # if set, save PNGs here
) {

  # ── 1. Load data ──────────────────────────────────────────
  if (is.character(data) && length(data) == 1 && file.exists(data)) {
    message("Reading: ", data)
    dt <- fread(data, encoding = "UTF-8")
  } else {
    dt <- as.data.table(data)
  }

  # ── 2. Standardise column names ───────────────────────────
  setnames(dt, trimws(gsub("[\u00a0\ufeff]", "", names(dt))))

  col_map <- list(
    device   = c("Device", "device", "tag", "Tag", "id", "ID"),
    datetime = c("Time (UTC)", "Time(UTC)", "datetime", "timestamp", "Timestamp"),
    position = c("Position", "position", "GPS", "Location"),
    radius   = c("Radius (m) (Source/Status)", "Radius (m)", "radius_m",
                 "position_radius_m"),
    lqi      = c("LQI", "lqi", "Link Quality", "link_quality"),
    seq_num  = c("Sequence Number", "sequence_number", "seq")
  )

  lower_names <- tolower(trimws(names(dt)))
  for (canonical in names(col_map)) {
    if (canonical %in% names(dt)) next
    candidates_lower <- tolower(trimws(col_map[[canonical]]))
    hit <- match(candidates_lower, lower_names)
    hit <- hit[!is.na(hit)][1]
    if (!is.na(hit)) {
      message(sprintf("  Renaming '%s' -> '%s'", names(dt)[hit], canonical))
      setnames(dt, names(dt)[hit], canonical)
      lower_names <- tolower(trimws(names(dt)))
    }
  }

  missing_mandatory <- setdiff(c("device", "datetime"), names(dt))
  if (length(missing_mandatory) > 0) {
    stop(sprintf(
      "Could not find mandatory column(s): %s\nAvailable columns: %s",
      paste(missing_mandatory, collapse = ", "),
      paste(names(dt), collapse = ", ")
    ))
  }

  # Detect optional sensor column groups
  vedba_cols    <- grep("(?i)vedba",               names(dt), value = TRUE, perl = TRUE)
  pressure_cols <- grep("(?i)pressure",            names(dt), value = TRUE, perl = TRUE)
  temp_min_col  <- grep("(?i)min.*temp|temp.*min", names(dt), value = TRUE, perl = TRUE)
  temp_max_col  <- grep("(?i)max.*temp|temp.*max", names(dt), value = TRUE, perl = TRUE)

  has_vedba    <- length(vedba_cols)    > 0
  has_pressure <- length(pressure_cols) > 0
  has_temp     <- length(temp_min_col)  > 0 | length(temp_max_col) > 0

  message(sprintf(
    "Columns detected -- VeDBA: %s | Pressure: %s | Temperature: %s",
    has_vedba, has_pressure, has_temp
  ))

  # ── 3. Parse datetime ─────────────────────────────────────
  dt[, datetime := parse_wildcloud_datetime(datetime, datetime_tz)]
  if (all(is.na(dt$datetime))) stop("Could not parse datetime column. Check format.")
  dt[, date := as.Date(datetime, tz = datetime_tz)]
  dt[, hour := lubridate::hour(datetime)]

  # ── 4. Timestamp filters ──────────────────────────────────
  parse_ts <- function(x, tz) {
    if (is.null(x)) return(NULL)
    if (inherits(x, "POSIXct")) return(x)
    out <- as.POSIXct(x, format = "%Y-%m-%d %H:%M:%S", tz = tz)
    if (is.na(out)) out <- as.POSIXct(x, format = "%Y-%m-%d", tz = tz)
    if (is.na(out)) stop("Cannot parse timestamp '", x,
                         "'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.")
    out
  }
  start_ts <- parse_ts(start_time, datetime_tz)
  end_ts   <- parse_ts(end_time,   datetime_tz)
  if (!is.null(start_ts)) {
    before <- nrow(dt); dt <- dt[datetime >= start_ts]
    message(sprintf("  start_time filter: %d -> %d records", before, nrow(dt)))
  }
  if (!is.null(end_ts)) {
    before <- nrow(dt); dt <- dt[datetime <= end_ts]
    message(sprintf("  end_time filter:   %d -> %d records", before, nrow(dt)))
  }

  # ── 5. Parse position ─────────────────────────────────────
  if ("position" %in% names(dt)) {
    dt[, latitude  := as.numeric(trimws(sub(",.*",  "", position)))]
    dt[, longitude := as.numeric(trimws(sub(".*,", "", position)))]
  }
  dt[, coord_ok := !is.na(latitude) & !is.na(longitude) &
       data.table::between(latitude, 35, 72) &
       data.table::between(longitude, -15, 40)]
  n_bad <- sum(!dt$coord_ok)
  if (n_bad > 0)
    message(sprintf("  %d records flagged with missing/implausible coordinates (coord_ok=FALSE)", n_bad))
  if ("radius" %in% names(dt))
    dt[, radius_m := as.numeric(sub("\\s*\\(.*", "", radius))]

  # ── 6. Quality filters ────────────────────────────────────
  if (!is.null(min_quality) && "lqi" %in% names(dt)) {
    quality_levels <- c("Poor", "Average", "Good", "Excellent")
    min_idx <- match(min_quality, quality_levels)
    if (is.na(min_idx)) warning("min_quality not recognised; ignoring filter")
    else {
      before <- nrow(dt)
      dt <- dt[lqi %in% quality_levels[min_idx:length(quality_levels)]]
      message(sprintf("  LQI filter: %d -> %d records", before, nrow(dt)))
    }
  }
  if (!is.null(max_radius) && "radius_m" %in% names(dt)) {
    before <- nrow(dt)
    dt <- dt[is.na(radius_m) | radius_m <= max_radius]
    message(sprintf("  Radius filter (<= %dm): %d -> %d records", max_radius, before, nrow(dt)))
  }

  # ── 7. Step distance & speed ──────────────────────────────
  setorder(dt, device, datetime)
  dt[, `:=`(
    lag_lat      = shift(latitude,  1, type = "lag"),
    lag_lon      = shift(longitude, 1, type = "lag"),
    lag_time     = shift(datetime,  1, type = "lag"),
    lag_coord_ok = shift(coord_ok,  1, type = "lag")
  ), by = device]
  dt[, step_dist_m := ifelse(
    coord_ok & !is.na(lag_coord_ok) & lag_coord_ok & !is.na(lag_lat),
    mapply(function(lo1, la1, lo2, la2) distHaversine(c(lo1, la1), c(lo2, la2)),
           lag_lon, lag_lat, longitude, latitude),
    NA_real_
  )]
  dt[, dt_hours := as.numeric(difftime(datetime, lag_time, units = "hours"))]
  dt[, speed_ms := ifelse(
    !is.na(step_dist_m) & !is.na(dt_hours) & dt_hours > 0,
    step_dist_m / (dt_hours * 3600),
    NA_real_
  )]

  # ── 8. VeDBA ─────────────────────────────────────────────
  if (has_vedba) {
    dt[, (vedba_cols) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.]", "", x))),
       .SDcols = vedba_cols]
    dt[, vedba_sum         := rowSums(.SD, na.rm = TRUE),         .SDcols = vedba_cols]
    dt[, vedba_any_nonzero := rowSums(.SD > 0, na.rm = TRUE) > 0, .SDcols = vedba_cols]
  }

  # ── 9. Pressure & temperature ────────────────────────────
  if (has_pressure)
    dt[, (pressure_cols) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.]", "", x))),
       .SDcols = pressure_cols]
  if (length(temp_min_col) > 0)
    dt[, (temp_min_col) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.-]", "", x))),
       .SDcols = temp_min_col]
  if (length(temp_max_col) > 0)
    dt[, (temp_max_col) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.-]", "", x))),
       .SDcols = temp_max_col]

  # ── 10. Per-device summary ────────────────────────────────
  agg_cols <- c("step_dist_m", "speed_ms")
  if (has_vedba) agg_cols <- c(agg_cols, "vedba_sum")
  summary_dt <- dt[coord_ok == TRUE, {
    out <- list(n_fixes = .N)
    for (col in agg_cols) {
      vals <- get(col)
      out[[paste0(col, "_n")]]    <- sum(!is.na(vals))
      out[[paste0(col, "_mean")]] <- mean(vals, na.rm = TRUE)
      out[[paste0(col, "_sd")]]   <- sd(vals,   na.rm = TRUE)
      out[[paste0(col, "_max")]]  <- suppressWarnings(max(vals, na.rm = TRUE))
    }
    out
  }, by = device]

  # ── 11. Plots ─────────────────────────────────────────────
  plots <- if (plot) make_diagnostic_plots(
    dt, has_vedba, has_pressure, has_temp,
    vedba_cols, pressure_cols, temp_min_col, temp_max_col,
    gap_hours       = gap_hours,
    plot_daily      = plot_daily,
    plot_raw        = plot_raw,
    plot_temp       = plot_temp,
    free_y_raw      = free_y_raw,
    facets_per_page = facets_per_page,
    plot_dir        = plot_dir
  ) else NULL

  # ── 12. Return ────────────────────────────────────────────
  message(sprintf(
    "\nDone. %d records | %d devices | date range: %s - %s",
    nrow(dt), dplyr::n_distinct(dt$device),
    min(dt$date, na.rm = TRUE), max(dt$date, na.rm = TRUE)
  ))

  invisible(list(
    data       = dt,
    summary    = summary_dt,
    vedba_cols = vedba_cols,
    plots      = plots
  ))
}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

parse_wildcloud_datetime <- function(x, tz = "UTC") {
  out <- as.POSIXct(x, format = "%d.%m.%Y, %H:%M:%S", tz = tz)
  if (mean(is.na(out)) > 0.5)
    out <- as.POSIXct(x, format = "%Y-%m-%d %H:%M:%S", tz = tz)
  if (mean(is.na(out)) > 0.5)
    out <- as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%SZ", tz = tz)
  out
}


make_diagnostic_plots <- function(
    dt, has_vedba, has_pressure, has_temp,
    vedba_cols, pressure_cols, temp_min_col, temp_max_col,
    gap_hours       = 3,
    plot_daily      = TRUE,
    plot_raw        = TRUE,
    plot_temp       = TRUE,
    free_y_raw      = TRUE,
    facets_per_page = 20,
    plot_dir        = NULL
) {

  library(suncalc)
  library(ggforce)

  y_scales <- if (free_y_raw) "free_y" else "fixed"
  n_devices <- dplyr::n_distinct(dt$device)
  n_pages   <- ceiling(n_devices / facets_per_page)
  ncol_fac  <- min(4L, facets_per_page)

  theme_bat <- theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      strip.text       = element_text(face = "bold", size = 9),
      plot.title       = element_text(face = "bold"),
      axis.text.x      = element_text(angle = 45, hjust = 1),
      legend.position  = "none"
    )

  x_datetime <- scale_x_datetime(date_labels = "%b %d", date_breaks = "1 week")

  # ── ISA pressure -> altitude ──────────────────────────────
  pressure_to_altitude_m <- function(p_mbar) {
    p0 <- 1013.25; L <- 0.0065; T0 <- 288.15; g <- 9.80665; R <- 287.05
    (T0 / L) * (1 - (p_mbar / p0) ^ (R * L / g))
  }

  # ── Segment ID ───────────────────────────────────────────
  add_segment_id <- function(d, time_col = "obs_time", gap_h = gap_hours) {
    setorderv(d, c("device", time_col))
    d[, lag_t   := shift(get(time_col), 1L), by = device]
    d[, elapsed := as.numeric(difftime(get(time_col), lag_t, units = "hours"))]
    d[, segment := cumsum(is.na(lag_t) | elapsed > gap_h), by = device]
    d[, c("lag_t", "elapsed") := NULL]
    d
  }

  # ── Sunrise / sunset rectangles ───────────────────────────
  make_sun_rects <- function(dt_full, t_min, t_max, tz = "UTC") {
    device_locs <- dt_full[coord_ok == TRUE,
                           .(lat = median(latitude,  na.rm = TRUE),
                             lon = median(longitude, na.rm = TRUE)),
                           by = device]
    if (nrow(device_locs) == 0) return(NULL)
    avg_lat <- mean(device_locs$lat, na.rm = TRUE)
    avg_lon <- mean(device_locs$lon, na.rm = TRUE)
    dates <- seq(as.Date(t_min, tz = tz) - 1,
                 as.Date(t_max, tz = tz) + 1, by = "day")
    sun <- as.data.table(getSunlightTimes(
      date = dates, lat = avg_lat, lon = avg_lon,
      keep = c("sunset", "sunrise"), tz = tz
    ))
    setorder(sun, date)
    sun[, sunrise_next := shift(sunrise, 1L, type = "lead")]
    rects <- sun[!is.na(sunrise_next), .(xmin = sunset, xmax = sunrise_next)]
    rects[, xmin := pmax(xmin, t_min)]
    rects[, xmax := pmin(xmax, t_max)]
    as.data.frame(rects[xmax > xmin])
  }

  t_min <- min(dt$datetime, na.rm = TRUE)
  t_max <- max(dt$datetime, na.rm = TRUE)
  night_rects <- make_sun_rects(dt, t_min, t_max)

  night_layer <- function(rects) {
    if (is.null(rects) || nrow(rects) == 0) return(NULL)
    geom_rect(data = rects,
              aes(xmin = xmin, xmax = xmax, ymin = -Inf, ymax = Inf),
              inherit.aes = FALSE, fill = "grey20", alpha = 0.15)
  }

  # ── Paginated facet helper ────────────────────────────────
  # Builds a named list of ggplots, one per page.
  # base_plot: a ggplot object that already has all layers EXCEPT facet_wrap.
  # data:      the data.frame used to build the plot (for n_pages calculation).
  # label:     used to name list entries, e.g. "raw_vedba_p1", "raw_vedba_p2".
  # extra_theme: optional theme additions (e.g. legend.position = "right").
  make_pages <- function(base_plot, label, extra_theme = NULL) {
    page_list <- vector("list", n_pages)
    for (pg in seq_len(n_pages)) {
      p <- base_plot +
        facet_wrap_paginate(
          ~device,
          scales   = y_scales,
          ncol     = ncol_fac,
          nrow     = ceiling(facets_per_page / ncol_fac),
          page     = pg
        )
      if (!is.null(extra_theme)) p <- p + extra_theme
      page_list[[pg]] <- p
    }
    names(page_list) <- if (n_pages == 1)
      label
    else
      paste0(label, "_p", seq_len(n_pages))
    page_list
  }

  plots <- list()

  # ═══════════════════════════════════════════════════════════
  # DAILY PLOTS — one line per device, no faceting needed
  # ═══════════════════════════════════════════════════════════
  if (plot_daily) {

    # ── P1: Daily distance ──────────────────────────────────
    dist_daily <- dt[coord_ok == TRUE, .(
      total_dist_km = sum(step_dist_m, na.rm = TRUE) / 1000
    ), by = .(device, date)]
    dist_daily[, obs_time := as.POSIXct(date, tz = "UTC")]

    plots$daily_distance <-
      ggplot(dist_daily, aes(obs_time, total_dist_km, col = device)) +
      night_layer(night_rects) +
      geom_path(alpha = 0.5) +
      geom_point(size = 1.5) +
      x_datetime +
      labs(title = "Daily distance travelled per tag",
           x = NULL, y = "Distance (km)") +
      theme_bat

    # ── P2: Daily mean VeDBA ────────────────────────────────
    if (has_vedba && "vedba_sum" %in% names(dt)) {
      vedba_daily <- dt[!is.na(vedba_sum), .(
        mean_vedba = mean(vedba_sum, na.rm = TRUE)
      ), by = .(device, date)]
      vedba_daily[, obs_time := as.POSIXct(date, tz = "UTC")]

      plots$daily_vedba <-
        ggplot(vedba_daily, aes(obs_time, mean_vedba, col = device)) +
        night_layer(night_rects) +
        geom_path(alpha = 0.5) +
        geom_point(size = 1.5) +
        x_datetime +
        labs(title = "Mean daily VeDBA activity per tag",
             x = NULL, y = "Mean VeDBA sum (m/s\u00b2)") +
        theme_bat
    }

  } # end plot_daily

  # ═══════════════════════════════════════════════════════════
  # RAW PLOTS — faceted per device, paginated if > facets_per_page
  # ═══════════════════════════════════════════════════════════
  if (plot_raw) {

    # ── P3: Speed over time ─────────────────────────────────
    spd <- dt[!is.na(speed_ms) & speed_ms < 30]
    spd[, obs_time := datetime]
    spd <- add_segment_id(spd, time_col = "obs_time")

    p3_base <-
      ggplot(spd, aes(obs_time, speed_ms, col = device,
                      group = interaction(device, segment))) +
      night_layer(night_rects) +
      geom_path(alpha = 0.35, linewidth = 0.5) +
      geom_point(size = 0.5, alpha = 0.7) +
      x_datetime +
      labs(title = "Step speed per tag over time",
           y = "Speed (m/s)", x = NULL) +
      theme_bat

    plots <- c(plots, make_pages(p3_base, "speed_over_time"))

    # ── P4: Transmission timeline — device on y, no faceting ─
    plots$transmission_timeline <-
      ggplot(dt, aes(datetime, device, colour = coord_ok)) +
      night_layer(night_rects) +
      geom_point(size = 0.8, alpha = 0.6) +
      scale_colour_manual(
        values = c("TRUE" = "steelblue", "FALSE" = "tomato"),
        labels = c("TRUE" = "Valid", "FALSE" = "Flagged"),
        name   = "Position"
      ) +
      x_datetime +
      labs(title = "Transmission timeline per tag", x = NULL, y = NULL) +
      theme_bat +
      theme(legend.position = "right")

    # ── P5: Raw VeDBA — time-corrected, segmented ───────────
    if (has_vedba && length(vedba_cols) > 0) {
      vedba_long <- melt(
        dt[, c("device", "datetime", vedba_cols), with = FALSE],
        id.vars       = c("device", "datetime"),
        variable.name = "window", value.name = "vedba"
      )[!is.na(vedba)]
      vedba_long[, offset_min := as.numeric(gsub(".*?(\\d+)\\s*min.*", "\\1", window))]
      vedba_long[, obs_time   := datetime - offset_min * 60]
      setorder(vedba_long, device, obs_time, offset_min)
      vedba_long <- vedba_long[, .SD[1L], by = .(device, obs_time)]
      vedba_long <- add_segment_id(vedba_long)

      p5_base <-
        ggplot(vedba_long, aes(obs_time, vedba, col = device,
                               group = interaction(device, segment))) +
        night_layer(night_rects) +
        geom_path(alpha = 0.35, linewidth = 0.5) +
        geom_point(size = 0.5, alpha = 0.7) +
        x_datetime +
        labs(title = sprintf("Raw VeDBA per tag — time-corrected  [y: %s]",
                             ifelse(free_y_raw, "free per tag", "shared")),
             x = NULL, y = "VeDBA (m/s\u00b2)") +
        theme_bat

      plots <- c(plots, make_pages(p5_base, "raw_vedba"))
    }

    # ── P6: Altitude from pressure — time-corrected, segmented
    if (has_pressure && length(pressure_cols) > 0) {
      pres_long <- melt(
        dt[, c("device", "datetime", pressure_cols), with = FALSE],
        id.vars       = c("device", "datetime"),
        variable.name = "window", value.name = "pressure_mbar"
      )[!is.na(pressure_mbar)]
      pres_long[, offset_min := as.numeric(gsub(".*?(\\d+)\\s*min.*", "\\1", window))]
      pres_long[, obs_time   := datetime - offset_min * 60]
      setorder(pres_long, device, obs_time, offset_min)
      pres_long <- pres_long[, .SD[1L], by = .(device, obs_time)]
      pres_long[, altitude_m := pressure_to_altitude_m(pressure_mbar)]
      pres_long <- add_segment_id(pres_long)

      p6_base <-
        ggplot(pres_long, aes(obs_time, altitude_m, col = device,
                              group = interaction(device, segment))) +
        night_layer(night_rects) +
        geom_path(alpha = 0.35, linewidth = 0.5) +
        geom_point(size = 0.5, alpha = 0.7) +
        x_datetime +
        labs(title = sprintf("Altitude ASL per tag (ISA)  [y: %s]",
                             ifelse(free_y_raw, "free per tag", "shared")),
             x = NULL, y = "Altitude (m ASL)") +
        theme_bat

      plots <- c(plots, make_pages(p6_base, "raw_altitude"))
    }

    # ── P7: Temperature ──────────────────────────────────────
    if (plot_temp && has_temp &&
        (length(temp_min_col) > 0 || length(temp_max_col) > 0)) {

      temp_parts <- list()
      if (length(temp_min_col) > 0) {
        tmp <- dt[, c("device", "datetime", temp_min_col[1]), with = FALSE]
        setnames(tmp, temp_min_col[1], "temp_c")
        tmp[, metric := "Min"]
        temp_parts[[1]] <- tmp
      }
      if (length(temp_max_col) > 0) {
        tmp <- dt[, c("device", "datetime", temp_max_col[1]), with = FALSE]
        setnames(tmp, temp_max_col[1], "temp_c")
        tmp[, metric := "Max"]
        temp_parts[[2]] <- tmp
      }
      temp_long <- rbindlist(temp_parts)[!is.na(temp_c)]
      temp_long[, obs_time := datetime]
      temp_long <- add_segment_id(temp_long)

      p7_base <-
        ggplot(temp_long, aes(obs_time, temp_c, col = metric,
                              group = interaction(device, metric, segment))) +
        night_layer(night_rects) +
        geom_path(alpha = 0.35, linewidth = 0.5) +
        geom_point(size = 0.5, alpha = 0.7) +
        x_datetime +
        scale_colour_manual(values = c("Min" = "steelblue", "Max" = "#e07b39"),
                            name = NULL) +
        labs(title = sprintf("Min & max temperature per tag  [y: %s]",
                             ifelse(free_y_raw, "free per tag", "shared")),
             x = NULL, y = "Temperature (\u00b0C)") +
        theme_bat

      plots <- c(plots, make_pages(p7_base, "raw_temperature",
                                   extra_theme = theme(legend.position = "right")))
    }

  } # end plot_raw

  # ── P8: Map with country outlines ────────────────────────
  valid_fixes <- dt[coord_ok == TRUE]
  if (nrow(valid_fixes) > 0) {
    pad  <- 1.5
    xlim <- c(min(valid_fixes$longitude, na.rm = TRUE) - pad,
              max(valid_fixes$longitude, na.rm = TRUE) + pad)
    ylim <- c(min(valid_fixes$latitude,  na.rm = TRUE) - pad,
              max(valid_fixes$latitude,  na.rm = TRUE) + pad)
    world <- map_data("world")

    plots$map <-
      ggplot() +
      geom_polygon(data = world,
                   aes(x = long, y = lat, group = group),
                   fill = "grey90", colour = "white", linewidth = 0.2) +
      geom_path(data = valid_fixes,
                aes(x = longitude, y = latitude, col = device, group = device),
                alpha = 0.4, linewidth = 0.4) +
      geom_point(data = valid_fixes,
                 aes(x = longitude, y = latitude, col = device),
                 size = 0.8, alpha = 0.7) +
      coord_quickmap(xlim = xlim, ylim = ylim, expand = FALSE) +
      labs(title = "Tag locations", x = NULL, y = NULL, colour = "Device") +
      theme_bat +
      theme(
        legend.position  = "right",
        axis.text        = element_text(size = 7),
        panel.border     = element_rect(colour = "grey60", fill = NA, linewidth = 0.4),
        panel.background = element_rect(fill = "aliceblue")
      )
  }

  # ── Save if requested ─────────────────────────────────────
  if (!is.null(plot_dir)) {
    dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)
    for (nm in names(plots)) {
      w <- if (nm == "map") 8 else 10
      h <- if (nm == "map") 6 else 5
      ggplot2::ggsave(file.path(plot_dir, paste0(nm, ".png")),
                      plots[[nm]], width = w, height = h, dpi = 150)
      message("Saved: ", nm, ".png")
    }
  }

  plots
}

# ─────────────────────────────────────────────────────────────
# EXAMPLE
# ─────────────────────────────────────────────────────────────
run <- TRUE
if (run) {
  wildcloud_quicklook(
    "../../../Downloads/Swiss_nanofoxFSP_04_05_2026_records.csv",
    start_time      = "2026-04-09",
    plot_daily      = TRUE,
    plot_raw        = TRUE,
    plot_temp       = TRUE,
    free_y_raw      = FALSE,
    facets_per_page = 20,
    plot_dir        = "../../../Dropbox/MPI/Noctule/Plots/Spring26/Swiss/Quicklook/"
  )
}
