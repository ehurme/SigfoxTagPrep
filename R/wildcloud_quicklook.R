# process_wildcloud.R
# Process movement data exported from WildCloud (Sigfox bat tags)
# Extracts step distance, speed, VeDBA; produces diagnostic plots
# Edward Hurme | MPI-AB

library(data.table)
library(tidyverse)
library(geosphere)  # distHaversine

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
    plot         = TRUE,
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
  # Strip BOM / non-breaking spaces (common in WildCloud CSV headers)
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
  vedba_cols    <- grep("(?i)vedba",    names(dt), value = TRUE, perl = TRUE)
  pressure_cols <- grep("(?i)pressure", names(dt), value = TRUE, perl = TRUE)
  temp_cols     <- grep("(?i)temp",     names(dt), value = TRUE, perl = TRUE)

  has_vedba    <- length(vedba_cols)    > 0
  has_pressure <- length(pressure_cols) > 0
  has_temp     <- length(temp_cols)     > 0

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

  # Flag implausible coordinates (European bat range)
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

  # Haversine distance; NA when either fix is invalid
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
    dt[, vedba_sum          := rowSums(.SD, na.rm = TRUE),        .SDcols = vedba_cols]
    dt[, vedba_any_nonzero  := rowSums(.SD > 0, na.rm = TRUE) > 0, .SDcols = vedba_cols]
  }

  # ── 9. Pressure & temperature ────────────────────────────
  if (has_pressure)
    dt[, (pressure_cols) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.]",  "", x))),
       .SDcols = pressure_cols]
  if (has_temp)
    dt[, (temp_cols) := lapply(.SD, function(x) as.numeric(gsub("[^0-9.-]", "", x))),
       .SDcols = temp_cols]

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
  plots <- if (plot) make_diagnostic_plots(dt, has_vedba, plot_dir) else NULL

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
  out <- as.POSIXct(x, format = "%d.%m.%Y, %H:%M:%S", tz = tz)   # WildCloud native
  if (mean(is.na(out)) > 0.5)
    out <- as.POSIXct(x, format = "%Y-%m-%d %H:%M:%S", tz = tz)
  if (mean(is.na(out)) > 0.5)
    out <- as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%SZ", tz = tz)
  out
}


make_diagnostic_plots <- function(dt, has_vedba, plot_dir = NULL) {

  theme_bat <- theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      strip.text       = element_text(face = "bold", size = 9),
      plot.title       = element_text(face = "bold"),
      axis.text.x      = element_text(angle = 45, hjust = 1),
      legend.position  = "none"
    )

  plots <- list()

  # ── P1: Total distance travelled per tag per date ─────────
  dist_daily <- dt[coord_ok == TRUE, .(
    total_dist_km = sum(step_dist_m, na.rm = TRUE) / 1000
  ), by = .(device, date)]

  plots$daily_distance <-
    ggplot(dist_daily, aes(date, total_dist_km, col = device)) +
    geom_path() +
    #facet_wrap(~device, scales = "free_y", ncol = 4) +
    scale_x_date(date_labels = "%b %d", date_breaks = "1 week") +
    labs(title = "Daily distance travelled per tag",
         x = NULL, y = "Distance (km)") +
    theme_bat

  # ── P2: Mean VeDBA activity per tag per date ──────────────
  if (has_vedba && "vedba_sum" %in% names(dt)) {
    vedba_daily <- dt[!is.na(vedba_sum), .(
      mean_vedba = mean(vedba_sum, na.rm = TRUE)
    ), by = .(device, date)]

    plots$daily_vedba <-
      ggplot(vedba_daily, aes(date, mean_vedba, col = device)) +
      geom_path() +
      # facet_wrap(~device, scales = "free_y", ncol = 4) +
      scale_x_date(date_labels = "%b %d", date_breaks = "1 week") +
      labs(title = "Mean daily VeDBA activity per tag",
           x = NULL, y = "Mean VeDBA sum (m/s\u00b2)") +
      theme_bat
  }

  # ── P3: Speed distribution per tag ────────────────────────
  plots$speed_dist <- ggplot(
    dt[!is.na(speed_ms) & speed_ms < 30],
    aes(date, speed_ms, col = device)
  ) +
    geom_path() +
    # facet_wrap(~device, scales = "free_y", ncol = 4) +
    labs(title = "Step speed distribution per tag (m/s)",
         y = "Speed (m/s)", x = "Date") +
    theme_bat

  # ── P4: Transmission timeline ─────────────────────────────
  plots$transmission_timeline <- ggplot(dt, aes(datetime, device, colour = coord_ok)) +
    geom_point(size = 0.8, alpha = 0.6) +
    scale_colour_manual(
      values = c("TRUE" = "steelblue", "FALSE" = "tomato"),
      labels = c("TRUE" = "Valid", "FALSE" = "Flagged"),
      name   = "Position"
    ) +
    labs(title = "Transmission timeline per tag", x = NULL, y = NULL) +
    theme_bat +
    theme(legend.position = "right")

  # ── Save if requested ─────────────────────────────────────
  if (!is.null(plot_dir)) {
    dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)
    for (nm in names(plots)) {
      ggplot2::ggsave(file.path(plot_dir, paste0(nm, ".png")),
                      plots[[nm]], width = 10, height = 5, dpi = 150)
      message("Saved: ", nm, ".png")
    }
  }

  plots
}

wildcloud_quicklook("../../../Downloads/24_04_2026_records (1).csv",
                  start_time = "2026-04-01",
                  plot_dir = "../../../Dropbox/MPI/Noctule/Plots/WildCloud")
