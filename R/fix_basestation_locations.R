# =============================================================================
# fix_basestation_locations.R
#
# Corrects GPS locations in WildCloud output CSVs where a faulty base station
# was the sole receiver for a fix. Replaces the combined "lat, lon" Position
# field with the known correct coordinates for that station, within a
# specified time window.
#
# Expected WildCloud CSV format (semicolon-delimited):
#   Device ; Time (UTC) ; ... ; Position ; ... ; Base Stations (ID, RSSI, Reps) ; ...
#   Position field: "50.80774, 12.285544"  (single "lat, lon" string)
#   Time (UTC) field: "05.05.2026, 16:31:43"
#
# Depends on: parse_basestations.R
#
# Main entry points:
#   fix_basestation_locations(csv_path, corrections, ...)
#   plot_basestation_correction(before, after, ...)   — standalone plot
# =============================================================================

source("./R/parse_basestations.R")   # adjust path as needed


# -----------------------------------------------------------------------------
# .parse_correction_times(corrections)
#
# Internal: coerce the start/end columns to POSIXct (UTC).
# Accepts mixed formats: ISO 8601, "d.m.yyyy H:MM", or NA / "?" for open ends.
# Returns the corrections data frame with start_dt and end_dt columns added.
# -----------------------------------------------------------------------------
.parse_correction_times <- function(corrections) {
  .try_parse <- function(x) {
    if (is.na(x) || trimws(x) %in% c("", "?")) return(as.POSIXct(NA))

    fmts <- c(
      "%Y-%m-%d %H:%M",   # 2026-04-22 14:00
      "%Y-%m-%d",         # 2026-04-22
      "%d.%m.%Y %H:%M",   # 4.08.2025 9:00
      "%d.%m.%Y"          # 15.08.2025
    )
    for (fmt in fmts) {
      dt <- suppressWarnings(as.POSIXct(trimws(x), format = fmt, tz = "UTC"))
      if (!is.na(dt)) return(dt)
    }
    warning("Could not parse datetime: '", x, "' - treating as open (NA).")
    as.POSIXct(NA)
  }

  corrections$start_dt <- vapply(as.character(corrections$start), .try_parse,
                                 FUN.VALUE = as.POSIXct(NA))
  corrections$end_dt   <- vapply(as.character(corrections$end),   .try_parse,
                                 FUN.VALUE = as.POSIXct(NA))
  corrections
}


# -----------------------------------------------------------------------------
# .parse_position_col(pos_vec)
#
# Internal: split a "lat, lon" character vector into a two-column data frame.
# -----------------------------------------------------------------------------
.parse_position_col <- function(pos_vec) {
  parts <- strsplit(as.character(pos_vec), ",")
  lat   <- suppressWarnings(as.numeric(vapply(parts,
                                              function(p) if (length(p) >= 1) trimws(p[1]) else NA_character_,
                                              character(1))))
  lon   <- suppressWarnings(as.numeric(vapply(parts,
                                              function(p) if (length(p) >= 2) trimws(p[2]) else NA_character_,
                                              character(1))))
  data.frame(lat = lat, lon = lon)
}


# -----------------------------------------------------------------------------
# plot_basestation_correction(before, after, title, station_ids)
#
# Side-by-side before/after map of all fixes, with corrected points highlighted
# and arrows showing the displacement. Uses only base R graphics (no ggplot2).
#
# Args:
#   before       data frame: lon, lat, changed (logical) — one row per fix
#   after        data frame: lon, lat           — same row order as before
#   title        overall plot title
#   station_ids  optional character vector (length = nrow(before)) of station
#                IDs matched to each row; used to colour-code arrows when
#                multiple faulty stations are corrected in one pass
# -----------------------------------------------------------------------------
plot_basestation_correction <- function(
    before,
    after,
    title       = "Basestation location correction",
    station_ids = NULL
) {
  stopifnot(
    all(c("lon", "lat", "changed") %in% names(before)),
    all(c("lon", "lat")            %in% names(after)),
    nrow(before) == nrow(after)
  )

  changed <- before$changed

  # Colour palette (up to 8 distinct faulty stations)
  pal <- c("#E41A1C", "#377EB8", "#4DAF4A", "#984EA3",
           "#FF7F00", "#A65628", "#F781BF", "#999999")

  if (!is.null(station_ids) && length(unique(station_ids[changed])) > 1) {
    sta_levels  <- unique(station_ids[changed])
    sta_col_map <- setNames(pal[seq_along(sta_levels)], sta_levels)
    arrow_cols  <- sta_col_map[station_ids[changed]]
    show_sta_legend <- TRUE
  } else {
    arrow_cols      <- rep(pal[1], sum(changed))
    show_sta_legend <- FALSE
  }

  # Shared axis limits across both panels + 5% padding
  all_lon <- c(before$lon, after$lon)
  all_lat <- c(before$lat, after$lat)
  pad_lon <- diff(range(all_lon, na.rm = TRUE)) * 0.05
  pad_lat <- diff(range(all_lat, na.rm = TRUE)) * 0.05
  xlim    <- range(all_lon, na.rm = TRUE) + c(-pad_lon, pad_lon)
  ylim    <- range(all_lat, na.rm = TRUE) + c(-pad_lat, pad_lat)
  if (diff(xlim) == 0) xlim <- xlim + c(-0.01, 0.01)
  if (diff(ylim) == 0) ylim <- ylim + c(-0.01, 0.01)

  # Approximate equal-area aspect ratio
  asp <- 1 / cos(mean(ylim, na.rm = TRUE) * pi / 180)

  opar <- par(no.readonly = TRUE)
  on.exit(par(opar))
  par(mfrow = c(1, 2), mar = c(4, 4, 3, 1), oma = c(0, 0, 2.5, 0))

  # ---- Panel 1: Before -------------------------------------------------------
  plot(
    before$lon, before$lat,
    pch  = 16, cex = 0.7,
    col  = ifelse(changed, "#E41A1C", adjustcolor("steelblue", 0.4)),
    xlim = xlim, ylim = ylim, asp = asp,
    xlab = "Longitude", ylab = "Latitude",
    main = sprintf("Before  (n corrected = %d)", sum(changed))
  )
  if (any(changed)) {
    points(before$lon[changed], before$lat[changed],
           pch = 1, cex = 1.6, col = "#E41A1C", lwd = 1.5)
  }

  # ---- Panel 2: After --------------------------------------------------------
  plot(
    after$lon, after$lat,
    pch  = 16, cex = 0.7,
    col  = ifelse(changed, "#4DAF4A", adjustcolor("steelblue", 0.4)),
    xlim = xlim, ylim = ylim, asp = asp,
    xlab = "Longitude", ylab = "Latitude",
    main = "After"
  )
  if (any(changed)) {
    arrows(
      x0     = before$lon[changed],
      y0     = before$lat[changed],
      x1     = after$lon[changed],
      y1     = after$lat[changed],
      length = 0.08, angle = 20, lwd = 1.5,
      col    = arrow_cols
    )
    points(after$lon[changed], after$lat[changed],
           pch = 17, cex = 1.2, col = "#4DAF4A")
  }

  # ---- Legend ----------------------------------------------------------------
  leg_items <- c("Unchanged", "Faulty (before)", "Corrected (after)")
  leg_cols  <- c(adjustcolor("steelblue", 0.4), "#E41A1C", "#4DAF4A")
  leg_pch   <- c(16, 16, 17)
  leg_lty   <- rep(NA, 3)
  leg_lwd   <- rep(NA, 3)

  if (show_sta_legend) {
    leg_items <- c(leg_items, paste("Station", sta_levels))
    leg_cols  <- c(leg_cols, sta_col_map)
    leg_pch   <- c(leg_pch, rep(NA, length(sta_levels)))
    leg_lty   <- c(leg_lty, rep(1,  length(sta_levels)))
    leg_lwd   <- c(leg_lwd, rep(2,  length(sta_levels)))
  }

  legend("bottomleft", legend = leg_items,
         col = leg_cols, pch = leg_pch, lty = leg_lty, lwd = leg_lwd,
         bty = "n", cex = 0.75, pt.cex = 1.1)

  mtext(title, outer = TRUE, cex = 1.1, font = 2)
  invisible(NULL)
}


# -----------------------------------------------------------------------------
# fix_basestation_locations(csv_path, corrections, ...)
#
# Loads a WildCloud CSV, identifies fixes where a faulty base station was the
# *sole* receiver, and replaces the Position field with the correct coordinates.
#
# The Position column ("lat, lon" string) is split into working lat/lon columns
# for matching and correction, then reconstructed before saving. The original
# string is preserved verbatim for unaffected rows.
#
# Args:
#   csv_path       Path to the WildCloud output CSV (semicolon-delimited).
#
#   corrections    Data frame with one row per faulty base station period:
#                    - station_id  : character hex ID (e.g. "1228B")
#                    - correct_lat : numeric
#                    - correct_lon : numeric
#                    - start       : character/POSIXct, lower time bound (NA/"?" = open)
#                    - end         : character/POSIXct, upper time bound (NA/"?" = open)
#
#   bs_col         Basestation string column (default: "Base Stations (ID, RSSI, Reps)")
#   time_col       Timestamp column          (default: "Time (UTC)")
#   pos_col        Combined position column  (default: "Position")
#   sep            CSV field separator       (default: ";")
#   out_path       Output path. Defaults to <stem>_corrected.csv
#   time_tz        Timezone for timestamps   (default: "UTC")
#   plot           Show before/after map     (default: TRUE)
#   verbose        Print change summary      (default: TRUE)
#
# Returns (invisibly) the corrected data frame.
# -----------------------------------------------------------------------------
fix_basestation_locations <- function(
    csv_path,
    corrections,
    bs_col   = "Base Stations (ID, RSSI, Reps)",
    time_col = "Time (UTC)",
    pos_col  = "Position",
    sep      = ";",
    out_path = NULL,
    time_tz  = "UTC",
    plot     = TRUE,
    verbose  = TRUE
) {
  # ---- 0. Validate inputs ----------------------------------------------------
  stopifnot(
    file.exists(csv_path),
    is.data.frame(corrections),
    all(c("station_id", "correct_lat", "correct_lon", "start", "end") %in% names(corrections))
  )

  corrections$station_id <- toupper(trimws(as.character(corrections$station_id)))
  corrections <- .parse_correction_times(corrections)

  # ---- 1. Load CSV -----------------------------------------------------------
  df <- read.csv(csv_path, sep = sep, stringsAsFactors = FALSE, check.names = FALSE,
                 quote = "\"", fill = TRUE, encoding = "UTF-8")

  required_cols <- c(bs_col, time_col, pos_col)
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop("Column(s) not found in CSV: ", paste(missing, collapse = ", "))
  }

  # ---- 2. Parse timestamps ---------------------------------------------------
  # WildCloud format: "05.05.2026, 16:31:43"
  df$..time <- as.POSIXct(trimws(df[[time_col]]), format = "%d.%m.%Y, %H:%M:%S", tz = time_tz)
  if (all(is.na(df$..time))) {
    df$..time <- as.POSIXct(trimws(df[[time_col]]), format = "%d.%m.%Y %H:%M:%S", tz = time_tz)
  }
  if (sum(!is.na(df$..time)) == 0) {
    warning("No timestamps could be parsed from '", time_col, "'. Time window filtering disabled.")
  }

  # ---- 3. Split Position into working lat/lon --------------------------------
  pos      <- .parse_position_col(df[[pos_col]])
  df$..lat <- pos$lat
  df$..lon <- pos$lon
  orig_lat <- df$..lat
  orig_lon <- df$..lon

  # ---- 4. Parse basestation strings ------------------------------------------
  bs_parsed       <- parse_basestations(df[[bs_col]])
  df$..n_stations <- bs_parsed$n_stations
  df$..best_id    <- toupper(bs_parsed$best_id)

  # ---- 5. Track changes ------------------------------------------------------
  df$..changed       <- FALSE
  df$..change_reason <- NA_character_

  # ---- 6. Apply each correction rule -----------------------------------------
  for (i in seq_len(nrow(corrections))) {
    rule <- corrections[i, ]

    sole_receiver <- !is.na(df$..best_id) &
      df$..best_id == rule$station_id &
      df$..n_stations == 1L

    in_window <- rep(TRUE, nrow(df))
    if (!is.na(rule$start_dt))
      in_window <- in_window & !is.na(df$..time) & df$..time >= rule$start_dt
    if (!is.na(rule$end_dt))
      in_window <- in_window & !is.na(df$..time) & df$..time <= rule$end_dt

    rows_to_fix <- which(sole_receiver & in_window)
    if (length(rows_to_fix) == 0) next

    df$..lat[rows_to_fix] <- rule$correct_lat
    df$..lon[rows_to_fix] <- rule$correct_lon
    df$..changed[rows_to_fix]  <- TRUE
    df$..change_reason[rows_to_fix] <- sprintf(
      "sole_receiver:%s corrected to (%.6f, %.6f)",
      rule$station_id, rule$correct_lat, rule$correct_lon
    )
  }

  # ---- 7. Reconstruct Position column ----------------------------------------
  changed_rows <- which(df$..changed)
  if (length(changed_rows) > 0) {
    df[[pos_col]][changed_rows] <- sprintf(
      "%.6f, %.6f", df$..lat[changed_rows], df$..lon[changed_rows]
    )
  }

  # ---- 8. Report -------------------------------------------------------------
  if (verbose) {
    n_changed <- sum(df$..changed)
    cat(sprintf(
      "fix_basestation_locations: %d of %d rows corrected (%.1f%%)\n",
      n_changed, nrow(df), 100 * n_changed / nrow(df)
    ))
    if (n_changed > 0) {
      cat("\nBy correction rule:\n")
      tbl <- table(df$..change_reason[df$..changed])
      for (nm in names(tbl)) cat(sprintf("  %d rows - %s\n", tbl[[nm]], nm))
      cat(sprintf(
        "\nOriginal lat range : [%.5f, %.5f]\n",
        range(orig_lat, na.rm = TRUE)[1], range(orig_lat, na.rm = TRUE)[2]
      ))
      cat(sprintf(
        "Corrected lat range: [%.5f, %.5f]\n",
        range(df$..lat, na.rm = TRUE)[1], range(df$..lat, na.rm = TRUE)[2]
      ))
    }
  }

  # ---- 8b. Before/after plot -------------------------------------------------
  if (plot && any(df$..changed)) {
    before_df <- data.frame(lon = orig_lon, lat = orig_lat, changed = df$..changed)
    after_df  <- data.frame(lon = df$..lon, lat = df$..lat)
    plot_basestation_correction(
      before      = before_df,
      after       = after_df,
      title       = sprintf("Basestation correction — %s", basename(csv_path)),
      station_ids = df$..best_id
    )
  }

  # ---- 9. Drop working columns -----------------------------------------------
  df$..time          <- NULL
  df$..lat           <- NULL
  df$..lon           <- NULL
  df$..n_stations    <- NULL
  df$..best_id       <- NULL
  df$..changed       <- NULL
  df$..change_reason <- NULL

  # ---- 10. Save --------------------------------------------------------------
  if (is.null(out_path)) {
    stem     <- tools::file_path_sans_ext(csv_path)
    ext      <- tools::file_ext(csv_path)
    out_path <- paste0(stem, "_corrected.", ext)
  }
  write.table(df, out_path, sep = sep, row.names = FALSE, na = "", quote = TRUE)

  if (verbose) cat(sprintf("\nSaved: %s\n", out_path))

  invisible(df)
}

# =============================================================================
# Example usage
# =============================================================================
if (FALSE) {

  # Define correction rules matching your table
  corrections <- data.frame(
    station_id  = c("1228B"),
    correct_lat = c(47.57225),
    correct_lon = c(8.894595),
    start       = c("2026-04-01 12:00"),
    end         = c("2026-04-22 14:00"),
    stringsAsFactors = FALSE
  )

  result <- fix_basestation_locations(
    csv_path    = "//10.0.16.7/grpdechmann/Bat projects/Noctule captures/Movebank/Switzerland/wildcloud/Swiss2026_NanofoxFSP_05_05_2026_records.csv",
    corrections = corrections
    # plot     = TRUE   # default: shows before/after map
    # bs_col   = "Base Stations (ID, RSSI, Reps)"  # default
    # time_col = "Time (UTC)"                       # default
    # pos_col  = "Position"                         # default
    # sep      = ";"                                # default
  )
}

