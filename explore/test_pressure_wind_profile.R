# test_pressure_wind_profile.R
# ─────────────────────────────────────────────────────────────────────────────
# Smoke-test for pressure_wind_profile.R using synthetic data.
# No real data or network access required.
#
# Tests:
#   1. Config built directly (bypasses interactive wizard)
#   2. find_complete_periods() — nightly mode
#   3. find_complete_periods() — daily mode
#   4. pressure_wind_profile() — loop over all individuals × all periods
#
# Two synthetic individuals:
#   TAG_A  — 3 migration nights, higher altitude (850 hPa), faster
#   TAG_B  — 2 migration nights, lower altitude  (925 hPa), slower
#
# Run from the project root:
#   source("explore/test_pressure_wind_profile.R")
# ─────────────────────────────────────────────────────────────────────────────

library(sf)
library(dplyr)

source("R/pressure_wind_profile.R")

# ── 1. Synthetic data ─────────────────────────────────────────────────────────

set.seed(42)

pressure_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)

# Helper: add ERA5-style wind columns (random but plausible)
.add_wind_cols <- function(df, pressure_levels) {
  n <- nrow(df)
  for (lv in pressure_levels) {
    # Wind support: positive = tailwind; stronger at higher altitude (lower hPa)
    ws_mean <- (1000 - lv) / 100          # 0 at 1000 hPa, 5 at 500 hPa
    df[[paste0("wind_support_", lv)]] <- round(rnorm(n, ws_mean, 2), 2)
    df[[paste0("wind_speed_",   lv)]] <- round(abs(rnorm(n, ws_mean + 3, 1.5)), 2)
  }
  # Best-wind columns (matching annotate_era5 output)
  ws_mat <- df[, paste0("wind_support_", pressure_levels), drop = FALSE]
  best_idx <- apply(ws_mat, 1, which.max)
  df$best_wind_level   <- pressure_levels[best_idx]
  df$best_wind_support <- apply(ws_mat, 1, max, na.rm = TRUE)
  # Flight-level wind support: based on barometric_pressure column if present
  if ("barometric_pressure" %in% names(df)) {
    matched_lv <- pressure_levels[
      apply(outer(df$barometric_pressure, pressure_levels,
                  function(p, lv) abs(p - lv)), 1, which.min)
    ]
    df$matched_pressure_level <- matched_lv
    df$wind_support_flight <- mapply(
      function(ts, lv) df[[paste0("wind_support_", lv)]][df$barometric_pressure == ts][1],
      df$barometric_pressure, matched_lv
    )
    # Simpler: just use the value at matched level for each row
    df$wind_support_flight <- diag(as.matrix(
      ws_mat[, match(paste0("wind_support_", matched_lv),
                     names(ws_mat)), drop = FALSE]
    ))
  }
  df$wind_support_10m <- round(rnorm(n, 1, 1.5), 2)
  df
}

# Helper: generate fixes for one migration night
.gen_night_fixes <- function(id, lon0, lat0, lon1, lat1,
                              dep_date, n_fixes = 7,
                              pressure_flight = 900) {
  t_dep <- as.POSIXct(paste0(dep_date, " 20:00:00"), tz = "UTC")
  t_arr <- as.POSIXct(paste0(format(as.Date(dep_date) + 1, "%Y-%m-%d"),
                             " 05:30:00"), tz = "UTC")
  times <- seq(t_dep, t_arr, length.out = n_fixes)
  frac  <- seq(0, 1, length.out = n_fixes)
  data.frame(
    individual_local_identifier = id,
    timestamp           = times,
    lon                 = lon0 + frac * (lon1 - lon0) + rnorm(n_fixes, 0, 0.03),
    lat                 = lat0 + frac * (lat1 - lat0) + rnorm(n_fixes, 0, 0.015),
    barometric_pressure = pressure_flight + rnorm(n_fixes, 0, 8),
    vedba_sum           = round(runif(n_fixes, 50, 300), 1),
    external_temperature = round(rnorm(n_fixes, 10, 3), 1),
    stringsAsFactors    = FALSE
  )
}

# Helper: generate roost fixes (daytime, near surface pressure)
.gen_roost_fixes <- function(id, lon, lat, date, n_fixes = 2) {
  t_noon <- as.POSIXct(paste0(date, " 12:00:00"), tz = "UTC")
  times  <- t_noon + sort(sample(-4 * 3600:4 * 3600, n_fixes))
  data.frame(
    individual_local_identifier = id,
    timestamp           = times,
    lon                 = lon + rnorm(n_fixes, 0, 0.001),
    lat                 = lat + rnorm(n_fixes, 0, 0.001),
    barometric_pressure = 1013 + rnorm(n_fixes, 0, 1.5),
    vedba_sum           = round(runif(n_fixes, 0, 20), 1),
    external_temperature = round(rnorm(n_fixes, 14, 2), 1),
    stringsAsFactors    = FALSE
  )
}

# ── TAG_A: 3 nights, starting Bavaria → northeastward ────────────────────────
# Night 1: 2024-09-15 dep  (Bavaria → Czech Republic)
# Night 2: 2024-09-16 dep  (Czech  → Poland)
# Night 3: 2024-09-17 dep  (Poland → Lithuania)

waypoints_A <- list(
  c(lon = 11.0, lat = 48.2),   # Bavaria
  c(lon = 14.5, lat = 49.8),   # Czech Republic / Bohemia
  c(lon = 18.8, lat = 51.5),   # Southern Poland
  c(lon = 23.5, lat = 53.8)    # NE Poland / Lithuania border
)

nights_A <- list(
  .gen_night_fixes("TAG_A", waypoints_A[[1]]["lon"], waypoints_A[[1]]["lat"],
                   waypoints_A[[2]]["lon"], waypoints_A[[2]]["lat"],
                   "2024-09-15", pressure_flight = 855),
  .gen_night_fixes("TAG_A", waypoints_A[[2]]["lon"], waypoints_A[[2]]["lat"],
                   waypoints_A[[3]]["lon"], waypoints_A[[3]]["lat"],
                   "2024-09-16", pressure_flight = 840),
  .gen_night_fixes("TAG_A", waypoints_A[[3]]["lon"], waypoints_A[[3]]["lat"],
                   waypoints_A[[4]]["lon"], waypoints_A[[4]]["lat"],
                   "2024-09-17", pressure_flight = 862)
)

roosts_A <- list(
  .gen_roost_fixes("TAG_A", waypoints_A[[1]]["lon"], waypoints_A[[1]]["lat"], "2024-09-15"),
  .gen_roost_fixes("TAG_A", waypoints_A[[2]]["lon"], waypoints_A[[2]]["lat"], "2024-09-16"),
  .gen_roost_fixes("TAG_A", waypoints_A[[3]]["lon"], waypoints_A[[3]]["lat"], "2024-09-17"),
  .gen_roost_fixes("TAG_A", waypoints_A[[4]]["lon"], waypoints_A[[4]]["lat"], "2024-09-18")
)

# ── TAG_B: 2 nights, same region but lower altitude, slower ──────────────────
waypoints_B <- list(
  c(lon = 10.8, lat = 48.0),
  c(lon = 14.0, lat = 49.5),
  c(lon = 17.8, lat = 51.0)
)

nights_B <- list(
  .gen_night_fixes("TAG_B", waypoints_B[[1]]["lon"], waypoints_B[[1]]["lat"],
                   waypoints_B[[2]]["lon"], waypoints_B[[2]]["lat"],
                   "2024-09-15", n_fixes = 6, pressure_flight = 925),
  .gen_night_fixes("TAG_B", waypoints_B[[2]]["lon"], waypoints_B[[2]]["lat"],
                   waypoints_B[[3]]["lon"], waypoints_B[[3]]["lat"],
                   "2024-09-16", n_fixes = 6, pressure_flight = 918)
)

roosts_B <- list(
  .gen_roost_fixes("TAG_B", waypoints_B[[1]]["lon"], waypoints_B[[1]]["lat"], "2024-09-15"),
  .gen_roost_fixes("TAG_B", waypoints_B[[2]]["lon"], waypoints_B[[2]]["lat"], "2024-09-16"),
  .gen_roost_fixes("TAG_B", waypoints_B[[3]]["lon"], waypoints_B[[3]]["lat"], "2024-09-17")
)

# ── Combine and add wind columns ──────────────────────────────────────────────
raw_df <- bind_rows(
  c(nights_A, roosts_A, nights_B, roosts_B)
) %>%
  arrange(individual_local_identifier, timestamp)

raw_df <- .add_wind_cols(raw_df, pressure_levels)

# Convert to sf
data_sf <- sf::st_as_sf(raw_df, coords = c("lon", "lat"), crs = 4326)

cat("\n── Synthetic dataset ────────────────────────────────────────────────\n")
cat("Individuals:", paste(unique(data_sf$individual_local_identifier), collapse = ", "), "\n")
cat("Date range: ",
    format(min(data_sf$timestamp), "%Y-%m-%d"), "–",
    format(max(data_sf$timestamp), "%Y-%m-%d"), "\n")
cat("Total rows:", nrow(data_sf), "\n")
cat("Columns with wind data:",
    sum(grepl("^wind_support_", names(data_sf))), "levels\n")

# ── 2. Build config directly (no interactive wizard) ─────────────────────────
cfg_nightly <- list(
  firmware            = "NanoFox_finescale",
  pressure_type       = "instantaneous",
  timestamp_col       = "timestamp",
  individual_col      = "individual_local_identifier",
  pressure_col        = "barometric_pressure",
  heading_col         = NULL,
  window_type         = "nightly",
  interpolation       = "none",
  interp_timestep_min = NA_real_,
  vedba_col           = "vedba_sum",
  vedba_aggregation   = "precomputed",
  temp_col            = "external_temperature",
  temp_aggregation    = "mean",
  pressure_levels     = pressure_levels,
  # Synthetic nights have fixes evenly distributed from 20:00–05:30, so the
  # date-boundary pair (last Sep N fix / first Sep N+1 fix) spans only ~52 km.
  # 40 km captures that reliably; use the real 100 km threshold for production.
  min_displacement_km = 40,
  out_path            = "output/test_wind_profiles"
)

cfg_daily <- modifyList(cfg_nightly, list(
  window_type         = "daily",
  min_displacement_km = 0,
  out_path            = "output/test_wind_profiles_daily"
))

cat("\n── Config (nightly) ─────────────────────────────────────────────────\n")
for (nm in names(cfg_nightly))
  cat(sprintf("  %-28s %s\n", nm, paste(cfg_nightly[[nm]], collapse = ", ")))

# ── 3. find_complete_periods() — nightly ──────────────────────────────────────
cat("\n── find_complete_periods() [nightly] ────────────────────────────────\n")
periods_nightly <- find_complete_periods(
  data         = data_sf,
  cfg          = cfg_nightly,
  min_coverage = 0.1,   # low threshold so synthetic data passes
  verbose      = TRUE
)

if (nrow(periods_nightly) == 0) {
  warning("No qualifying nightly periods found — check displacement threshold or ERA5 coverage.")
} else {
  cat("\nQualifying nightly periods:\n")
  print(periods_nightly[, c("individual_id", "t_start", "t_end",
                             "displacement_km", "n_fixes", "era5_coverage")])
  cat("\nBy individual:\n")
  print(table(periods_nightly$individual_id))
}

# ── 4. find_complete_periods() — daily ───────────────────────────────────────
cat("\n── find_complete_periods() [daily] ──────────────────────────────────\n")
periods_daily <- find_complete_periods(
  data         = data_sf,
  cfg          = cfg_daily,
  min_coverage = 0.1,
  verbose      = TRUE
)

if (nrow(periods_daily) == 0) {
  warning("No qualifying daily periods found.")
} else {
  cat("\nQualifying daily periods:\n")
  print(periods_daily[, c("individual_id", "t_start", "t_end",
                           "displacement_km", "n_fixes", "era5_coverage")])
}

# ── 5. pressure_wind_profile() — loop nightly (no interpolation) ─────────────
cat("\n── pressure_wind_profile() [nightly — all individuals × periods] ────\n")

# find_complete_periods returns the date-boundary pair (last fix on d0 /
# first fix on d1), which is mid-night for evenly-spaced synthetic fixes.
# Extend t_end to 06:30 on the arrival date so all in-flight fixes are shown.
.full_night_end <- function(t_end) {
  as.POSIXct(paste0(format(as.Date(t_end), "%Y-%m-%d"), " 06:30:00"), tz = "UTC")
}

if (nrow(periods_nightly) > 0) {
  for (i in seq_len(nrow(periods_nightly))) {
    p <- periods_nightly[i, ]
    cat(sprintf("\n[%d/%d] %s  %s – %s  (%.0f km)\n",
                i, nrow(periods_nightly),
                p$individual_id,
                format(p$t_start, "%Y-%m-%d %H:%M"),
                format(p$t_end,   "%H:%M UTC"),
                p$displacement_km))

    result <- tryCatch(
      pressure_wind_profile(
        data          = data_sf,
        cfg           = cfg_nightly,
        individual_id = p$individual_id,
        t_start       = p$t_start,
        t_end         = .full_night_end(p$t_end),
        elev_z        = 4,
        buffer_deg    = 1.0,
        verbose       = TRUE
      ),
      error = function(e) { message("  ERROR: ", conditionMessage(e)); NULL }
    )

    if (!is.null(result)) {
      cat("  → PNG + RDS saved to:", cfg_nightly$out_path, "\n")
      cat("  → plot_data rows:", nrow(result$plot_data), "\n")
      cat("  → wind_long rows:", if (!is.null(result$wind_long)) nrow(result$wind_long) else 0, "\n")
      cat("  → elev_profile:  ", if (!is.null(result$elev_profile)) "yes" else "no", "\n")
    }
  }
} else {
  cat("Skipped — no qualifying periods.\n")
}

# ── 6. pressure_wind_profile() — spot check daily ────────────────────────────
cat("\n── pressure_wind_profile() [daily — first 2 periods] ───────────────\n")

if (nrow(periods_daily) > 0) {
  for (i in seq_len(min(2L, nrow(periods_daily)))) {
    p <- periods_daily[i, ]
    cat(sprintf("\n[%d] %s  %s\n",
                i, p$individual_id,
                format(as.Date(p$t_start), "%Y-%m-%d")))

    result <- tryCatch(
      pressure_wind_profile(
        data          = data_sf,
        cfg           = cfg_daily,
        individual_id = p$individual_id,
        t_start       = p$t_start,
        t_end         = p$t_end,
        elev_z        = 4,
        buffer_deg    = 1.0,
        verbose       = TRUE
      ),
      error = function(e) { message("  ERROR: ", conditionMessage(e)); NULL }
    )

    if (!is.null(result))
      cat("  → plot assembled,", nrow(result$plot_data), "rows in plot_data\n")
  }
} else {
  cat("Skipped — no qualifying periods.\n")
}

# ── 7. Interpolation methods — all three compared on period 1 of TAG_A ───────
cat("\n── Interpolation method comparison ──────────────────────────────────\n")
cat("Using period 1 (TAG_A, first qualifying night), timestep = 30 min\n\n")

interp_methods <- list(
  list(method = "none",   label = "None  (raw fixes)",                   timestep = NA_real_),
  list(method = "linear", label = "Linear interpolation  (30 min step)", timestep = 30),
  list(method = "crawl",  label = "CRAWL / momentuHMM   (30 min step)",  timestep = 30)
)

interp_summary <- list()

if (nrow(periods_nightly) > 0) {
  p        <- periods_nightly[1, ]
  t_start  <- p$t_start
  t_end    <- .full_night_end(p$t_end)

  for (m in interp_methods) {
    cat(sprintf("── [%s] %s\n", m$method, m$label))

    # Skip crawl gracefully if momentuHMM is not installed
    if (m$method == "crawl" && !requireNamespace("momentuHMM", quietly = TRUE)) {
      cat("   momentuHMM not installed — skipping crawl test.\n\n")
      interp_summary[[m$method]] <- list(
        method        = m$method,
        raw_fixes     = NA_integer_,
        interp_rows   = NA_integer_,
        plot_rows     = NA_integer_,
        wind_rows     = NA_integer_,
        status        = "skipped (momentuHMM not found)"
      )
      next
    }

    cfg_m <- modifyList(cfg_nightly, list(
      interpolation       = m$method,
      interp_timestep_min = m$timestep,
      out_path            = paste0("output/test_wind_profiles_", m$method)
    ))

    res <- tryCatch(
      pressure_wind_profile(
        data          = data_sf,
        cfg           = cfg_m,
        individual_id = p$individual_id,
        t_start       = t_start,
        t_end         = t_end,
        elev_z        = 4,
        buffer_deg    = 1.0,
        verbose       = FALSE  # suppress per-panel messages for cleaner output
      ),
      error = function(e) { message("   ERROR: ", conditionMessage(e)); NULL }
    )

    if (!is.null(res)) {
      n_raw    <- nrow(res$raw_data)
      n_interp <- if (!is.null(res$interp_track)) nrow(res$interp_track) else n_raw
      n_plot   <- nrow(res$plot_data)
      n_wind   <- if (!is.null(res$wind_long)) nrow(res$wind_long) else 0L
      ts_class <- class(res$plot_data$timestamp)[1]

      cat(sprintf("   raw fixes:         %d\n",   n_raw))
      cat(sprintf("   interpolated rows: %d\n",   n_interp))
      cat(sprintf("   plot_data rows:    %d\n",   n_plot))
      cat(sprintf("   wind_long rows:    %d\n",   n_wind))
      cat(sprintf("   timestamp class:   %s\n",   ts_class))
      cat(sprintf("   PNG saved:         %s\n\n",
                  basename(list.files(cfg_m$out_path, "\\.png$")[1])))

      interp_summary[[m$method]] <- list(
        method        = m$method,
        raw_fixes     = n_raw,
        interp_rows   = n_interp,
        plot_rows     = n_plot,
        wind_rows     = n_wind,
        ts_class      = ts_class,
        status        = "ok"
      )
    } else {
      interp_summary[[m$method]] <- list(
        method   = m$method,
        status   = "error"
      )
    }
  }

  # Summary table
  cat("── Comparison summary ───────────────────────────────────────────────\n")
  cat(sprintf("  %-8s  %-12s  %-14s  %-10s  %-8s  %s\n",
              "Method", "Raw fixes", "Interp rows", "Plot rows",
              "Wind rows", "Status"))
  cat(strrep("-", 70), "\n")
  for (s in interp_summary) {
    cat(sprintf("  %-8s  %-12s  %-14s  %-10s  %-8s  %s\n",
                s$method %||% "?",
                if (!is.null(s$raw_fixes))   s$raw_fixes   else "-",
                if (!is.null(s$interp_rows)) s$interp_rows else "-",
                if (!is.null(s$plot_rows))   s$plot_rows   else "-",
                if (!is.null(s$wind_rows))   s$wind_rows   else "-",
                s$status %||% "?"))
  }
} else {
  cat("Skipped — no qualifying periods.\n")
}

# ── 8. RDS round-trip check ───────────────────────────────────────────────────
cat("\n── RDS round-trip ───────────────────────────────────────────────────\n")

rds_files <- list.files("output/test_wind_profiles", pattern = "\\.rds$",
                        full.names = TRUE)
if (length(rds_files) > 0) {
  rds_file <- rds_files[1]
  loaded   <- readRDS(rds_file)
  cat("Loaded:", basename(rds_file), "\n")
  cat("Fields:", paste(names(loaded), collapse = ", "), "\n")
  cat("Config firmware:", loaded$config$firmware, "\n")
  cat("Individual:     ", loaded$individual_id, "\n")
  cat("t_start:        ", format(loaded$t_start, "%Y-%m-%d %H:%M UTC"), "\n")
  cat("plot_data rows: ", nrow(loaded$plot_data), "\n")
  cat("wind_long rows: ", if (!is.null(loaded$wind_long)) nrow(loaded$wind_long) else 0, "\n")
  cat("plot class:     ", class(loaded$plot), "\n")
} else {
  cat("No .rds files found — plots may not have been saved.\n")
}

cat("\n══ Test complete ════════════════════════════════════════════════════\n")
