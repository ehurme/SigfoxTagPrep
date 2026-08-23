# test_pressure_wind_profile_6h.R
# ─────────────────────────────────────────────────────────────────────────────
# Smoke-test for R/pressure_wind_profile.R using 6-hour location sampling
# and 6-hour minimum-pressure (maximum-altitude) windows.
#
# This mimics a tag that sends one location every 6 h, where each pressure
# reading is the lowest pressure (highest altitude) observed in the previous
# 6-hour window.
#
# Run from the project root:
#   source("explore/test_pressure_wind_profile_6h.R")
# ─────────────────────────────────────────────────────────────────────────────

library(sf)
library(dplyr)
library(tidyr)

source("R/pressure_wind_profile.R")

# ── 1. Synthetic data helpers ─────────────────────────────────────────────────

set.seed(2024)

pressure_levels <- c(500, 600, 700, 800, 850, 900, 925, 950, 1000)

# Add ERA5-style wind columns to a data frame.
.add_wind_cols <- function(df, pressure_levels) {
  n <- nrow(df)
  for (lv in pressure_levels) {
    ws_mean <- (1000 - lv) / 100
    df[[paste0("wind_support_", lv)]] <- round(rnorm(n, ws_mean, 2), 2)
    df[[paste0("wind_speed_",   lv)]] <- round(abs(rnorm(n, ws_mean + 3, 1.5)), 2)
  }
  ws_mat <- df[, paste0("wind_support_", pressure_levels), drop = FALSE]
  best_idx <- apply(ws_mat, 1, which.max)
  df$best_wind_level   <- pressure_levels[best_idx]
  df$best_wind_support <- apply(ws_mat, 1, max, na.rm = TRUE)

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
    df$wind_support_flight <- diag(as.matrix(
      ws_mat[, match(paste0("wind_support_", matched_lv), names(ws_mat)), drop = FALSE]
    ))
  }
  df$wind_support_10m <- round(rnorm(n, 1, 1.5), 2)
  df
}

# Generate a single 6-hour location fix.
# pressure_mode controls the intended behaviour of the *window* ending at this
# fix: "roost" (near surface), "depart" (starting to climb),
# "cruise" (mid-flight), "arrive" (descending).
.gen_fix <- function(id, lon, lat, timestamp, pressure_mode = "roost") {
  pressure <- switch(pressure_mode,
    roost  = 1013 + rnorm(1, 0, 2),
    depart = 975 + rnorm(1, 0, 8),
    cruise = 900 + rnorm(1, 0, 12),
    arrive = 980 + rnorm(1, 0, 8)
  )
  data.frame(
    individual_local_identifier = id,
    timestamp           = timestamp,
    lon                 = lon,
    lat                 = lat,
    barometric_pressure = round(pressure, 1),
    vedba_sum           = round(runif(1, 50, 300), 1),
    external_temperature = round(rnorm(1, 10, 3), 1),
    stringsAsFactors    = FALSE
  )
}

# Generate one migration night for a 6-hour tag, plus surrounding roost fixes.
# A "night" produces fixes at 20:00 (departure, day N), 02:00 and 08:00 (day N+1).
.gen_night_6h <- function(id, lon0, lat0, lon1, lat1, dep_date) {
  t_dep <- as.POSIXct(paste0(dep_date, " 20:00:00"), tz = "UTC")
  t_mid <- as.POSIXct(paste0(format(as.Date(dep_date) + 1, "%Y-%m-%d"),
                             " 02:00:00"), tz = "UTC")
  t_arr <- as.POSIXct(paste0(format(as.Date(dep_date) + 1, "%Y-%m-%d"),
                             " 08:00:00"), tz = "UTC")

  frac_mid <- as.numeric(t_mid - t_dep) / as.numeric(t_arr - t_dep)

  lon_mid <- lon0 + frac_mid * (lon1 - lon0) + rnorm(1, 0, 0.05)
  lat_mid <- lat0 + frac_mid * (lat1 - lat0) + rnorm(1, 0, 0.03)

  bind_rows(
    .gen_fix(id, lon0, lat0, t_dep, "depart"),
    .gen_fix(id, lon_mid, lat_mid, t_mid, "cruise"),
    .gen_fix(id, lon1, lat1, t_arr, "arrive")
  )
}

# Generate daytime roost fixes for a 6-hour schedule.
.gen_roost_6h <- function(id, lon, lat, date) {
  times <- as.POSIXct(
    paste0(date, c(" 12:00:00", " 18:00:00")),
    tz = "UTC"
  )
  data.frame(
    individual_local_identifier = id,
    timestamp           = times,
    lon                 = lon + rnorm(2, 0, 0.002),
    lat                 = lat + rnorm(2, 0, 0.002),
    barometric_pressure = 1013 + rnorm(2, 0, 2),
    vedba_sum           = round(runif(2, 0, 20), 1),
    external_temperature = round(rnorm(2, 14, 2), 1),
    stringsAsFactors    = FALSE
  )
}

# ── 2. Build two synthetic individuals ────────────────────────────────────────

# TAG_6H: 3 nights, central Europe north-eastward.
waypoints <- list(
  c(lon = 11.0, lat = 48.2),
  c(lon = 14.5, lat = 49.8),
  c(lon = 18.8, lat = 51.5),
  c(lon = 23.5, lat = 53.8)
)

nights <- list(
  .gen_night_6h("TAG_6H", waypoints[[1]]["lon"], waypoints[[1]]["lat"],
                waypoints[[2]]["lon"], waypoints[[2]]["lat"], "2024-09-15"),
  .gen_night_6h("TAG_6H", waypoints[[2]]["lon"], waypoints[[2]]["lat"],
                waypoints[[3]]["lon"], waypoints[[3]]["lat"], "2024-09-16"),
  .gen_night_6h("TAG_6H", waypoints[[3]]["lon"], waypoints[[3]]["lat"],
                waypoints[[4]]["lon"], waypoints[[4]]["lat"], "2024-09-17")
)

roosts <- list(
  .gen_roost_6h("TAG_6H", waypoints[[1]]["lon"], waypoints[[1]]["lat"], "2024-09-15"),
  .gen_roost_6h("TAG_6H", waypoints[[2]]["lon"], waypoints[[2]]["lat"], "2024-09-16"),
  .gen_roost_6h("TAG_6H", waypoints[[3]]["lon"], waypoints[[3]]["lat"], "2024-09-17"),
  .gen_roost_6h("TAG_6H", waypoints[[4]]["lon"], waypoints[[4]]["lat"], "2024-09-18")
)

raw_df <- bind_rows(c(nights, roosts)) %>%
  arrange(individual_local_identifier, timestamp)

raw_df <- .add_wind_cols(raw_df, pressure_levels)

data_sf <- sf::st_as_sf(raw_df, coords = c("lon", "lat"), crs = 4326)

cat("\n── Synthetic 6-hour dataset ───────────────────────────────────────\n")
cat("Individuals:", paste(unique(data_sf$individual_local_identifier), collapse = ", "), "\n")
cat("Date range: ",
    format(min(data_sf$timestamp), "%Y-%m-%d %H:%M"), "–",
    format(max(data_sf$timestamp), "%Y-%m-%d %H:%M"), "\n")
cat("Total rows:", nrow(data_sf), "\n")
cat("Fix interval:", round(median(diff(as.numeric(raw_df$timestamp)) / 3600), 1), "hours\n")
cat("Columns with wind data:", sum(grepl("^wind_support_", names(data_sf))), "levels\n")

# ── 3. Config for 6-hour minimum-pressure tag ───────────────────────────────

cfg_6h <- list(
  firmware            = "TinyFox_6h",
  pressure_type       = "min_6h",
  timestamp_col       = "timestamp",
  individual_col      = "individual_local_identifier",
  pressure_col        = "barometric_pressure",
  heading_col         = NULL,
  window_type         = "nightly",
  interpolation       = "linear",
  interp_timestep_min = 360,   # 6-hour location interval
  vedba_col           = "vedba_sum",
  vedba_aggregation   = "precomputed",
  temp_col            = "external_temperature",
  temp_aggregation    = "mean",
  pressure_levels     = pressure_levels,
  min_displacement_km = 40,    # lowered for synthetic data
  out_path            = "output/test_wind_profiles_6h"
)

cat("\n── Config (6-hour minimum pressure) ─────────────────────────────────\n")
for (nm in names(cfg_6h))
  cat(sprintf("  %-28s %s\n", nm, paste(cfg_6h[[nm]], collapse = ", ")))

# ── 4. Find qualifying periods ────────────────────────────────────────────────

cat("\n── find_complete_periods() [nightly, 6-hour data] ────────────────\n")
periods_6h <- find_complete_periods(
  data         = data_sf,
  cfg          = cfg_6h,
  min_coverage = 0.1,
  verbose      = TRUE
)

if (nrow(periods_6h) == 0) {
  warning("No qualifying nightly periods found — check displacement threshold or ERA5 coverage.")
} else {
  cat("\nQualifying periods:\n")
  print(periods_6h[, c("individual_local_identifier", "t_start", "t_end",
                       "displacement_km", "n_fixes", "era5_coverage")])
}

# ── 5. Run pressure_wind_profile() for each qualifying period ─────────────────

cat("\n── pressure_wind_profile() [6-hour data] ───────────────────────────\n")
results <- list()

if (nrow(periods_6h) > 0) {
  for (i in seq_len(nrow(periods_6h))) {
    p <- periods_6h[i, ]
    cat(sprintf("\n[%d/%d] %s  %s – %s  (%.0f km)\n",
                i, nrow(periods_6h),
                p$individual_local_identifier,
                format(p$t_start, "%Y-%m-%d %H:%M"),
                format(p$t_end, "%H:%M UTC"),
                p$displacement_km))

    res <- tryCatch(
      pressure_wind_profile(
        data          = data_sf,
        cfg           = cfg_6h,
        individual_id = p$individual_local_identifier,
        t_start       = p$t_start,
        t_end         = p$t_end,
        elev_z        = 4,
        buffer_deg    = 1.0,
        verbose       = TRUE
      ),
      error = function(e) { message("  ERROR: ", conditionMessage(e)); NULL }
    )

    results[[i]] <- res
    if (!is.null(res)) {
      cat("  → PNG + RDS saved to:", cfg_6h$out_path, "\n")
      cat("  → plot_data rows:", nrow(res$plot_data), "\n")
      cat("  → wind_long rows:", if (!is.null(res$wind_long)) nrow(res$wind_long) else 0, "\n")
      cat("  → elev_profile:",   if (!is.null(res$elev_profile)) "yes" else "no", "\n")
    }
  }
} else {
  cat("Skipped — no qualifying periods.\n")
}

# ── 6. Summary ────────────────────────────────────────────────────────────────

cat("\n══ 6-hour test summary ═══════════════════════════════════════════════\n")
ok <- !vapply(results, is.null, logical(1))
cat("Completed:", sum(ok), "/", length(results), "periods\n")
cat("Output files:\n")
print(list.files(cfg_6h$out_path, pattern = "\\.(png|rds)$", full.names = TRUE))

if (sum(ok) > 0) {
  r <- results[[which(ok)[1]]]
  cat("\nFirst successful result:\n")
  cat("  raw fixes:      ", nrow(r$raw_data), "\n")
  cat("  interp rows:    ", if (!is.null(r$interp_track)) nrow(r$interp_track) else 0, "\n")
  cat("  plot_data rows: ", nrow(r$plot_data), "\n")
  cat("  wind_long rows: ", if (!is.null(r$wind_long)) nrow(r$wind_long) else 0, "\n")
}

cat("\n══ Test complete ══════════════════════════════════════════════════════\n")
