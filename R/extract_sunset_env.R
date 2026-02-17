extract_sunset_env <- function(
    timestamps,
    latitudes,
    longitudes,
    lat_prev = NULL,
    lon_prev = NULL,
    IDs = NULL,
    shift_hours = 0,
    raster_by_year,
    var_names,
    tz = "UTC",
    time_round = "hour",
    coord_crs = "EPSG:4326",
    keep_debug_cols = TRUE,
    verbose = TRUE
) {
  require(terra)
  require(lubridate)
  require(dplyr)
  require(suncalc)

  n <- length(timestamps)
  stopifnot(length(latitudes) == n, length(longitudes) == n)
  if (!is.null(IDs)) stopifnot(length(IDs) == n)

  # previous coords default to current if not supplied
  if (is.null(lat_prev)) lat_prev <- latitudes
  if (is.null(lon_prev)) lon_prev <- longitudes
  stopifnot(length(lat_prev) == n, length(lon_prev) == n)

  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be a named list with year names, e.g. list('2024'='path.grib').")
  }
  if (missing(var_names) || length(var_names) < 1) stop("Provide var_names in the GRIB variable order.")

  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }

  # ---- base table ----
  df <- data.frame(
    .row_id = seq_len(n),
    timestamp = as.POSIXct(timestamps, tz = tz),
    date = as.Date(timestamps),
    lat = latitudes,
    lon = longitudes,
    lat_prev = lat_prev,
    lon_prev = lon_prev,
    ID = if (is.null(IDs)) NA_character_ else as.character(IDs),
    stringsAsFactors = FALSE
  ) %>%
    dplyr::filter(!is.na(.row_id))

  # ---- sunset at current coords ----
  if (verbose) message("Computing sunset at current coords...")
  sun_now <- suncalc::getSunlightTimes(
    data = df,
    tz   = tz,
    keep = c("sunset")
  )
  df$sunset_time_now <- as.POSIXct(sun_now$sunset, tz = tz)

  # ---- decide whether to switch to prev coords ----
  # Rule per your request:
  #   if sunset is AFTER timestamp => use that sunset (and current coords)
  #   else (sunset <= timestamp)    => recompute sunset using lat_prev/lon_prev and extract using prev coords
  df$use_prev <- !is.na(df$sunset_time_now) & (df$sunset_time_now <= df$timestamp)

  if (verbose) {
    message("Rows using prev coords: ", sum(df$use_prev, na.rm = TRUE), " / ", nrow(df))
  }

  # ---- recompute sunset at prev coords where needed ----
  df$sunset_time <- df$sunset_time_now
  if (any(df$use_prev, na.rm = TRUE)) {
    if (verbose) message("Recomputing sunset for rows where sunset<=timestamp using lat_prev/lon_prev...")
    idx <- which(df$use_prev)
    df_prev <- df[idx,]
    df$lat <- df$lat_prev
    df$lon <- df$lon_prev
    sun_prev <- suncalc::getSunlightTimes(
      data = df[idx,],
      tz   = tz,
      keep = c("sunset")
    )
    df$sunset_time[idx] <- as.POSIXct(sun_prev$sunset, tz = tz)
  }

  # extraction coords: current or prev depending on flag
  df$lon_use <- ifelse(df$use_prev, df$lon_prev, df$lon)
  df$lat_use <- ifelse(df$use_prev, df$lat_prev, df$lat)

  # ---- target hour: sunset + 1h (+ shift), rounded ----
  df <- df %>%
    mutate(
      target_time_raw = sunset_time + lubridate::hours(1 + shift_hours),
      target_time     = lubridate::round_date(target_time_raw, unit = time_round),
      year            = lubridate::year(target_time)
    )

  # ---- preallocate env columns ----
  out_cols <- paste0(var_names, "_sunset1h_", shift_hours, "h")
  for (nm in out_cols) df[[nm]] <- NA_real_

  years_needed <- sort(unique(df$year[!is.na(df$year)]))
  if (verbose) message("Years needed: ", paste(years_needed, collapse = ", "))

  # ---- extract year-by-year ----
  for (yr in years_needed) {
    yr_chr <- as.character(yr)
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping year ", yr_chr, " (no raster provided).")
      next
    }

    r <- open_raster(raster_by_year[[yr_chr]])
    rt <- terra::time(r)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no terra::time() vector.")

    idx_rows <- which(df$year == yr & !is.na(df$target_time) & !is.na(df$lon_use) & !is.na(df$lat_use))
    if (length(idx_rows) == 0) next

    rt_num <- as.numeric(rt)
    t_num  <- as.numeric(df$target_time[idx_rows])
    idx_match <- match(t_num, rt_num)

    ok <- which(!is.na(idx_match))
    if (length(ok) == 0) {
      if (verbose) message("No matching sunset+1h times in raster for year ", yr_chr)
      next
    }

    sub_rows <- idx_rows[ok]
    time_idx <- idx_match[ok]
    uniq_time_idx <- sort(unique(time_idx))

    pts <- terra::vect(
      data.frame(x = df$lon_use[sub_rows], y = df$lat_use[sub_rows]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) {
      pts <- terra::project(pts, r_crs)
    }

    if (verbose) {
      message("Year ", yr_chr, ": rows=", length(sub_rows), ", unique target hours=", length(uniq_time_idx))
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_time_idx), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    # GRIB layout: each hour has one layer per variable.
    # For a given time index ti, select ALL layers whose time == that hour (a block),
    # then assign names in var_names order.
    for (i in seq_along(uniq_time_idx)) {
      if (verbose) utils::setTxtProgressBar(pb, i)
      ti <- uniq_time_idx[i]
      p_idx <- which(time_idx == ti)
      if (length(p_idx) == 0) next

      hour_val <- rt_num[ti]
      layer_idx <- which(rt_num == hour_val)

      if (length(layer_idx) < 1) next

      # keep exactly up to length(var_names) in the GRIB order
      layer_idx <- layer_idx[seq_len(min(length(layer_idx), length(var_names)))]
      rr <- r[[layer_idx]]
      names(rr) <- var_names[seq_len(terra::nlyr(rr))]

      vals <- terra::extract(rr, pts[p_idx], ID = FALSE)
      target_rows <- sub_rows[p_idx]

      # write into the corresponding subset of out_cols (in case fewer layers than var_names)
      df[target_rows, out_cols[seq_len(ncol(vals))]] <- as.matrix(vals)
    }
  }

  # ---- return WITHOUT duplicated base columns (join-safe) ----
  # Always return .row_id + env cols; optionally include a small debug set.
  if (keep_debug_cols) {
    return(df %>%
             dplyr::select(
               .row_id,
               sunset_time,
               target_time,
               use_prev,
               dplyr::all_of(out_cols)
             ))
  } else {
    return(df %>%
             dplyr::select(.row_id, dplyr::all_of(out_cols)))
  }
}

raster_by_year <- list(
  "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
  "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
  "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
  "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
)

vars <- c("u10","v10","t2m",
          "msl","sp","tp",
          "u100","v100",
          "i10fg","tcc")

load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")

bats_daily$.row_id <- seq_len(nrow(bats_daily))
bats_daily_sunset <- bats_daily
hours <- -2:2*24
for(hour in hours){
  # ensure stable row id once
  bats_daily_sunset$.row_id <- seq_len(nrow(bats_daily_sunset))

  sun1 <- extract_sunset_env(
    timestamps = bats_daily_sunset$timestamp,
    latitudes  = bats_daily_sunset$lat,
    longitudes = bats_daily_sunset$lon,
    lat_prev   = bats_daily_sunset$lat_prev,
    lon_prev   = bats_daily_sunset$lon_prev,
    IDs        = bats_daily_sunset$individual_local_identifier,
    shift_hours = 0,
    raster_by_year = raster_by_year,
    var_names = vars,
    tz = "UTC",
    keep_debug_cols = TRUE
  )

  # join-safe: bring in only what sun1 returns
  bats_daily_sunset <- dplyr::left_join(bats_daily_sunset, sun1, by = ".row_id")
}
# summary(bats_daily_sunset)

# calculate wind features
source("./R/calculate_wind_features.R")
bats_daily_sunset <- calculate_wind_features(
  data = bats_daily_sunset,
  u_col_base = "u10",
  v_col_base = "v10",
  distance_col = "distance",
  time_diff_col = "dt_prev",
  bearing_col = "azimuth_prev",
  offsets = -2:2,
  offset_units = "days",
  time_diff_units = "seconds"
)

bats_daily_sunset <- calculate_wind_features(
  data = bats_daily_sunset,
  u_col_base = "u100",
  v_col_base = "v100",
  distance_col = "distance",
  time_diff_col = "dt_prev",
  bearing_col = "azimuth_prev",
  offsets = -2:2,
  offset_units = "days",
  time_diff_units = "seconds"
)

# delta wind direction helper (degrees), result in [-180, 180]
circ_diff_deg <- function(to_deg, from_deg) {
  ((to_deg - from_deg + 180) %% 360) - 180
}

bats_daily_sunset <- bats_daily_sunset %>%
  mutate(
    # ---- scalar ERA5 variables ----
    dt2m_1d   = t2m_0h - `t2m_-24h`,
    dmsl_1d   = msl_0h - `msl_-24h`,
    dsp_1d    = sp_0h  - `sp_-24h`,
    dtp_1d    = tp_0h  - `tp_-24h`,
    dtcc_1d   = tcc_0h - `tcc_-24h`,

    # ---- wind magnitude/support etc. (pick your level, here "100") ----
    ## 10m
    dwindsp10_1d  = windsp10_0h - `windsp10_-24h`,
    dwinddir10_1d = circ_diff_deg(winddir10_0h, `winddir10_-24h`),

    dws10_1d      = ws10_0h - `ws10_-24h`,
    dcw10_1d      = cw10_0h - `cw10_-24h`,
    dairspeed10_1d = airspeed10_0h - `airspeed10_-24h`,
    ## 100m
    dwindsp100_1d  = windsp100_0h - `windsp100_-24h`,
    dwinddir100_1d = circ_diff_deg(winddir100_0h, `winddir100_-24h`),

    dws100_1d      = ws100_0h - `ws100_-24h`,
    dcw100_1d      = cw100_0h - `cw100_-24h`,
    dairspeed100_1d = airspeed100_0h - `airspeed100_-24h`
  )

summary(bats_daily_sunset)
# remove extra ID, timestamp, lat, lon, sunset_time, target_time

save(bats_daily_sunset, file = "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_sunset_env.robj")


