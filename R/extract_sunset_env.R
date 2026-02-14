extract_sunset_env <- function(
    timestamps,
    latitudes,
    longitudes,
    IDs = NULL,
    shift_hours = 0,
    raster_by_year,
    var_names,
    tz = "UTC",
    time_round = "hour",
    coord_crs = "EPSG:4326",
    verbose = TRUE
) {
  require(terra)
  require(lubridate)
  require(dplyr)
  require(suncalc)

  n <- length(timestamps)
  stopifnot(length(latitudes) == n, length(longitudes) == n)
  if (!is.null(IDs)) stopifnot(length(IDs) == n)

  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be a named list with year names, e.g. list('2024'='path.grib').")
  }
  if (missing(var_names) || length(var_names) < 1) stop("Provide var_names in the raster layer order.")

  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }

  # ---- build base table ----
  df <- data.frame(
    #.row_id = seq_len(timestamps),
    timestamp = as.POSIXct(timestamps, tz = tz),
    date = as.Date(timestamps),
    lat = latitudes,
    lon = longitudes,
    ID = if (is.null(IDs)) NA_character_ else as.character(IDs),
    stringsAsFactors = FALSE
  )
  df$.row_id <- 1:nrow(df)
  # %>% dplyr::filter(!is.na(.row_id)) # defensive

  # ---- compute sunset (same date as timestamp, at that location) ----
  if (verbose) message("Computing sunset times per fix...")

  sun_tbl <- suncalc::getSunlightTimes(
    data = df,
    # lat  = df$latitude,
    # lon  = df$longitude,
    tz   = tz,
    keep = c("sunset")
  )
  # sunset can be NA at high latitudes in some seasons; handle later
  df$sunset_time <- as.POSIXct(sun_tbl$sunset, tz = tz)

  # ---- target hour = sunset + 1h (+ optional shift), rounded to raster hour ----
  df <- df %>%
    mutate(
      target_time_raw = sunset_time + lubridate::hours(1 + shift_hours),
      target_time     = lubridate::round_date(target_time_raw, unit = time_round),
      year            = lubridate::year(target_time)
    )

  # preallocate output columns
  out_cols <- paste0(var_names, "_sunset1h_", shift_hours, "h")
  for (nm in out_cols) df[[nm]] <- NA_real_

  years_needed <- sort(unique(df$year[is.finite(df$year)]))
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

    # rows for this year with valid target_time
    idx_rows <- which(df$year == yr & !is.na(df$target_time))
    if (length(idx_rows) == 0) next

    # match target_time to raster time vector (numeric match)
    t_num  <- as.numeric(df$target_time[idx_rows])
    rt_num <- as.numeric(rt)
    idx_match <- match(t_num, rt_num)

    ok <- which(!is.na(idx_match))
    if (length(ok) == 0) {
      if (verbose) message("No matching sunset+1h times in raster for year ", yr_chr)
      next
    }

    sub_rows <- idx_rows[ok]
    time_idx <- idx_match[ok]
    uniq_time_idx <- sort(unique(time_idx))

    # points for the OK rows (one per row)
    pts <- terra::vect(
      data.frame(x = df$lon[sub_rows], y = df$lat[sub_rows]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    # project to raster CRS if needed
    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) {
      pts <- terra::project(pts, r_crs)
    }

    if (verbose) {
      message("Year ", yr_chr, ": rows=", length(sub_rows), ", unique target hours=", length(uniq_time_idx))
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_time_idx), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    # IMPORTANT: in these GRIB stacks, each hour has one layer per variable.
    # So for a given hour, we must select ALL layers whose time == that hour (a block),
    # then take them in the provided var_names order.
    for (i in seq_along(uniq_time_idx)) {
      if (verbose) utils::setTxtProgressBar(pb, i)
      ti <- uniq_time_idx[i]

      # which point-rows correspond to this raster time index
      p_idx <- which(time_idx == ti)
      if (length(p_idx) == 0) next

      # all layer indices for this time (should be ~length(var_names))
      layer_idx <- which(rt_num == rt_num[ti])

      if (length(layer_idx) < length(var_names)) {
        # still extract what we can, but warn
        if (verbose) message("\nWarning: year ", yr_chr, " time=", as.character(rt[ti]),
                             " has only ", length(layer_idx), " layers; expected ", length(var_names))
      }

      layer_idx <- layer_idx[seq_len(min(length(layer_idx), length(var_names)))]
      rr <- r[[layer_idx]]
      names(rr) <- var_names[seq_len(terra::nlyr(rr))]

      vals <- terra::extract(rr, pts[p_idx], ID = FALSE)

      # write back into df
      target_rows <- sub_rows[p_idx]
      df[target_rows, out_cols[seq_len(ncol(vals))]] <- as.matrix(vals)
    }
  }

  # return just the useful columns + extracted env
  df %>%
    dplyr::select(.row_id, ID, timestamp, lat, lon, sunset_time, target_time, dplyr::all_of(out_cols))
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

hours <- -2:2*24
for(hour in hours){
  sun1 <- extract_sunset_env(
    timestamps = bats_daily$timestamp,
    latitudes  = bats_daily$lat,
    longitudes = bats_daily$lon,
    IDs        = bats_daily$individual_local_identifier,
    shift_hours = hour,
    raster_by_year = raster_by_year,
    var_names = c("u10","v10","t2m","msl","sp","tp","u100","v100","i10fg","tcc"),
    tz = "UTC",
    verbose = TRUE
  )

  # join back by .row_id if you already have it; otherwise add one first
  bats_daily$.row_id <- seq_len(nrow(bats_daily))
  bats_daily_sunset <- dplyr::left_join(bats_daily, sun1, by = ".row_id")
}


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
summary(bats_daily_sunset)
save(bats_daily_sunset, file = "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_sunset_env.robj")
