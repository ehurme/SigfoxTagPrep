extract_nighttime_hours_24h <- function(timestamps, latitudes, longitudes, tz = "UTC") {
  require(lubridate)
  require(suncalc)

  stopifnot(length(timestamps) == length(latitudes), length(latitudes) == length(longitudes))

  out <- vector("list", length(timestamps))

  for (i in seq_along(timestamps)) {
    t0 <- as.POSIXct(timestamps[i], tz = tz)
    start <- t0 - hours(24)
    end <- t0

    sun_today <- suncalc::getSunlightTimes(
      date = as.Date(t0, tz = tz), lat = latitudes[i], lon = longitudes[i], tz = tz
    )
    sun_yday <- suncalc::getSunlightTimes(
      date = as.Date(t0 - days(1), tz = tz), lat = latitudes[i], lon = longitudes[i], tz = tz
    )

    # Night in the 24h window is: (yday sunset -> today sunrise) AND (today sunset -> end)
    night1_start <- max(sun_yday$sunset, start)
    night1_end   <- min(sun_today$sunrise, end)

    night2_start <- max(sun_today$sunset, start)
    night2_end   <- end

    hours_seq <- seq(from = start, to = end, by = "hour")
    keep <- rep(FALSE, length(hours_seq))

    if (!is.na(night1_start) && !is.na(night1_end) && night1_start < night1_end) {
      keep <- keep | (hours_seq >= night1_start & hours_seq <= night1_end)
    }
    if (!is.na(night2_start) && !is.na(night2_end) && night2_start < night2_end) {
      keep <- keep | (hours_seq >= night2_start & hours_seq <= night2_end)
    }

    out[[i]] <- lubridate::round_date(hours_seq[keep], unit = "hour")
  }

  out
}

extract_avg_night_env_from_year_stacks <- function(
    timestamps,
    latitudes,
    longitudes,
    IDs = NULL,
    shift_hours = 0,
    raster_by_year,
    var_names = NULL,
    tz = "UTC",
    coord_crs = "EPSG:4326",
    verbose = TRUE
) {
  require(terra)
  require(lubridate)
  require(dplyr)

  n <- length(timestamps)
  stopifnot(length(latitudes) == n, length(longitudes) == n)
  if (!is.null(IDs)) stopifnot(length(IDs) == n)

  # stable row id for joining
  df <- data.frame(
    .row_id = seq_len(n),
    timestamp = as.POSIXct(timestamps, tz = tz),
    latitude = latitudes,
    longitude = longitudes,
    ID = if (is.null(IDs)) NA_character_ else as.character(IDs),
    stringsAsFactors = FALSE
  )

  # shift & round fix time (this is the "end" of the 24h window)
  df <- df |>
    mutate(
      .time_adj  = as.POSIXct(timestamp, tz = tz) + hours(shift_hours),
      .time_round = round_date(.time_adj, "hour"),
      .year = year(.time_round)
    )

  # open rasters lazily
  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }
  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be named with years, e.g. list('2024'='path_or_raster').")
  }

  # infer var names if needed
  if (is.null(var_names)) {
    r0 <- open_raster(raster_by_year[[1]])
    vn0 <- tryCatch(terra::varnames(r0), error = function(e) NULL)
    var_names <- if (!is.null(vn0) && length(vn0) == terra::nlyr(r0)) vn0 else names(r0)
  }

  # build nighttime hours (list length n)
  if (verbose) message("Computing night-hour sequences per fix...")
  night_hours <- extract_nighttime_hours_24h(df$.time_adj, df$latitude, df$longitude, tz = tz)

  # expand to long table: one row per (fix, night_hour)
  # memory-safe: store only needed columns
  if (verbose) message("Expanding to fix-hour table...")
  long_tbl <- dplyr::bind_rows(lapply(seq_len(n), function(i) {
    nh <- night_hours[[i]]
    if (length(nh) == 0) return(NULL)
    data.frame(
      .row_id = df$.row_id[i],
      night_hour = nh,
      latitude = df$latitude[i],
      longitude = df$longitude[i],
      ID = df$ID[i],
      year = lubridate::year(nh[1]),
      stringsAsFactors = FALSE
    )
  }))

  if (nrow(long_tbl) == 0) {
    if (verbose) message("No night hours found for any fix.")
    return(list(night_data = NULL, average_night_data = NULL))
  }

  # preallocate env columns in long table
  env_cols <- paste0(var_names, "_", shift_hours, "h")
  for (nm in env_cols) long_tbl[[nm]] <- NA_real_

  # extract year-by-year, and within year by unique raster hour index
  years_needed <- sort(unique(long_tbl$year))
  if (verbose) message("Years in night-hour table: ", paste(years_needed, collapse = ", "))

  for (yr in years_needed) {
    yr_chr <- as.character(yr)
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping year ", yr_chr, " (no raster provided).")
      next
    }

    r <- open_raster(raster_by_year[[yr_chr]])
    rt <- terra::time(r)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no time vector (terra::time() is NULL).")

    sub_idx <- which(long_tbl$year == yr)
    if (length(sub_idx) == 0) next

    # match each night_hour to raster time index
    t_num <- as.numeric(as.POSIXct(long_tbl$night_hour[sub_idx], tz = tz))
    rt_num <- as.numeric(rt)
    idx_match <- match(t_num, rt_num)

    ok <- which(!is.na(idx_match))
    if (length(ok) == 0) {
      if (verbose) message("No matching raster hours for year ", yr_chr)
      next
    }

    sub_ok <- sub_idx[ok]
    time_idx <- idx_match[ok]
    uniq_time_idx <- sort(unique(time_idx))

    # points for the ok rows
    pts <- terra::vect(
      data.frame(x = long_tbl$longitude[sub_ok], y = long_tbl$latitude[sub_ok]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    # project points to raster CRS if needed
    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) {
      pts <- terra::project(pts, r_crs)
    }

    if (verbose) {
      message("Year ", yr_chr, ": rows=", length(sub_ok), ", unique hours=", length(uniq_time_idx))
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_time_idx), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    for (i in seq_along(uniq_time_idx)) {
      if (verbose) utils::setTxtProgressBar(pb, i)
      ti <- uniq_time_idx[i]
      p_idx <- which(time_idx == ti)
      if (length(p_idx) == 0) next

      rr <- r[[ti]]
      if (!is.null(var_names) && length(var_names) == terra::nlyr(rr)) names(rr) <- var_names

      vals <- terra::extract(rr, pts[p_idx], ID = FALSE)

      # write back into long_tbl for these rows
      long_tbl[sub_ok[p_idx], env_cols] <- as.matrix(vals)
    }
  }

  # ---- aggregate back to one row per fix ----
  # mean for most variables, sum for tp (if present)
  if (verbose) message("Aggregating night-hour values to per-fix averages...")

  has_tp <- any(grepl("^tp_", env_cols)) || any(var_names == "tp")
  tp_col <- paste0("tp_", shift_hours, "h")
  # Note: env_cols are already suffixed; use those.
  avg_tbl <- long_tbl |>
    dplyr::group_by(.row_id, ID) |>
    dplyr::summarise(
      latitude  = dplyr::first(latitude),
      longitude = dplyr::first(longitude),
      night_n   = dplyr::n(),
      dplyr::across(dplyr::all_of(env_cols), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    )

  # If tp exists, override mean with sum (precip is usually additive over time)
  if (tp_col %in% names(avg_tbl)) {
    tp_sum <- long_tbl |>
      dplyr::group_by(.row_id) |>
      dplyr::summarise(tp_sum = sum(.data[[tp_col]], na.rm = TRUE), .groups = "drop")
    avg_tbl <- avg_tbl |>
      dplyr::left_join(tp_sum, by = ".row_id") |>
      dplyr::mutate(!!tp_col := .data$tp_sum) |>
      dplyr::select(-tp_sum)
  }

  # reattach original fix timestamp (unshifted) for clarity
  avg_tbl <- avg_tbl |>
    dplyr::left_join(df[, c(".row_id", "timestamp")], by = ".row_id") |>
    dplyr::relocate(timestamp, .after = ID)

  # return both
  list(
    night_data = long_tbl,          # one row per (fix, night hour)
    average_night_data = avg_tbl    # one row per fix
  )
}

raster_by_year <- list(
  "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
  "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
  "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
  "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
)

vars <- c("u10","v10","u100","v100","tp","t2m","msl","i10fg","tcc")

b_daily <- x$daily
res <- extract_avg_night_env_from_year_stacks(
  timestamps = b_daily,
  latitudes  = b_daily$lat,
  longitudes = b_daily$lon,
  IDs        = b_daily$individual_local_identifier,
  shift_hours = 0,
  raster_by_year = raster_by_year,
  var_names = vars
)

avg_night <- res$average_night_data
night_long <- res$night_data
