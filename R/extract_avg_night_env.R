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

    # ---- define sub_idx FIRST (rows in long_tbl for this year) ----
    sub_idx <- which(long_tbl$year == yr)
    if (length(sub_idx) == 0) next

    # --- GRIB FIX: layers are (time x variable), time repeats for each variable ---
    rt <- as.POSIXct(terra::time(r), tz = tz)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no time vector.")

    n_var <- length(var_names)
    if (terra::nlyr(r) %% n_var != 0) {
      stop("nlyr(r) (", terra::nlyr(r), ") is not divisible by n_var (", n_var,
           "). Check var_names ordering / raster contents.")
    }

    rt_unique <- unique(rt)
    rt_unique_round <- lubridate::round_date(rt_unique, unit = "hour")

    # match each night_hour to *hour index* (not layer index)
    t_round <- as.POSIXct(long_tbl$night_hour[sub_idx], tz = tz)
    t_round <- lubridate::round_date(t_round, unit = "hour")

    hour_idx <- match(as.numeric(t_round), as.numeric(rt_unique_round))

    ok <- which(!is.na(hour_idx))
    if (length(ok) == 0) {
      if (verbose) message("No matching raster hours for year ", yr_chr)
      next
    }

    sub_ok    <- sub_idx[ok]
    hour_idx  <- hour_idx[ok]
    uniq_hours <- sort(unique(hour_idx))

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
      message("Year ", yr_chr, ": rows=", length(sub_ok), ", unique hours=", length(uniq_hours),
              ", vars/hour=", n_var)
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_hours), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    for (ii in seq_along(uniq_hours)) {
      if (verbose) utils::setTxtProgressBar(pb, ii)

      hidx <- uniq_hours[ii]
      p_idx <- which(hour_idx == hidx)
      if (length(p_idx) == 0) next

      # hour index -> layer indices for all variables
      layer_idx <- ((hidx - 1L) * n_var + 1L) : (hidx * n_var)

      rr <- r[[layer_idx]]
      names(rr) <- var_names

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



consolidate_avg_night <- function(avg_night, row_id = ".row_id") {
  require(dplyr)

  stopifnot(row_id %in% names(avg_night))

  avg_night %>%
    # drop NA / blank row_id
    dplyr::filter(!is.na(.data[[row_id]]), .data[[row_id]] != "") %>%
    # ensure numeric row_id if it came in as character
    dplyr::mutate(!!row_id := as.integer(.data[[row_id]])) %>%
    # consolidate duplicate rows per row_id by taking first non-NA value in each column
    dplyr::group_by(.data[[row_id]]) %>%
    dplyr::summarise(
      dplyr::across(
        dplyr::everything(),
        ~ {
          x <- .x
          x <- x[!is.na(x)]
          if (length(x) == 0) NA else x[[1]]
        }
      ),
      .groups = "drop"
    )
}

add_avg_night_to_move2 <- function(m, avg_night_data, row_id = ".row_id") {
  require(dplyr)
  require(sf)
  stopifnot(inherits(m, "move2"))
  stopifnot(row_id %in% names(avg_night_data))

  attr_df <- sf::st_drop_geometry(m)

  if (!(row_id %in% names(attr_df))) {
    attr_df[[row_id]] <- seq_len(nrow(attr_df))
  }

  # IMPORTANT: only join the *new* env cols (+ row_id), avoid duplicating timestamp/ID/etc
  env_cols <- setdiff(names(avg_night_data), row_id)
  # remove extra timestamps
  env_cols <- env_cols[which(!grepl(pattern = "timestamp.", env_cols))]
  # remove row_id NA
  avg_night_data <- avg_night_data[!is.na(avg_night_data$.row_id),]

  avg_night_slim <- avg_night_data %>% dplyr::select(dplyr::all_of(c(row_id, env_cols)))

  attr_df2 <- attr_df %>%
    dplyr::left_join(avg_night_slim, by = row_id)

  m_out <- m
  geom_col <- attr(m_out, "sf_column")
  for (nm in setdiff(names(attr_df2), geom_col)) {
    m_out[[nm]] <- attr_df2[[nm]]
  }

  m_out
}





# b_daily$individual_local_identifier %>% table()
# temp <- b_daily %>% filter(individual_local_identifier == "Nnoc24_swiss_133_120CC37")
# for(day in days){
#   res <- extract_avg_night_env_from_year_stacks(
#     timestamps = temp$timestamp,
#     latitudes  = temp$lat,
#     longitudes = temp$lon,
#     IDs        = temp$individual_local_identifier,
#     shift_hours = day*24,
#     raster_by_year = raster_by_year,
#     var_names = vars
#   )
#   temp <- add_avg_night_to_move2(
#     m = temp,
#     avg_night_data = res$average_night_data
#   )
# }
