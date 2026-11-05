.calc_dist_bearing <- function(lon1, lat1, lon2, lat2) {
  if (requireNamespace("geosphere", quietly = TRUE)) {
    d <- geosphere::distHaversine(cbind(lon1, lat1), cbind(lon2, lat2))
    b <- geosphere::bearing(cbind(lon1, lat1), cbind(lon2, lat2))
    b <- (b + 360) %% 360
    return(list(dist_m = d, bearing_deg = b))
  } else {
    # fallback: approximate distance via sf in EPSG:3857
    p1 <- sf::st_as_sf(data.frame(lon = lon1, lat = lat1), coords = c("lon", "lat"), crs = 4326)
    p2 <- sf::st_as_sf(data.frame(lon = lon2, lat = lat2), coords = c("lon", "lat"), crs = 4326)
    p1m <- sf::st_transform(p1, 3857)
    p2m <- sf::st_transform(p2, 3857)
    d <- as.numeric(sf::st_distance(p1m, p2m, by_element = TRUE))
    # bearing fallback (spherical-ish)
    b <- atan2(lon2 - lon1, lat2 - lat1) * 180 / pi
    b <- (b + 360) %% 360
    return(list(dist_m = d, bearing_deg = b))
  }
}

mt_add_message_timediff <- function(x,
                                    dt_prev_s_col = "dt_prev_s",
                                    dt_prev_h_col = "dt_prev_h") {

  assert_that(move2::mt_is_time_ordered(x))

  time_col  <- move2::mt_time_column(x)
  track_col <- move2::mt_track_id_column(x)

  x %>%
    dplyr::group_by(!!sym(track_col)) %>%
    dplyr::mutate(
      !!dt_prev_s_col := as.numeric(difftime(!!sym(time_col), dplyr::lag(!!sym(time_col)), units = "secs")),
      !!dt_prev_h_col := !!sym(dt_prev_s_col) / 3600
    ) %>%
    dplyr::ungroup()
}

# Night that ends at each daily fix, labelled by the date of its evening
# (noon-to-noon bat day). Night fixes (sun below horizon at the fix) keep the
# bat day they fall in; daytime fixes take the night before their calendar
# date. With daily_method = "noon_roost" this is unique per track, so it is
# the key for joining night-level data (sensor summaries, night weather) to
# daily rows; calendar as.Date(timestamp) is not, since an afternoon day fix
# and that evening's night fix can share a date. Fixes without a usable
# position fall back to the bat day.
#' Night date for each daily fix
#'
#' Labels each daily fix with the date of the evening of the night that ends
#' at that fix (noon-to-noon "bat day"). Use it, not `as.Date(timestamp)`, to
#' join night-level data to daily rows.
#'
#' @param x A `move2` object of daily fixes.
#' @param tz Time zone for calendar dates.
#' @param day_anchor_hour Hour (local to `tz`) at which a bat day starts.
#' @return A `Date` vector, one per row of `x`.
#' @export
mt_daily_night_date <- function(x, tz = "UTC", day_anchor_hour = 12) {
  ts <- move2::mt_time(x)
  bat_day <- as.Date(ts - lubridate::hours(day_anchor_hour), tz = tz)
  x_ll <- x
  if (!sf::st_is_longlat(x_ll)) x_ll <- sf::st_transform(x_ll, 4326)
  xy <- suppressWarnings(sf::st_coordinates(sf::st_geometry(x_ll)))
  alt <- rep(NA_real_, length(ts))
  if (nrow(xy) == length(ts)) {
    alt <- suncalc::getSunlightPosition(
      data = data.frame(date = ts, lat = xy[, "Y"], lon = xy[, "X"]),
      keep = "altitude"
    )$altitude
  }
  out <- as.Date(ts, tz = tz) - 1
  use_bat_day <- is.na(alt) | alt < 0
  out[use_bat_day] <- bat_day[use_bat_day]
  out
}

#' Add night-level sensor summaries to daily fixes
#'
#' Summarises the sensor rows of a full multi-sensor `move2` object (VeDBA,
#' temperature, pressure, activity) over the night that precedes each daily
#' fix and joins them as `daily_*` columns (e.g. `daily_vedba_sum`,
#' `daily_temp_mean`, `daily_p_min`, `daily_alt_night_max_m`). NanoFox and
#' TinyFox columns are handled separately.
#'
#' @param b_all Full multi-sensor `move2` object.
#' @param b_daily Daily `move2` object (e.g. from [mt_thin_daily_solar_noon()]).
#' @param tz Time zone for day boundaries.
#' @param day_anchor_hour Hour at which a bat day starts.
#' @param night_key `"preceding_night"` (default, via [mt_daily_night_date()])
#'   or `"bat_day"` to key summaries to daily rows.
#' @param tag_type_col,model_col Columns with tag type and model.
#' @param skip_if_model,skip_uwasp If `skip_uwasp` is `TRUE` and any row's
#'   model is in `skip_if_model`, `b_daily` is returned unchanged.
#' @param nano_vedba_col,nano_temp_col,nano_pres_col NanoFox sensor columns.
#' @param nano_vedba_active_threshold,nano_vedba_flying_threshold NanoFox
#'   VeDBA thresholds for counting active and flying bursts.
#' @param tiny_act_col,tiny_pmin_col,tiny_tmax_col,tiny_tmin_col,tiny_vedba_col
#'   TinyFox 24-hour summary columns.
#' @param p0_hpa Sea-level reference pressure (hPa) for altitude conversion.
#' @return `b_daily` with added `daily_*` columns.
#' @export
mt_add_daily_sensor_metrics <- function(b_all,
                                        b_daily,
                                        tz = "UTC",
                                        day_anchor_hour = 12,
                                        night_key = c("preceding_night", "bat_day"),
                                        tag_type_col = "tag_type",
                                        model_col = "model",
                                        skip_if_model = c("uWasp", "uwasp"),
                                        skip_uwasp = TRUE,

                                        # nanofox
                                        nano_vedba_col = "vedba",
                                        nano_vedba_active_threshold = 1,
                                        nano_vedba_flying_threshold = 2,
                                        nano_temp_col  = "avg_temp",
                                        nano_pres_col  = "min_3h_pressure",

                                        # tinyfox
                                        tiny_act_col   = "tinyfox_activity_percent_last_24h",
                                        tiny_pmin_col  = "tinyfox_pressure_min_last_24h",
                                        tiny_tmax_col  = "tinyfox_temperature_max_last_24h",
                                        tiny_tmin_col  = "tinyfox_temperature_min_last_24h",
                                        tiny_vedba_col = "tinyfox_total_vedba",

                                        # altitude
                                        p0_hpa = 1013.25) {

  b_all   <- b_all[order(mt_track_id(b_all),   mt_time(b_all)), ]
  b_daily <- b_daily[order(mt_track_id(b_daily), mt_time(b_daily)), ]
  assert_that(move2::mt_is_time_ordered(b_all))
  assert_that(move2::mt_is_time_ordered(b_daily))

  time_col_all   <- move2::mt_time_column(b_all)
  track_col      <- move2::mt_track_id_column(b_all)
  time_col_daily <- move2::mt_time_column(b_daily)

  # --- optional: skip if uWasp present ---
  if (skip_uwasp && model_col %in% names(b_all)) {
    mods <- unique(tolower(as.character(b_all[[model_col]])))
    if (any(mods %in% tolower(skip_if_model))) return(b_daily)
  }

  get_bat_day <- function(ts) as.Date(ts - lubridate::hours(day_anchor_hour), tz = tz)

  # Which night's metrics a daily fix carries.
  #   "preceding_night" (default): mt_daily_night_date(), the night that ends
  #     at the fix (the flight behind its displacement from the previous
  #     daily fix).
  #   "bat_day": legacy get_bat_day(timestamp). Daytime fixes after
  #     day_anchor_hour then carry the FOLLOWING night, misaligned with the
  #     displacement into that fix.
  night_key <- match.arg(night_key)
  get_daily_night <- function(b) {
    if (night_key == "bat_day") return(get_bat_day(dplyr::pull(b, !!sym(time_col_daily))))
    mt_daily_night_date(b, tz = tz, day_anchor_hour = day_anchor_hour)
  }

  # tag types present
  tag_present <- character(0)
  if (tag_type_col %in% names(b_all)) {
    tag_present <- unique(tolower(na.omit(as.character(b_all[[tag_type_col]]))))
  }
  has_nano <- "nanofox" %in% tag_present
  has_tiny <- "tinyfox" %in% tag_present

  # ---------- full-res prep for QA + altitude + day/night ----------
  b_ll <- b_all
  if (!sf::st_is_longlat(b_ll)) b_ll <- sf::st_transform(b_ll, 4326)
  coords <- sf::st_coordinates(b_ll)
  lon <- coords[, 1]; lat <- coords[, 2]

  ts_all <- dplyr::pull(b_all, !!sym(time_col_all))
  bat_day_all <- get_bat_day(ts_all)

  # Non-location sensor rows (VeDBA, temp, pressure) have empty geometry of
  # their own -- st_coordinates() returns NaN for them, which propagates to
  # NA sunrise/sunset and leaves .is_day/.is_night NA for every such row
  # (silently zeroing out any day/night split keyed off them). Borrow the
  # nearest-in-time fix from the same track so sun-time can still be resolved.
  .track_all <- as.character(move2::mt_track_id(b_all))
  .fill_tbl <- tibble::tibble(.idx = seq_along(ts_all), .trk = .track_all, .ts = ts_all, lon = lon, lat = lat) %>%
    dplyr::arrange(.trk, .ts) %>%
    dplyr::group_by(.trk) %>%
    tidyr::fill(lon, lat, .direction = "downup") %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.idx)
  lon <- .fill_tbl$lon
  lat <- .fill_tbl$lat

  sun <- suncalc::getSunlightTimes(
    data = data.frame(date = bat_day_all, lat = lat, lon = lon),
    keep = c("sunrise", "sunset"),
    tz = tz
  )

  b2 <- b_all %>%
    dplyr::mutate(
      .bat_day = bat_day_all,
      .sunrise = sun$sunrise,
      .sunset  = sun$sunset
    ) %>%
    dplyr::group_by(!!sym(track_col)) %>%
    dplyr::arrange(!!sym(time_col_all), .by_group = TRUE) %>%
    dplyr::mutate(
      .dt_prev_h = as.numeric(difftime(!!sym(time_col_all), dplyr::lag(!!sym(time_col_all)), units = "hours"))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      .is_night = ifelse(is.na(.sunrise) | is.na(.sunset), NA,
                         (!!sym(time_col_all) >= .sunset) | (!!sym(time_col_all) < .sunrise)),
      .is_day   = ifelse(is.na(.sunrise) | is.na(.sunset), NA,
                         (!!sym(time_col_all) >= .sunrise) & (!!sym(time_col_all) < .sunset))
    )

  # altitude per row (safe if columns missing)
  # Fallback: if primary pressure column absent try alternatives (30Days NanoFox
  # may land pressure in min_pressure or barometric_pressure instead of min_3h_pressure).
  if (!nano_pres_col %in% names(b2)) {
    for (.alt_pres in c("min_pressure", "barometric_pressure")) {
      if (.alt_pres %in% names(b2)) { nano_pres_col <- .alt_pres; break }
    }
  }
  nano_ok <- nano_pres_col %in% names(b2)
  # Strip units from pressure column so downstream min() and dplyr::if_else() get plain double.
  if (nano_ok && inherits(b2[[nano_pres_col]], "units")) {
    b2[[nano_pres_col]] <- as.numeric(b2[[nano_pres_col]])
  }
  tiny_ok <- tiny_pmin_col %in% names(b2)
  nano_p  <- if (nano_ok) as.numeric(b2[[nano_pres_col]]) else rep(NA_real_, nrow(b2))
  tiny_p  <- if (tiny_ok) as.numeric(b2[[tiny_pmin_col]]) else rep(NA_real_, nrow(b2))
  tag_vec <- if (tag_type_col %in% names(b2)) tolower(as.character(b2[[tag_type_col]])) else rep(NA_character_, nrow(b2))

  pressure_used <- rep(NA_real_, nrow(b2))
  pressure_used[which(tag_vec == "nanofox")] <- nano_p[which(tag_vec == "nanofox")]
  pressure_used[which(tag_vec == "tinyfox")] <- tiny_p[which(tag_vec == "tinyfox")]

  b2 <- b2 %>%
    dplyr::mutate(
      pressure_hpa_used = pressure_used,
      altitude_m = pressure_to_altitude_m(pressure_hpa_used, p0_hpa = p0_hpa)
    )
  b2$altitude_m[which(b2$altitude_m > 7000)] <- NA

  # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  # CRITICAL: DO ALL SUMMARIES ON ATTRIBUTE TABLE ONLY (NO GEOMETRY)
  # This prevents sf::summarise() from creating MULTIPOINT geometries.
  b2_attr <- sf::st_drop_geometry(b2)
  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

  # Strip units class from any temperature/pressure/vedba columns so that all
  # groups return plain <double> from summarise. Without this, bind_rows across
  # studies produces a mix of units and double in the same column → dplyr type
  # error, and comparisons like `vedba > threshold` error outright on a units
  # object compared against a plain number.
  for (.strip_col in intersect(
    c("temperature_min", "temperature_max", nano_temp_col, "external_temperature",
      nano_pres_col, nano_vedba_col),
    names(b2_attr)
  )) {
    if (inherits(b2_attr[[.strip_col]], "units"))
      b2_attr[[.strip_col]] <- as.numeric(b2_attr[[.strip_col]])
  }

  # ---------- NanoFox summaries (attributes only) ----------
  nano_daily <- NULL
  if (has_nano) {
    # Precompute column-presence flags outside summarise: names(.) inside a
    # dplyr::summarise quosure is unreliable (. may not resolve to the grouped
    # data), so scalar variables captured in the closure are used instead.
    .has_nano_v   <- nano_vedba_col %in% names(b2_attr)
    .has_tmin_col <- "temperature_min" %in% names(b2_attr)
    .has_tmax_col <- "temperature_max" %in% names(b2_attr)
    .has_avg_temp <- nano_temp_col %in% names(b2_attr)
    .has_pres     <- nano_pres_col %in% names(b2_attr)

    nano_daily <- b2_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "nanofox") %>%
      dplyr::group_by(.data[[track_col]], .bat_day) %>%
      dplyr::summarise(
        daily_n_msg      = dplyr::n(),
        daily_gap_h_mean = mean(.dt_prev_h, na.rm = TRUE),
        daily_gap_h_max  = suppressWarnings(max(.dt_prev_h, na.rm = TRUE)),

        daily_vedba_sum   = if (.has_nano_v) sum(.data[[nano_vedba_col]], na.rm = TRUE) else NA_real_,
        daily_vedba_sum_n = if (.has_nano_v) sum(!is.na(.data[[nano_vedba_col]])) else 0L,
        # Mean, not sum: comparable scale to per-transmission vedba (~0-30) for
        # overlaying on the raw VeDBA panel. daily_vedba_sum accumulates over
        # every transmission in the day (100s of windows) and reaches values
        # 100x the raw signal, which blew up the scantrack.R y-axis.
        daily_vedba_mean  = if (.has_nano_v) mean(.data[[nano_vedba_col]], na.rm = TRUE) else NA_real_,
        # Count of windows above the "active" threshold: a duty-cycle-style
        # activity count that isn't pulled around by a handful of extreme
        # bursts the way a mean is, and unlike daily_vedba_sum doesn't
        # conflate "more active" with "more transmissions received" on days
        # with denser reception.
        daily_vedba_active_n = if (.has_nano_v)
          sum(.data[[nano_vedba_col]] > nano_vedba_active_threshold, na.rm = TRUE)
          else NA_integer_,

        # Median is far more robust than the mean when reception is patchy:
        # a few flight bursts don't get washed out by many quiet/roost windows
        # on days with high message coverage.
        daily_vedba_median = if (.has_nano_v)
          suppressWarnings(stats::median(.data[[nano_vedba_col]], na.rm = TRUE))
          else NA_real_,

        # Number of Vedba windows above a (usually lower) flying threshold.
        # Default 0 counts any non-zero burst as flight; raise it if you only
        # want to count stronger bursts.
        daily_vedba_flying_n = if (.has_nano_v)
          sum(.data[[nano_vedba_col]] > nano_vedba_flying_threshold, na.rm = TRUE)
          else NA_integer_,

        # Day/night split: NanoFox migration nights show up as a burst of
        # activity against a quiet daytime roost baseline, which a whole-day
        # mean/sum washes out. Same sum-vs-mean/median scale caveat as above
        # applies to each half independently.
        daily_vedba_day_sum    = if (.has_nano_v) sum(.data[[nano_vedba_col]][.is_day %in% TRUE], na.rm = TRUE) else NA_real_,
        daily_vedba_day_mean   = if (.has_nano_v) mean(.data[[nano_vedba_col]][.is_day %in% TRUE], na.rm = TRUE) else NA_real_,
        daily_vedba_day_median = if (.has_nano_v)
          suppressWarnings(stats::median(.data[[nano_vedba_col]][.is_day %in% TRUE], na.rm = TRUE))
          else NA_real_,
        daily_vedba_day_n      = if (.has_nano_v) sum(!is.na(.data[[nano_vedba_col]][.is_day %in% TRUE])) else 0L,
        daily_vedba_day_flying_n = if (.has_nano_v)
          sum(.data[[nano_vedba_col]][.is_day %in% TRUE] > nano_vedba_flying_threshold, na.rm = TRUE)
          else NA_integer_,
        daily_vedba_night_sum    = if (.has_nano_v) sum(.data[[nano_vedba_col]][.is_night %in% TRUE], na.rm = TRUE) else NA_real_,
        daily_vedba_night_mean   = if (.has_nano_v) mean(.data[[nano_vedba_col]][.is_night %in% TRUE], na.rm = TRUE) else NA_real_,
        daily_vedba_night_median = if (.has_nano_v)
          suppressWarnings(stats::median(.data[[nano_vedba_col]][.is_night %in% TRUE], na.rm = TRUE))
          else NA_real_,
        daily_vedba_night_n      = if (.has_nano_v) sum(!is.na(.data[[nano_vedba_col]][.is_night %in% TRUE])) else 0L,
        daily_vedba_night_flying_n = if (.has_nano_v)
          sum(.data[[nano_vedba_col]][.is_night %in% TRUE] > nano_vedba_flying_threshold, na.rm = TRUE)
          else NA_integer_,

        # temperature_min / temperature_max exist on 30DaysFineScalePressure;
        # avg_temp is the fallback for standard 30Days firmware.
        daily_temp_min = suppressWarnings(min(
          if (.has_tmin_col && any(!is.na(.data[["temperature_min"]])))
            .data[["temperature_min"]]
          else if (.has_avg_temp) .data[[nano_temp_col]]
          else NA_real_,
          na.rm = TRUE
        )),
        daily_temp_max = suppressWarnings(max(
          if (.has_tmax_col && any(!is.na(.data[["temperature_max"]])))
            .data[["temperature_max"]]
          else if (.has_avg_temp) .data[[nano_temp_col]]
          else NA_real_,
          na.rm = TRUE
        )),
        daily_temp_n = sum(!is.na(
          if (.has_tmin_col) .data[["temperature_min"]]
          else if (.has_avg_temp) .data[[nano_temp_col]]
          else NA_real_
        )),

        daily_p_min   = if (.has_pres) suppressWarnings(min(.data[[nano_pres_col]], na.rm = TRUE)) else NA_real_,
        daily_p_min_n = if (.has_pres) sum(!is.na(.data[[nano_pres_col]])) else 0L,

        daily_alt_day_mean_m  = mean(altitude_m[.is_day %in% TRUE], na.rm = TRUE),
        daily_alt_day_mean_n  = sum(is.finite(altitude_m) & (.is_day %in% TRUE)),
        daily_alt_night_max_m = suppressWarnings(max(altitude_m[.is_night %in% TRUE], na.rm = TRUE)),
        daily_alt_night_max_n = sum(is.finite(altitude_m) & (.is_night %in% TRUE)),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        daily_gap_h_max       = dplyr::if_else(is.infinite(daily_gap_h_max), NA_real_, daily_gap_h_max),
        daily_temp_min        = dplyr::if_else(is.infinite(daily_temp_min), NA_real_, daily_temp_min),
        daily_temp_max        = dplyr::if_else(is.infinite(daily_temp_max), NA_real_, daily_temp_max),
        daily_p_min           = dplyr::if_else(is.infinite(daily_p_min), NA_real_, daily_p_min),
        daily_alt_night_max_m = dplyr::if_else(is.infinite(daily_alt_night_max_m), NA_real_, daily_alt_night_max_m)
      )
  }

  # ---------- TinyFox summaries (attributes only) ----------
  tiny_daily <- NULL
  if (has_tiny) {
    tiny_gap_alt <- b2_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "tinyfox") %>%
      dplyr::group_by(.data[[track_col]], .bat_day) %>%
      dplyr::summarise(
        daily_n_msg      = dplyr::n(),
        daily_gap_h_mean = mean(.dt_prev_h, na.rm = TRUE),
        daily_gap_h_max  = suppressWarnings(max(.dt_prev_h, na.rm = TRUE)),

        daily_alt_day_mean_m  = mean(altitude_m[.is_day %in% TRUE], na.rm = TRUE),
        daily_alt_day_mean_n  = sum(is.finite(altitude_m) & (.is_day %in% TRUE)),
        daily_alt_night_max_m = suppressWarnings(max(altitude_m[.is_night %in% TRUE], na.rm = TRUE)),
        daily_alt_night_max_n = sum(is.finite(altitude_m) & (.is_night %in% TRUE)),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        daily_gap_h_max       = dplyr::if_else(is.infinite(daily_gap_h_max), NA_real_, daily_gap_h_max),
        daily_alt_night_max_m = dplyr::if_else(is.infinite(daily_alt_night_max_m), NA_real_, daily_alt_night_max_m)
      )

    # attribute-only daily table
    b_daily_attr <- b_daily |>
      sf::st_drop_geometry() |>
      tibble::as_tibble()

    b_daily_attr$.bat_day <- get_daily_night(b_daily)

    # Precompute column flags (same reason as NanoFox: names(.) in mutate/transmute
    # quosures may not resolve reliably across all dplyr versions).
    .has_tiny_v    <- tiny_vedba_col %in% names(b_daily_attr)
    .has_tiny_act  <- tiny_act_col   %in% names(b_daily_attr)
    .has_tiny_pmin <- tiny_pmin_col  %in% names(b_daily_attr)
    .has_tiny_tmax <- tiny_tmax_col  %in% names(b_daily_attr)
    .has_tiny_tmin <- tiny_tmin_col  %in% names(b_daily_attr)

    tiny_from_noon <- b_daily_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "tinyfox") %>%
      dplyr::arrange(.data[[track_col]], .data[[time_col_daily]]) %>%
      dplyr::group_by(.data[[track_col]]) %>%
      dplyr::mutate(
        .tiny_v = if (.has_tiny_v) as.numeric(.data[[tiny_vedba_col]]) else NA_real_,
        .dt_h   = as.numeric(difftime(.data[[time_col_daily]], dplyr::lag(.data[[time_col_daily]]), units = "hours")),
        .dvedba = .tiny_v - dplyr::lag(.tiny_v),
        .dvedba = dplyr::if_else(is.finite(.dvedba) & .dvedba >= 0, .dvedba, NA_real_),
        daily_total_vedba_rate_h = dplyr::if_else(is.finite(.dt_h) & .dt_h > 0, .dvedba / .dt_h, NA_real_),
        daily_total_vedba_24h    = daily_total_vedba_rate_h * 24,
        daily_total_vedba_24h_n  = as.integer(!is.na(daily_total_vedba_24h))
      ) %>%
      dplyr::ungroup() %>%
      dplyr::transmute(
        !!rlang::sym(track_col), .bat_day,

        daily_activity_percent_24h   = if (.has_tiny_act)  .data[[tiny_act_col]]  else NA_real_,
        daily_activity_percent_24h_n = as.integer(!is.na(daily_activity_percent_24h)),

        daily_pmin_24h   = if (.has_tiny_pmin) .data[[tiny_pmin_col]] else NA_real_,
        daily_pmin_24h_n = as.integer(!is.na(daily_pmin_24h)),

        daily_tmax_24h   = if (.has_tiny_tmax) .data[[tiny_tmax_col]] else NA_real_,
        daily_tmax_24h_n = as.integer(!is.na(daily_tmax_24h)),

        daily_tmin_24h   = if (.has_tiny_tmin) .data[[tiny_tmin_col]] else NA_real_,
        daily_tmin_24h_n = as.integer(!is.na(daily_tmin_24h)),

        daily_total_vedba_rate_h,
        daily_total_vedba_24h,
        daily_total_vedba_24h_n
      )

    tiny_daily <- dplyr::left_join(tiny_gap_alt, tiny_from_noon, by = c(track_col, ".bat_day"))
  }

  # ---------- uWasp: external_temperature daily summaries ----------
  uwasp_daily <- NULL
  if ("uwasp" %in% tolower(unique(b2_attr[[tag_type_col]])) &&
      "external_temperature" %in% names(b2_attr)) {
    uwasp_daily <- b2_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "uwasp") %>%
      dplyr::group_by(.data[[track_col]], .bat_day) %>%
      dplyr::summarise(
        daily_n_msg           = dplyr::n(),
        daily_gap_h_mean      = mean(.dt_prev_h, na.rm = TRUE),
        daily_gap_h_max       = suppressWarnings(max(.dt_prev_h, na.rm = TRUE)),
        daily_temp_min        = suppressWarnings(min(as.numeric(external_temperature), na.rm = TRUE)),
        daily_temp_max        = suppressWarnings(max(as.numeric(external_temperature), na.rm = TRUE)),
        daily_temp_mean       = mean(as.numeric(external_temperature), na.rm = TRUE),
        daily_temp_n          = sum(!is.na(external_temperature)),
        daily_alt_day_mean_m  = mean(altitude_m[.is_day %in% TRUE], na.rm = TRUE),
        daily_alt_day_mean_n  = sum(is.finite(altitude_m) & (.is_day %in% TRUE)),
        daily_alt_night_max_m = suppressWarnings(max(altitude_m[.is_night %in% TRUE], na.rm = TRUE)),
        daily_alt_night_max_n = sum(is.finite(altitude_m) & (.is_night %in% TRUE)),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        daily_gap_h_max       = dplyr::if_else(is.infinite(daily_gap_h_max), NA_real_, daily_gap_h_max),
        daily_temp_min        = dplyr::if_else(is.infinite(daily_temp_min), NA_real_, daily_temp_min),
        daily_temp_max        = dplyr::if_else(is.infinite(daily_temp_max), NA_real_, daily_temp_max),
        daily_alt_night_max_m = dplyr::if_else(is.infinite(daily_alt_night_max_m), NA_real_, daily_alt_night_max_m)
      )
  }

  # ---------- combine summaries (PLAIN TIBBLE ONLY) ----------
  sensor_daily <- dplyr::bind_rows(
    if (!is.null(nano_daily)) nano_daily,
    if (!is.null(tiny_daily)) tiny_daily,
    if (!is.null(uwasp_daily)) uwasp_daily
  )

  if (nrow(sensor_daily) == 0) return(b_daily)

  # ---------- SAFE JOIN BACK ONTO move2 (no geometry touched) ----------
  key <- b_daily |>
    sf::st_drop_geometry() |>
    tibble::as_tibble()

  key$.row_id  <- seq_len(nrow(key))
  key$.bat_day <- get_daily_night(b_daily)

  sensor_daily_uniq <- sensor_daily |>
    dplyr::group_by(.data[[track_col]], .bat_day) |>
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x)), .groups = "drop")

  # many-to-one join: b_daily rows keyed to the same night (see
  # get_daily_night()) both receive that night's metrics; with
  # daily_method = "noon_roost" each night has at most one daily row.
  joined <- dplyr::left_join(
    key,
    sensor_daily_uniq,
    by = c(track_col, ".bat_day")
  )

  new_cols <- setdiff(names(sensor_daily_uniq), c(track_col, ".bat_day"))

  out <- b_daily
  for (nm in new_cols) out[[nm]] <- joined[[nm]]

  # geometry stays exactly as in b_daily (POINT/empty), never MULTIPOINT
  out
}

# b_daily <- mt_thin_daily_solar_noon(b_full, tz = "UTC")
# b_daily2 <- mt_add_daily_sensor_metrics(b_all = b_full, b_daily = b_daily, tz = "UTC")
# b_daily2$geometry
# b_daily2 %>% names()
# ggplot(b_daily2)+
#   geom_path(aes(timestamp, daily_total_vedba_rate_h))+
#   geom_path(aes(timestamp, daily_vedba_sum), col = 2)
#   ylim(c(0, 3000))
