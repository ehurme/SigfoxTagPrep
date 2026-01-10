.calc_dist_bearing <- function(lon1, lat1, lon2, lat2) {
  if (requireNamespace("geosphere", quietly = TRUE)) {
    d <- geosphere::distHaversine(cbind(lon1, lat1), cbind(lon2, lat2))
    b <- geosphere::bearing(cbind(lon1, lat1), cbind(lon2, lat2))
    b <- (b + 360) %% 360
    return(list(dist_m = d, bearing_deg = b))
  } else {
    # fallback: approximate distance via sf in EPSG:3857
    require(sf)
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
  require(move2)
  require(dplyr)
  require(rlang)
  require(assertthat)

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

mt_add_daily_sensor_metrics <- function(b_all,
                                        b_daily,
                                        tz = "UTC",
                                        day_anchor_hour = 12,
                                        tag_type_col = "tag_type",
                                        model_col = "model",
                                        skip_if_model = c("uWasp", "uwasp"),
                                        skip_uwasp = TRUE,

                                        # nanofox
                                        nano_vedba_col = "vedba_sum",
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
  require(move2)
  require(dplyr)
  require(lubridate)
  require(sf)
  require(suncalc)
  require(rlang)
  require(assertthat)
  require(tibble)

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
  nano_ok <- nano_pres_col %in% names(b2)
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

  # ---------- NanoFox summaries (attributes only) ----------
  nano_daily <- NULL
  if (has_nano) {
    nano_daily <- b2_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "nanofox") %>%
      dplyr::group_by(.data[[track_col]], .bat_day) %>%
      dplyr::summarise(
        daily_n_msg      = dplyr::n(),
        daily_gap_h_mean = mean(.dt_prev_h, na.rm = TRUE),
        daily_gap_h_max  = suppressWarnings(max(.dt_prev_h, na.rm = TRUE)),

        daily_vedba_sum   = if (nano_vedba_col %in% names(.)) sum(.data[[nano_vedba_col]], na.rm = TRUE) else NA_real_,
        daily_vedba_sum_n = if (nano_vedba_col %in% names(.)) sum(!is.na(.data[[nano_vedba_col]])) else 0L,

        daily_temp_min   = if (nano_temp_col %in% names(.)) min(.data[[nano_temp_col]], na.rm = TRUE) else NA_real_,
        daily_temp_max   = if (nano_temp_col %in% names(.)) max(.data[[nano_temp_col]], na.rm = TRUE) else NA_real_,
        daily_temp_mean   = if (nano_temp_col %in% names(.)) mean(.data[[nano_temp_col]], na.rm = TRUE) else NA_real_,
        daily_temp_n      = if (nano_temp_col %in% names(.)) sum(!is.na(.data[[nano_temp_col]])) else 0L,

        daily_p_min       = if (nano_pres_col %in% names(.)) suppressWarnings(min(.data[[nano_pres_col]], na.rm = TRUE)) else NA_real_,
        daily_p_min_n     = if (nano_pres_col %in% names(.)) sum(!is.na(.data[[nano_pres_col]])) else 0L,

        daily_alt_day_mean_m  = mean(altitude_m[.is_day %in% TRUE], na.rm = TRUE),
        daily_alt_day_mean_n  = sum(is.finite(altitude_m) & (.is_day %in% TRUE)),
        daily_alt_night_max_m = suppressWarnings(max(altitude_m[.is_night %in% TRUE], na.rm = TRUE)),
        daily_alt_night_max_n = sum(is.finite(altitude_m) & (.is_night %in% TRUE)),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        daily_gap_h_max       = dplyr::if_else(is.infinite(daily_gap_h_max), NA_real_, daily_gap_h_max),
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

    b_daily_attr$.bat_day <- get_bat_day(b_daily_attr[[time_col_daily]])

    tiny_from_noon <- b_daily_attr %>%
      dplyr::filter(tolower(.data[[tag_type_col]]) == "tinyfox") %>%
      dplyr::arrange(.data[[track_col]], .data[[time_col_daily]]) %>%
      dplyr::group_by(.data[[track_col]]) %>%
      dplyr::mutate(
        .tiny_v = if (tiny_vedba_col %in% names(.)) as.numeric(.data[[tiny_vedba_col]]) else NA_real_,
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

        daily_activity_percent_24h   = if (tiny_act_col %in% names(.)) .data[[tiny_act_col]] else NA_real_,
        daily_activity_percent_24h_n = as.integer(!is.na(daily_activity_percent_24h)),

        daily_pmin_24h   = if (tiny_pmin_col %in% names(.)) .data[[tiny_pmin_col]] else NA_real_,
        daily_pmin_24h_n = as.integer(!is.na(daily_pmin_24h)),

        daily_tmax_24h   = if (tiny_tmax_col %in% names(.)) .data[[tiny_tmax_col]] else NA_real_,
        daily_tmax_24h_n = as.integer(!is.na(daily_tmax_24h)),

        daily_tmin_24h   = if (tiny_tmin_col %in% names(.)) .data[[tiny_tmin_col]] else NA_real_,
        daily_tmin_24h_n = as.integer(!is.na(daily_tmin_24h)),

        daily_total_vedba_rate_h,
        daily_total_vedba_24h,
        daily_total_vedba_24h_n
      )

    tiny_daily <- dplyr::left_join(tiny_gap_alt, tiny_from_noon, by = c(track_col, ".bat_day"))
  }

  # ---------- combine summaries (PLAIN TIBBLE ONLY) ----------
  sensor_daily <- dplyr::bind_rows(
    if (!is.null(nano_daily)) nano_daily,
    if (!is.null(tiny_daily)) tiny_daily
  )

  if (nrow(sensor_daily) == 0) return(b_daily)

  # ---------- SAFE JOIN BACK ONTO move2 (no geometry touched) ----------
  key <- b_daily |>
    sf::st_drop_geometry() |>
    tibble::as_tibble()

  key$.row_id  <- seq_len(nrow(key))
  key$.bat_day <- get_bat_day(key[[time_col_daily]])

  key_unique <- key |>
    dplyr::arrange(.data[[track_col]], .data[[time_col_daily]], .row_id) |>
    dplyr::group_by(.data[[track_col]], .bat_day) |>
    dplyr::slice(1) |>
    dplyr::ungroup()

  sensor_daily_uniq <- sensor_daily |>
    dplyr::group_by(.data[[track_col]], .bat_day) |>
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x)), .groups = "drop")

  joined <- dplyr::left_join(
    key_unique,
    sensor_daily_uniq,
    by = c(track_col, ".bat_day"),
    relationship = "one-to-one"
  )

  new_cols <- setdiff(names(sensor_daily_uniq), c(track_col, ".bat_day"))

  metrics_full <- tibble::tibble(.row_id = key$.row_id)
  for (nm in new_cols) metrics_full[[nm]] <- NA
  metrics_full[match(joined$.row_id, metrics_full$.row_id), new_cols] <- joined[, new_cols, drop = FALSE]

  out <- b_daily
  for (nm in new_cols) out[[nm]] <- metrics_full[[nm]]

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
