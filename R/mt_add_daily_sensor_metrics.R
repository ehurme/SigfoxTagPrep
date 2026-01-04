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


mt_add_daily_sensor_metrics_batnight <- function(b_all,          # full-res move2
                                                 b_daily,        # noon-thinned move2
                                                 tz = "UTC",
                                                 day_anchor_hour = 12,  # noon->noon groups one full night
                                                 tag_type_col = "tag_type",

                                                 # NanoFox columns (3h window per row)
                                                 nano_vedba_col = "vedba_sum",
                                                 nano_temp_col  = "avg_temp",
                                                 nano_pres_col  = "min_3h_pressure",

                                                 # TinyFox columns (already last-24h / daily-like)
                                                 tiny_act_col   = "tinyfox_activity_percent_last_24h",
                                                 tiny_pmin_col  = "tinyfox_pressure_min_last_24h",
                                                 tiny_tmax_col  = "tinyfox_temperature_max_last_24h",
                                                 tiny_tmin_col  = "tinyfox_temperature_min_last_24h",
                                                 tiny_vedba_col = "tinyfox_total_vedba",

                                                 # Altitude conversion
                                                 p0_hpa = 1013.25,          # optionally replace with local/regional reference
                                                 out_prefix = "daily_") {
  require(move2)
  require(dplyr)
  require(lubridate)
  require(rlang)
  require(assertthat)
  require(sf)
  require(suncalc)

  assert_that(move2::mt_is_time_ordered(b_all))

  time_col  <- move2::mt_time_column(b_all)
  track_col <- move2::mt_track_id_column(b_all)

  # ---------- helper: bat-day key (noon->noon) ----------
  get_bat_day <- function(ts) {
    as.Date(ts - lubridate::hours(day_anchor_hour), tz = tz)
  }

  # ---------- prep full-res with coords + solar times ----------
  b_ll <- b_all
  if (!sf::st_is_longlat(b_ll)) b_ll <- sf::st_transform(b_ll, 4326)
  coords <- sf::st_coordinates(b_ll)
  lon <- coords[, 1]
  lat <- coords[, 2]

  ts <- dplyr::pull(b_all, !!sym(time_col))
  bat_day <- get_bat_day(ts)

  # sunrise/sunset for each fix at its location for that bat_day
  sun <- suncalc::getSunlightTimes(
    date = bat_day, lat = lat, lon = lon,
    keep = c("sunrise", "sunset"),
    tz = tz
  )

  b2 <- b_all %>%
    dplyr::mutate(
      .bat_day = bat_day,
      .sunrise = sun$sunrise,
      .sunset  = sun$sunset
    ) %>%
    mt_add_message_timediff(dt_prev_s_col = ".dt_prev_s", dt_prev_h_col = ".dt_prev_h") %>%
    dplyr::mutate(
      .is_night = ifelse(is.na(.sunrise) | is.na(.sunset), NA, (!!sym(time_col) >= .sunset) | (!!sym(time_col) < .sunrise)),
      .is_day   = ifelse(is.na(.sunrise) | is.na(.sunset), NA, (!!sym(time_col) >= .sunrise) & (!!sym(time_col) < .sunset))
    )

  # ---------- altitude per row (tag-type-specific pressure source) ----------
  b2 <- b2 %>%
    dplyr::mutate(
      .pressure_hpa_for_alt = dplyr::case_when(
        tolower(!!sym(tag_type_col)) == "nanofox" ~ as.numeric(.data[[nano_pres_col]]),
        tolower(!!sym(tag_type_col)) == "tinyfox" ~ as.numeric(.data[[tiny_pmin_col]]),
        TRUE ~ NA_real_
      ),
      .alt_m = pressure_to_altitude_m(.pressure_hpa_for_alt, p0_hpa = p0_hpa)
    )

  # ---------- NanoFox daily summaries (counts included) ----------
  nano_daily <- b2 %>%
    dplyr::filter(tolower(!!sym(tag_type_col)) == "nanofox") %>%
    dplyr::group_by(!!sym(track_col), .bat_day) %>%
    dplyr::summarise(
      # message QA
      !!paste0(out_prefix, "n_msg")       := dplyr::n(),
      !!paste0(out_prefix, "gap_h_mean")  := mean(.dt_prev_h, na.rm = TRUE),
      !!paste0(out_prefix, "gap_h_max")   := max(.dt_prev_h,  na.rm = TRUE),

      # VEDBA (sum across the bat-day)
      !!paste0(out_prefix, "vedba_sum")   := sum(.data[[nano_vedba_col]], na.rm = TRUE),
      !!paste0(out_prefix, "vedba_sum_n") := sum(!is.na(.data[[nano_vedba_col]])),

      # Temperature (mean/min/max + counts)
      !!paste0(out_prefix, "temp_mean")   := mean(.data[[nano_temp_col]], na.rm = TRUE),
      !!paste0(out_prefix, "temp_min")    := suppressWarnings(min(.data[[nano_temp_col]], na.rm = TRUE)),
      !!paste0(out_prefix, "temp_max")    := suppressWarnings(max(.data[[nano_temp_col]], na.rm = TRUE)),
      !!paste0(out_prefix, "temp_n")      := sum(!is.na(.data[[nano_temp_col]])),

      # Pressure minimum (and count)
      !!paste0(out_prefix, "p_min")       := suppressWarnings(min(.data[[nano_pres_col]], na.rm = TRUE)),
      !!paste0(out_prefix, "p_min_n")     := sum(!is.na(.data[[nano_pres_col]])),

      # Altitude summaries (day/night) + counts
      !!paste0(out_prefix, "alt_day_mean_m")   := mean(.alt_m[.is_day %in% TRUE], na.rm = TRUE),
      !!paste0(out_prefix, "alt_day_mean_n")   := sum(is.finite(.alt_m) & (.is_day %in% TRUE)),
      !!paste0(out_prefix, "alt_night_max_m")  := suppressWarnings(max(.alt_m[.is_night %in% TRUE], na.rm = TRUE)),
      !!paste0(out_prefix, "alt_night_max_n")  := sum(is.finite(.alt_m) & (.is_night %in% TRUE)),

      .groups = "drop"
    ) %>%
    # Fix min/max returning Inf/-Inf when all missing
    dplyr::mutate(
      dplyr::across(
        dplyr::matches(paste0("^", out_prefix, "(temp_min|temp_max|p_min|alt_night_max_m)$")),
        ~ dplyr::if_else(is.infinite(.x), NA_real_, .x)
      )
    )

  # ---------- TinyFox summaries ----------
  # Keep the last-24h style values from the solar-noon-selected row (b_daily),
  # BUT still compute message-gap QA and altitude day/night from full-res b2.

  tiny_gap_alt_daily <- b2 %>%
    dplyr::filter(tolower(!!sym(tag_type_col)) == "tinyfox") %>%
    dplyr::group_by(!!sym(track_col), .bat_day) %>%
    dplyr::summarise(
      !!paste0(out_prefix, "n_msg")      := dplyr::n(),
      !!paste0(out_prefix, "gap_h_mean") := mean(.dt_prev_h, na.rm = TRUE),
      !!paste0(out_prefix, "gap_h_max")  := max(.dt_prev_h,  na.rm = TRUE),

      !!paste0(out_prefix, "alt_day_mean_m")  := mean(.alt_m[.is_day %in% TRUE], na.rm = TRUE),
      !!paste0(out_prefix, "alt_day_mean_n")  := sum(is.finite(.alt_m) & (.is_day %in% TRUE)),
      !!paste0(out_prefix, "alt_night_max_m") := suppressWarnings(max(.alt_m[.is_night %in% TRUE], na.rm = TRUE)),
      !!paste0(out_prefix, "alt_night_max_n") := sum(is.finite(.alt_m) & (.is_night %in% TRUE)),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::matches(paste0("^", out_prefix, "alt_night_max_m$")),
        ~ dplyr::if_else(is.infinite(.x), NA_real_, .x)
      )
    )

  # tinyfox rolling fields: take from b_daily (noon-thinned), one record per bat-day
  b_daily2 <- b_daily %>%
    dplyr::mutate(.bat_day = get_bat_day(!!sym(move2::mt_time_column(b_daily))))

  tiny_from_noon <- b_daily2 %>%
    dplyr::filter(tolower(!!sym(tag_type_col)) == "tinyfox") %>%
    dplyr::select(
      !!sym(track_col), .bat_day,
      dplyr::all_of(c(tiny_act_col, tiny_pmin_col, tiny_tmax_col, tiny_tmin_col, tiny_vedba_col))
    ) %>%
    dplyr::transmute(
      !!sym(track_col), .bat_day,

      !!paste0(out_prefix, "activity_percent_24h") := .data[[tiny_act_col]],
      !!paste0(out_prefix, "activity_percent_24h_n") := as.integer(!is.na(.data[[tiny_act_col]])),

      !!paste0(out_prefix, "pmin_24h") := .data[[tiny_pmin_col]],
      !!paste0(out_prefix, "pmin_24h_n") := as.integer(!is.na(.data[[tiny_pmin_col]])),

      !!paste0(out_prefix, "tmax_24h") := .data[[tiny_tmax_col]],
      !!paste0(out_prefix, "tmax_24h_n") := as.integer(!is.na(.data[[tiny_tmax_col]])),

      !!paste0(out_prefix, "tmin_24h") := .data[[tiny_tmin_col]],
      !!paste0(out_prefix, "tmin_24h_n") := as.integer(!is.na(.data[[tiny_tmin_col]])),

      !!paste0(out_prefix, "total_vedba_24h") := .data[[tiny_vedba_col]],
      !!paste0(out_prefix, "total_vedba_24h_n") := as.integer(!is.na(.data[[tiny_vedba_col]]))
    )

  tiny_daily <- dplyr::left_join(tiny_gap_alt_daily, tiny_from_noon, by = c(track_col, ".bat_day"))

  # ---------- combine + join back onto b_daily ----------
  daily_metrics <- dplyr::bind_rows(nano_daily, tiny_daily)

  out <- b_daily2 %>%
    dplyr::left_join(daily_metrics, by = c(track_col, ".bat_day")) %>%
    dplyr::select(-.bat_day)

  out
}
