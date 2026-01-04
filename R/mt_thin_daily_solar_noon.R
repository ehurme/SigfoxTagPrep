mt_filter_daily_solar_noon <- function(x, tz = "UTC") {
  require(move2)
  require(dplyr)
  require(assertthat)
  require(lubridate)
  require(sf)
  require(suncalc)

  assert_that(move2::mt_is_time_ordered(x))

  time_col  <- move2::mt_time_column(x)
  track_col <- move2::mt_track_id_column(x)

  # work in lon/lat for solar calculations
  x_ll <- x
  if (!sf::st_is_longlat(x_ll)) x_ll <- sf::st_transform(x_ll, 4326)

  ts  <- dplyr::pull(x, !!rlang::sym(time_col))
  day <- as.Date(ts, tz = tz)
  x$date <- day

  sun <- suncalc::getSunlightTimes(
    data = x,
    keep = "solarNoon", tz = tz
  )
  solar_noon <- sun$solarNoon

  dt_abs <- abs(as.numeric(difftime(ts, solar_noon, units = "secs")))
  dt_abs[!is.finite(dt_abs)] <- NA_real_   # normalize Inf/-Inf to NA

  df <- x %>%
    dplyr::mutate(
      .track  = !!rlang::sym(track_col),
      .day    = day,
      .dt_abs = dt_abs,
      .row_in = dplyr::row_number()
    ) %>%
    dplyr::group_by(.track, .day) %>%
    dplyr::mutate(
      .n_ok  = sum(is.finite(.dt_abs), na.rm = TRUE),
      .minok = dplyr::if_else(.n_ok > 0, min(.dt_abs, na.rm = TRUE), NA_real_),
      .sel   = dplyr::if_else(.n_ok > 0, .dt_abs == .minok, FALSE)
    ) %>%
    dplyr::ungroup()

  # break ties (multiple fixes equally close): keep earliest-in-order within that track-day
  df %>%
    dplyr::group_by(.track, .day) %>%
    dplyr::mutate(.sel = .sel & dplyr::row_number() == min(dplyr::row_number()[.sel])) %>%
    dplyr::ungroup() %>%
    dplyr::pull(.sel)
}

# Safer wrapper: uses mt_filter() (preserves move2 structure)
mt_thin_daily_solar_noon <- function(x, tz = "UTC", drop_empty_tracks = TRUE) {
  require(move2)
  require(dplyr)

  sel <- mt_filter_daily_solar_noon(x, tz = tz)

  # IMPORTANT: use mt_filter, not `[`
  out <- filter(x, sel)

  if (drop_empty_tracks) {
    id_col <- move2::mt_track_id_column(out)
    # keep only track IDs that still have >=1 point
    keep_ids <- out %>% dplyr::pull(!!rlang::sym(id_col)) %>% unique()
    out <- out %>% dplyr::filter(!!rlang::sym(id_col) %in% keep_ids)
  }

  out
}

