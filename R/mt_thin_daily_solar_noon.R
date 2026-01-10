mt_filter_daily_solar_noon <- function(
    x,
    tz = "UTC",
    sensor_type_col = "sensor_type",
    location_label  = "location",
    fallback = c("drop_day", "first_location_geom", "first", "last", "random"),
    require_location_geom = TRUE
) {
  require(move2)
  require(dplyr)
  require(assertthat)
  require(lubridate)
  require(sf)
  require(suncalc)
  require(rlang)
  require(tibble)

  fallback <- match.arg(fallback)

  time_col  <- move2::mt_time_column(x)
  track_col <- move2::mt_track_id_column(x)

  ts  <- dplyr::pull(x, !!rlang::sym(time_col))
  day <- as.Date(ts, tz = tz)

  # geometry presence
  geom <- sf::st_geometry(x)
  has_geom <- !sf::st_is_empty(geom)

  # location rows based on sensor_type
  if (!sensor_type_col %in% names(x)) {
    stop("sensor_type_col not found: ", sensor_type_col)
  }
  stype <- as.character(x[[sensor_type_col]])
  is_loc <- !is.na(stype) & (tolower(stype) == tolower(location_label))

  # candidates: must be location rows; optionally must have geometry
  cand <- is_loc & (if (require_location_geom) has_geom else TRUE)

  sel <- rep(FALSE, nrow(x))

  # --- solar noon selection among candidates only ---
  if (any(cand)) {
    x_cand <- x[cand, , drop = FALSE]
    if (!sf::st_is_longlat(x_cand)) x_cand <- sf::st_transform(x_cand, 4326)

    sun <- suncalc::getSunlightTimes(
      data = dplyr::mutate(x_cand, date = day[cand]),
      keep = "solarNoon",
      tz = tz
    )

    solar_noon <- sun$solarNoon
    dt_abs <- abs(as.numeric(difftime(ts[cand], solar_noon, units = "secs")))
    dt_abs[!is.finite(dt_abs)] <- NA_real_

    df_valid <- x_cand %>%
      dplyr::mutate(
        .orig_i = which(cand),
        .track  = dplyr::pull(x_cand, !!rlang::sym(track_col)),
        .day    = day[cand],
        .dt_abs = dt_abs
      ) %>%
      dplyr::group_by(.track, .day) %>%
      dplyr::mutate(
        .n_ok  = sum(is.finite(.dt_abs), na.rm = TRUE),
        .minok = dplyr::if_else(.n_ok > 0, min(.dt_abs, na.rm = TRUE), NA_real_),
        .sel   = dplyr::if_else(.n_ok > 0, .dt_abs == .minok, FALSE)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::group_by(.track, .day) %>%
      dplyr::mutate(.sel = .sel & dplyr::row_number() == min(dplyr::row_number()[.sel])) %>%
      dplyr::ungroup()

    sel[df_valid$.orig_i] <- df_valid$.sel
  }


  # --- fallback: ensure <=1 per track-day, safely (no case_when) ---
  track_vals <- dplyr::pull(x, !!rlang::sym(track_col))

  # ensure no NA in sel before fallback logic
  sel[is.na(sel)] <- FALSE

  df_fb <- tibble::tibble(
    .i = seq_len(nrow(x)),
    .track = track_vals,
    .day = day,
    .cand = cand,
    .has_loc = is_loc
  ) %>%
    dplyr::group_by(.track, .day) %>%
    dplyr::summarise(
      .already = isTRUE(any(sel[.i], na.rm = TRUE)),
      .pick_i = {
        if (.already) {
          NA_integer_
        } else if (fallback == "drop_day") {
          NA_integer_
        } else {
          cand_i <- .i[.cand]
          loc_i  <- .i[.has_loc]

          if (length(cand_i) > 0) {
            if (fallback %in% c("first_location_geom", "first")) cand_i[1]
            else if (fallback == "last") cand_i[length(cand_i)]
            else if (fallback == "random") sample(cand_i, 1)
            else cand_i[1]
          } else if (!require_location_geom && length(loc_i) > 0) {
            if (fallback %in% c("first_location_geom", "first")) loc_i[1]
            else if (fallback == "last") loc_i[length(loc_i)]
            else if (fallback == "random") sample(loc_i, 1)
            else loc_i[1]
          } else {
            if (fallback == "first") .i[1]
            else if (fallback == "last") .i[dplyr::n()]
            else if (fallback == "random") sample(.i, 1)
            else NA_integer_
          }
        }
      },
      .groups = "drop"
    )


  add_i <- df_fb$.pick_i[!is.na(df_fb$.pick_i)]
  sel[add_i] <- TRUE
  sel[is.na(sel)] <- FALSE
  sel
}

mt_thin_daily_solar_noon <- function(
    x,
    tz = "UTC",
    sensor_type_col = "sensor_type",
    location_label = "location",
    fallback = "drop_day",
    require_location_geom = TRUE
) {
  require(move2)

  # order within track by time
  x <- x[order(move2::mt_track_id(x), move2::mt_time(x)), ]
  assert_that(move2::mt_is_time_ordered(x))

  sel <- mt_filter_daily_solar_noon(
    x = x,
    tz = tz,
    sensor_type_col = sensor_type_col,
    location_label = location_label,
    fallback = fallback,
    require_location_geom = require_location_geom
  )
  filter(x, sel)
}

# b_daily <- mt_thin_daily_solar_noon(b)
# b_daily$sensor_type %>% table()
