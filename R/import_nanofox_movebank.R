import_nanofox_movebank <- function(
    study_id,
    tag_type = NULL,
    sensor_external_ids = c("acceleration", "accessory-measurements", "barometer", "sigfox-geolocation"),
    sensor_labels       = c("VeDBA", "avg.temp", "min.baro.pressure", "location"),
    merge_studies = TRUE,
    track_combine = "merge",
    compute_vedba_sum = TRUE,
    vedba_col = "vedba",
    vedba_sum_name = "vedba_sum",
    run_elevation = TRUE,
    run_daily_metrics = TRUE,
    daily_method = c("solar_noon", "daytime_only", "noon_roost"),
    verbose = TRUE,
    script_mt_add_start         = "../SigfoxTagPrep/R/mt_add_start.R",
    script_add_vedba_temp       = "../SigfoxTagPrep/R/add_vedba_temp_to_locations.R",
    script_add_min_pressure     = "../SigfoxTagPrep/R/add_min_pressure_to_locations.R",
    script_mt_previous          = "../SigfoxTagPrep/R/mt_previous.R",
    script_calc_displacement    = "../SigfoxTagPrep/R/calc_displacement.R",
    script_pressure_to_altitude = "../SigfoxTagPrep/R/pressure_to_altitude_m.R",
    script_daily                = "../SigfoxTagPrep/R/mt_thin_daily_solar_noon.R",
    script_daily_sensor         = "../SigfoxTagPrep/R/mt_add_daily_sensor_metrics.R",
    tz = "UTC"
) {
  suppressPackageStartupMessages({
    require("tidyverse",  quietly = TRUE)
    require("dplyr",      quietly = TRUE)
    require("tibble",     quietly = TRUE)
    require("purrr",      quietly = TRUE)
    require("tidyr",      quietly = TRUE)
    require("stringr",    quietly = TRUE)
    require("lubridate",  quietly = TRUE)
    require("move2",      quietly = TRUE)
    require("sf",         quietly = TRUE)
    require("units",      quietly = TRUE)
    require(assertthat,   quietly = TRUE)
    if (isTRUE(run_elevation))     require("elevatr", quietly = TRUE)
    if (isTRUE(run_daily_metrics)) require("suncalc", quietly = TRUE)
  })

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  .msg      <- function(...) if (isTRUE(verbose)) message(...)
  `%||%`    <- function(a, b) if (!is.null(a)) a else b

  .safe_try <- function(expr, what = "step") {
    tryCatch(expr, error = function(e) {
      .msg("\u26a0\ufe0f  ", what, " failed: ", conditionMessage(e)); NULL
    })
  }

  .norm <- function(x) gsub("\\s+", " ", tolower(trimws(as.character(x))))

  .stop_if_missing <- function(path) {
    if (!file.exists(path)) stop("Missing required script: ", path)
    invisible(TRUE)
  }

  .source_local <- function(path) { .stop_if_missing(path); source(path); invisible(TRUE) }

  # Enforce strict time ordering per track (required by mt_speed / mt_distance).
  .dedupe_timestamps <- function(x) {
    x %>%
      group_by(individual_local_identifier, timestamp) %>%
      slice_max(
        order_by  = (is.na(comments) | comments != "start") + 0.1 * (!is.na(event_id)),
        n         = 1,
        with_ties = FALSE
      ) %>%
      ungroup() %>%
      arrange(individual_local_identifier, timestamp)
  }

  .add_start <- function(x) { .source_local(script_mt_add_start); mt_add_start(x) }

  add_prev_latlon <- function(x, lat_name = "lat_prev", lon_name = "lon_prev") {
    if (!inherits(x, "move2")) stop("x must be a move2 object")
    coords   <- sf::st_coordinates(x)
    track_id <- move2::mt_track_id(x)
    df <- tibble::tibble(track_id = track_id,
                         lon = coords[, "X"], lat = coords[, "Y"]) %>%
      group_by(track_id) %>%
      mutate(!!lon_name := dplyr::lag(lon),
             !!lat_name := dplyr::lag(lat)) %>%
      ungroup()
    x[[lon_name]] <- df[[lon_name]]
    x[[lat_name]] <- df[[lat_name]]
    x
  }

  .wanted_sensor_ids <- function(study_info, sensor_selected) {
    study_names <- trimws(unlist(strsplit(as.character(study_info$sensor_type_ids), "\\s*,\\s*")))
    wanted_ids  <- sensor_selected %>%
      mutate(name_n = .norm(.data$name)) %>%
      dplyr::filter(.data$name_n %in% .norm(study_names)) %>%
      pull(.data$id)
    list(wanted_ids = wanted_ids, study_sensor_names = study_names)
  }

  .add_event_attrs <- function(x) {
    x <- .safe_try(x %>% mt_as_event_attribute("taxon_canonical_name") %>%
                     mutate(species = taxon_canonical_name),
                   "mt_as_event_attribute(taxon_canonical_name)") %||% x
    for (attr in c("sex", "model", "attachment_comments"))
      x <- .safe_try(x %>% mt_as_event_attribute(attr),
                     paste0("mt_as_event_attribute(", attr, ")")) %||% x
    x
  }

  .set_tag_type <- function(x) {
    if (!is.null(tag_type) && length(tag_type) == 1) { x$tag_type <- tag_type; return(x) }
    x$tag_type <- "tinyfox"
    if (!is.null(x$model))  x$tag_type[grepl("Nano", x$model, ignore.case = TRUE)] <- "nanofox"
    if (is.null(x$model))   x$tag_type <- "uWasp"
    x
  }

  .add_lonlat <- function(x) {
    coords <- sf::st_coordinates(x); x$lon <- coords[, 1]; x$lat <- coords[, 2]; x
  }

  .calc_dist_bearing <- function(lon1, lat1, lon2, lat2) {
    if (require("geosphere", quietly = TRUE)) {
      list(dist_m     = geosphere::distHaversine(cbind(lon1, lat1), cbind(lon2, lat2)),
           bearing_deg = (geosphere::bearing(cbind(lon1, lat1), cbind(lon2, lat2)) + 360) %% 360)
    } else {
      p1m <- sf::st_transform(sf::st_as_sf(data.frame(lon = lon1, lat = lat1),
                                           coords = c("lon","lat"), crs = 4326), 3857)
      p2m <- sf::st_transform(sf::st_as_sf(data.frame(lon = lon2, lat = lat2),
                                           coords = c("lon","lat"), crs = 4326), 3857)
      list(dist_m      = as.numeric(sf::st_distance(p1m, p2m, by_element = TRUE)),
           bearing_deg = (atan2(lon2 - lon1, lat2 - lat1) * 180 / pi + 360) %% 360)
    }
  }

  .label_sensor_type <- function(x, sensor_selected) {
    x %>% left_join(sensor_selected %>% dplyr::select(id, sensor_type, is_location_sensor),
                    by = c("sensor_type_id" = "id"))
  }

  .fix_track_data_lists <- function(x) {
    td <- mt_track_data(x)
    if (any(vapply(td, is.list, logical(1)))) {
      .msg("Track data contains list-columns; expanding with tidyr::unnest_longer().")
      x <- mt_set_track_data(x, td %>% tidyr::unnest_longer(col = tidyselect::everything()))
    }
    x
  }

  # ---------------------------------------------------------------------------
  # .add_delta_altitude()
  #
  # Computes altitude change (m) between consecutive location fixes per track.
  #   Positive = ascending, negative = descending.
  #   Requires `altitude_m` (numeric, metres) from add_altitude_from_pressure().
  #   Result column `delta_altitude_m` carries explicit units = "m" via the
  #   units package, so downstream division by dt gives a unit-safe climb rate.
  # ---------------------------------------------------------------------------
  .add_delta_altitude <- function(x) {
    if (!"altitude_m" %in% names(x)) {
      warning(".add_delta_altitude: 'altitude_m' column not found; skipping.")
      return(x)
    }

    delta <- tibble::tibble(
      track_id   = move2::mt_track_id(x),
      altitude_m = as.numeric(x$altitude_m)   # drop units for lag(); re-attach below
    ) %>%
      dplyr::group_by(track_id) %>%
      dplyr::mutate(delta_altitude_m = altitude_m - dplyr::lag(altitude_m)) %>%
      dplyr::ungroup()

    x$delta_altitude_m <- units::set_units(delta$delta_altitude_m, "m")

    .msg("  delta_altitude_m computed (",
         sum(!is.na(delta$delta_altitude_m)), " non-NA values).")
    x
  }

  # ---------------------------------------------------------------------------
  # .make_location_metrics()
  #
  # Computes per-fix movement metrics on a location-only move2 object.
  #
  # Speed   : km/h  — "km/h" is the correct SI-derived units symbol.
  #           "km/hr" is not recognised by the units package and will error.
  # Distance: km
  # dt      : seconds (numeric; used as-is for climb-rate arithmetic)
  # delta_altitude_m: metres with explicit units (via .add_delta_altitude)
  # ---------------------------------------------------------------------------
  .make_location_metrics <- function(x) {
    b_loc <- x %>%
      dplyr::filter(.data$sensor_type == "location", !sf::st_is_empty(.data$geometry))

    b_loc <- .dedupe_timestamps(b_loc)
    b_loc$year <- lubridate::year(b_loc$timestamp)

    if (!move2::mt_is_time_ordered(b_loc))
      stop("Location data still not strictly time-ordered within track after dedupe.")

    b_loc <- b_loc %>%
      mutate(
        azimuth  = mt_azimuth(.),
        speed    = mt_speed(.,    units = "km/h"),   # km/h: valid SI symbol
        distance = mt_distance(., units = "km"),
        dt       = mt_time_lags(., "secs")
      )

    .source_local(script_mt_previous)
    b_loc <- b_loc %>%
      mt_add_prev_metrics(dist_units  = "km",
                          speed_units = "km/h",      # corrected from "km/hr"
                          time_units  = "secs")

    .source_local(script_calc_displacement)
    b_loc <- calc_displacement(b_loc)

    .source_local(script_pressure_to_altitude)
    b_loc <- .safe_try(add_altitude_from_pressure(b_loc), "add_altitude_from_pressure") %||% b_loc

    # Change in altitude between consecutive fixes (units = m)
    b_loc <- .safe_try(.add_delta_altitude(b_loc), "add_delta_altitude") %||% b_loc

    b_loc
  }

  # Produce a daily (solar-noon) location dataset + bat-night sensor summaries.
  .source_local(script_daily)
  .source_local(script_daily_sensor)

  # ---------------------------------------------------------------------------
  # .add_night_day_id()
  # ---------------------------------------------------------------------------
  .add_night_day_id <- function(x) {
    if (!require("suncalc", quietly = TRUE)) {
      warning(".add_night_day_id: suncalc not available; skipping."); return(x)
    }
    if (!all(c("lon", "lat") %in% names(x))) {
      coords <- sf::st_coordinates(x); x$lon <- coords[, 1]; x$lat <- coords[, 2]
    }
    df <- tibble::tibble(.row_idx = seq_len(nrow(x)),
                         individual = x$individual_local_identifier,
                         timestamp  = x$timestamp,
                         lon = x$lon, lat = x$lat) %>%
      dplyr::filter(!is.na(.data$lon), !is.na(.data$lat), !is.na(.data$timestamp))

    if (nrow(df) == 0) { x$night_day_id <- NA_character_; return(x) }

    sun_pos      <- suncalc::getSunlightPosition(
      data = data.frame(date = df$timestamp, lat = df$lat, lon = df$lon),
      keep = "altitude"
    )
    df$night_day <- ifelse(sun_pos$altitude < 0, "night", "day")
    df <- df %>%
      dplyr::group_by(.data$individual) %>%
      dplyr::mutate(first_date = as.Date(min(.data$timestamp, na.rm = TRUE)),
                    period     = as.integer(as.Date(.data$timestamp) - .data$first_date)) %>%
      dplyr::ungroup()
    df$night_day_id <- paste(df$night_day, df$period, sep = "_")

    out_vec <- rep(NA_character_, nrow(x))
    out_vec[df$.row_idx] <- df$night_day_id
    x$night_day_id <- out_vec

    .msg("  night_day_id assigned. Unique: ", dplyr::n_distinct(df$night_day_id),
         " | night: ", sum(df$night_day == "night"),
         " | day: ",   sum(df$night_day == "day"))
    x
  }

  # ---------------------------------------------------------------------------
  # .select_daily_daytime_only()
  # ---------------------------------------------------------------------------
  .select_daily_daytime_only <- function(x) {
    if (!inherits(x, c("move2", "sf"))) stop("x must be a move2/sf object")
    hr <- lubridate::hour(x$timestamp)
    flying_window <- hr >= 21 | hr < 5
    .msg("  [daytime_only] Removing ", sum(flying_window, na.rm = TRUE),
         " fixes in 21:00-05:00 UTC flight window.")
    x_day <- x[!flying_window, ]
    if (nrow(x_day) == 0) { warning("[daytime_only] No fixes remain."); return(NULL) }
    if (!exists("mt_thin_daily_solar_noon", mode = "function"))
      stop("mt_thin_daily_solar_noon() not found.")
    mt_thin_daily_solar_noon(x_day, tz = tz)
  }

  # ---------------------------------------------------------------------------
  # .select_daily_noon_roost()
  # ---------------------------------------------------------------------------
  .select_daily_noon_roost <- function(x) {
    if (!"night_day_id" %in% names(x)) stop("[noon_roost] 'night_day_id' not found.")
    rep_daily <- x %>%
      dplyr::filter(!stringr::str_detect(.data$night_day_id, "_0$")) %>%
      dplyr::mutate(
        night_day_split = stringr::str_split_fixed(.data$night_day_id, "_", 2),
        night_day = night_day_split[, 1], period_id = night_day_split[, 2]
      ) %>%
      dplyr::select(-night_day_split)
    .msg("  [noon_roost] Period-0 removed. Remaining: ", nrow(rep_daily), " of ", nrow(x))

    rep_daily <- rep_daily %>%
      dplyr::mutate(
        noon_date = dplyr::case_when(
          .data$night_day == "night" & lubridate::hour(.data$timestamp) >= 12 ~
            as.Date(.data$timestamp) + 1,
          TRUE ~ as.Date(.data$timestamp)
        ),
        noon_time    = as.POSIXct(paste0(.data$noon_date, " 12:00:00"), tz = "UTC"),
        dist_to_noon = abs(as.numeric(difftime(.data$timestamp, .data$noon_time, units = "secs")))
      ) %>%
      dplyr::group_by(.data$individual_local_identifier, .data$period_id) %>%
      dplyr::arrange(.data$night_day == "night", .data$dist_to_noon, .by_group = TRUE) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    .msg("  [noon_roost] Selected ", nrow(rep_daily), " daily points across ",
         dplyr::n_distinct(rep_daily$individual_local_identifier), " individuals.")
    rep_daily %>% dplyr::select(-dplyr::any_of(c("night_day", "period_id",
                                                 "noon_date", "noon_time", "dist_to_noon")))
  }

  # ---------------------------------------------------------------------------
  # .make_daily()
  # ---------------------------------------------------------------------------
  .make_daily <- function(x) {
    method  <- match.arg(daily_method, c("solar_noon", "daytime_only", "noon_roost"))
    b_daily <- switch(
      method,
      "solar_noon" = {
        if (!exists("mt_thin_daily_solar_noon", mode = "function"))
          stop("mt_thin_daily_solar_noon() not found.")
        .msg("Daily method: solar_noon")
        .safe_try(mt_thin_daily_solar_noon(x, tz = tz), "mt_thin_daily_solar_noon")
      },
      "daytime_only" = {
        .msg("Daily method: daytime_only (excludes 21:00-05:00 UTC)")
        .safe_try(.select_daily_daytime_only(x), "select_daily_daytime_only")
      },
      "noon_roost" = {
        .msg("Daily method: noon_roost")
        .safe_try(.select_daily_noon_roost(x), "select_daily_noon_roost")
      }
    )
    if (is.null(b_daily) || nrow(b_daily) == 0) {
      warning(".make_daily(): no rows returned for method '", method, "'."); return(b_daily)
    }
    if (exists("mt_add_daily_sensor_metrics", mode = "function"))
      b_daily <- .safe_try(mt_add_daily_sensor_metrics(b_all = x, b_daily = b_daily,
                                                       tz = tz, day_anchor_hour = 12),
                           "mt_add_daily_sensor_metrics") %||% b_daily
    b_daily
  }

  # ---------------------------------------------------------------------------
  # Input checks
  # ---------------------------------------------------------------------------
  if (length(study_id) < 1) stop("Provide at least one study_id.")
  if (length(sensor_external_ids) != length(sensor_labels))
    stop("sensor_external_ids and sensor_labels must have the same length.")

  # ---------------------------------------------------------------------------
  # Sensor lookup
  # ---------------------------------------------------------------------------
  sensors_tbl     <- move2::movebank_retrieve(entity_type = "tag_type") %>% as_tibble()
  sensor_selected <- sensors_tbl %>%
    dplyr::filter(.data$external_id %in% sensor_external_ids) %>%
    mutate(sensor_type = sensor_labels[match(.data$external_id, sensor_external_ids)])
  if (nrow(sensor_selected) == 0)
    stop("None of the requested sensor_external_ids found in movebank_retrieve(tag_type).")
  if (verbose) {
    .msg("Selected sensors:")
    print(sensor_selected %>% dplyr::select(id, name, external_id, sensor_type, is_location_sensor), n = 50)
  }

  # ---------------------------------------------------------------------------
  # Per-study download + processing
  # ---------------------------------------------------------------------------
  download_one_study <- function(id) {
    .msg("Downloading study: ", id)
    si         <- move2::movebank_download_study_info(study_id = id)
    w          <- .wanted_sensor_ids(si, sensor_selected)
    wanted_ids <- w$wanted_ids
    if (length(wanted_ids) == 0)
      stop("Study ", id, " has no matching sensors.\n",
           "Study: ",     paste(w$study_sensor_names, collapse = ", "), "\n",
           "Requested: ", paste(sensor_selected$name, collapse = ", "))

    b <- move2::movebank_download_study(study_id = id, sensor_type_id = wanted_ids)
    if (is.null(b$individual_local_identifier))
      mt_track_id(b) <- "individual_local_identifier"

    b <- .fix_track_data_lists(b)
    b <- .add_event_attrs(b)
    b <- .set_tag_type(b)
    b <- .add_lonlat(b)
    b <- .label_sensor_type(b, sensor_selected)
    b <- .safe_try(.add_start(b), "mt_add_start") %||% b

    .source_local(script_add_vedba_temp)
    b <- .safe_try(add_vedba_temp_to_locations(df = b),   "add_vedba_temp_to_locations")   %||% b

    .source_local(script_add_min_pressure)
    b <- .safe_try(add_min_pressure_to_locations(df = b), "add_min_pressure_to_locations") %||% b

    .source_local(script_pressure_to_altitude)
    b <- .safe_try(
      add_altitude_from_pressure(df = b,
                                 nano_pressure_col = "min_3h_pressure",
                                 tiny_pressure_col = "tinyfox_pressure_min_last_24h",
                                 altitude_col      = "altitude_m"),
      "add_altitude_from_pressure"
    ) %||% b

    b <- .safe_try(.add_night_day_id(b), "add_night_day_id") %||% b

    # Location subset: speed (km/h), distance (km), delta_altitude_m (m)
    b_loc <- .make_location_metrics(b)
    b_loc <- add_prev_latlon(b_loc)

    # Daily subset: same metrics pipeline (speed km/h, delta_altitude_m)
    .source_local(script_daily)
    b_daily <- .make_daily(b)
    if (is.null(b_daily) || nrow(b_daily) == 0) {
      .msg("Warning: daily dataset empty for study ", id, " \u2014 skipping metrics.")
      b_daily2 <- b_daily
    } else {
      b_daily2 <- .make_location_metrics(b_daily)
      b_daily2 <- add_prev_latlon(b_daily2)
    }

    # Temporal covariates on all three objects
    for (obj_name in c("b", "b_loc", "b_daily2")) {
      obj <- get(obj_name)
      if (!is.null(obj) && nrow(obj) > 0) {
        obj$year   <- factor(lubridate::year(obj$timestamp))
        obj$yday   <- factor(lubridate::yday(obj$timestamp))
        obj$season <- ifelse(lubridate::month(obj$timestamp) > 7, "Fall", "Spring")
        assign(obj_name, obj)
      }
    }

    list(study_id = id, full = b, location = b_loc, daily = b_daily2)
  }

  res_list        <- lapply(study_id, download_one_study)
  names(res_list) <- as.character(study_id)

  # ---------------------------------------------------------------------------
  # Merge studies
  # ---------------------------------------------------------------------------
  merge_stack <- function(objs) {
    if (length(objs) == 1) return(objs[[1]])
    out <- objs[[1]]
    for (i in 2:length(objs))
      out <- move2::mt_stack(out, objs[[i]], .track_combine = track_combine)
    out
  }
  full_merged  <- if (isTRUE(merge_studies)) merge_stack(lapply(res_list, `[[`, "full"))     else lapply(res_list, `[[`, "full")
  loc_merged   <- if (isTRUE(merge_studies)) merge_stack(lapply(res_list, `[[`, "location")) else lapply(res_list, `[[`, "location")
  daily_merged <- if (isTRUE(merge_studies)) merge_stack(lapply(res_list, `[[`, "daily"))    else lapply(res_list, `[[`, "daily")

  # ---------------------------------------------------------------------------
  # Summaries
  # ---------------------------------------------------------------------------
  if (verbose && isTRUE(merge_studies)) {
    .msg("Merged studies: ", paste(study_id, collapse = ", "))
    .msg("Sensor types (merged full):"); print(table(full_merged$sensor_type, useNA = "ifany"))
    .msg("Timestamp range (merged full):"); print(summary(full_merged$timestamp))
    if ("speed" %in% names(loc_merged))
      .msg("Speed units (location): ", deparse(units::deparse_unit(loc_merged$speed)))
    if ("delta_altitude_m" %in% names(loc_merged))
      .msg("Delta altitude units (location): ",
           deparse(units::deparse_unit(loc_merged$delta_altitude_m)))
  } else if (verbose) {
    .msg("Downloaded ", length(study_id), " studies (not merged).")
  }

  list(
    sensors_table    = sensors_tbl,
    sensors_selected = sensor_selected,
    studies          = res_list,
    full             = full_merged,
    location         = loc_merged,
    daily            = daily_merged
  )
}
