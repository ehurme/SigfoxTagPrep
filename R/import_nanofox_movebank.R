import_nanofox_movebank <- function(
    study_id,
    tag_type = NULL,
    sensor_external_ids = c("acceleration", "accessory-measurements", "barometer", "sigfox-geolocation"),
    sensor_labels       = c("VeDBA", "avg.temp", "min.baro.pressure", "location"),
    merge_studies = TRUE,
    track_combine = "merge",
    compute_vedba_sum = TRUE,  # (kept for backwards-compat; logic lives in sourced helpers)
    vedba_col = "vedba",       # (kept for backwards-compat; logic lives in sourced helpers)
    vedba_sum_name = "vedba_sum",
    run_elevation = TRUE,
    run_daily_metrics = TRUE,
    daily_method = c("solar_noon", "daytime_only", "noon_roost"),
    verbose = TRUE,
    # ---- local project scripts (keep explicit, so paths are discoverable) ----
    script_mt_add_start = "../SigfoxTagPrep/R/mt_add_start.R",
    script_add_vedba_temp = "../SigfoxTagPrep/R/add_vedba_temp_to_locations.R",
    script_add_min_pressure = "../SigfoxTagPrep/R/add_min_pressure_to_locations.R",
    script_mt_previous = "../SigfoxTagPrep/R/mt_previous.R",
    script_calc_displacement = "../SigfoxTagPrep/R/calc_displacement.R",
    script_pressure_to_altitude = "../SigfoxTagPrep/R/pressure_to_altitude_m.R",
    script_daily = "../SigfoxTagPrep/R/mt_thin_daily_solar_noon.R",
    script_daily_sensor = "../SigfoxTagPrep/R/mt_add_daily_sensor_metrics.R",
    tz = "UTC"
) {
  suppressPackageStartupMessages({
    # core
    requireNamespace("dplyr", quietly = TRUE)
    requireNamespace("tibble", quietly = TRUE)
    requireNamespace("purrr", quietly = TRUE)
    requireNamespace("tidyr", quietly = TRUE)
    requireNamespace("stringr", quietly = TRUE)
    requireNamespace("lubridate", quietly = TRUE)

    # move2 pipeline deps
    requireNamespace("move2", quietly = TRUE)
    requireNamespace("sf", quietly = TRUE)

    # assertthat must be *attached* (not just namespace-loaded) because
    # mt_thin_daily_solar_noon() calls assert_that() without a :: prefix.
    # requireNamespace() alone is not enough -- use require() to attach it.
    require(assertthat, quietly = TRUE)

    # optional extras used in your pipeline
    if (isTRUE(run_elevation)) requireNamespace("elevatr", quietly = TRUE)
    if (isTRUE(run_daily_metrics)) requireNamespace("suncalc", quietly = TRUE)
  })

  # ----------------------------
  # helpers
  # ----------------------------

  .msg <- function(...) if (isTRUE(verbose)) message(...)

  .safe_try <- function(expr, what = "step") {
    tryCatch(
      expr,
      error = function(e) {
        .msg("⚠️  ", what, " failed: ", conditionMessage(e))
        NULL
      }
    )
  }

  .norm <- function(x) {
    x <- tolower(trimws(as.character(x)))
    x <- gsub("\\s+", " ", x)
    x
  }

  .stop_if_missing <- function(path) {
    if (!file.exists(path)) stop("Missing required script: ", path)
    invisible(TRUE)
  }

  .source_local <- function(path) {
    .stop_if_missing(path)
    source(path)
    invisible(TRUE)
  }

  # Enforce strict time ordering per track by dropping duplicate timestamps.
  # Keeps one row per (track, timestamp) preferring non-"start" rows and then non-NA event_id.
  .dedupe_timestamps <- function(x) {
    # x is move2/sf; use dplyr but return as same object class (sf) and keep attributes,
    # then rely on move2 methods downstream on the sf-backed object
    # (If you need strict move2 class here, do it in base indexing instead.)
    x %>%
      group_by(individual_local_identifier, timestamp) %>%
      slice_max(
        order_by = (is.na(comments) | comments != "start") + 0.1 * (!is.na(event_id)),
        n = 1,
        with_ties = FALSE
      ) %>%
      ungroup() %>%
      arrange(individual_local_identifier, timestamp)
  }

  # Label the existing first record as "start" if stacking created a duplicate at t0.
  # Uses the updated mt_add_start you already fixed to preserve move2 class.
  .add_start <- function(x) {
    .source_local(script_mt_add_start)
    mt_add_start(x)
  }

  add_prev_latlon <- function(x,
                              lat_name = "lat_prev",
                              lon_name = "lon_prev") {
    require(move2)
    require(sf)
    require(dplyr)

    if (!inherits(x, "move2")) {
      stop("x must be a move2 object")
    }

    # extract coordinates
    coords <- sf::st_coordinates(x)

    # get track ids (already defines grouping)
    track_id <- move2::mt_track_id(x)

    df_coords <- tibble::tibble(
      track_id = track_id,
      lon = coords[, "X"],
      lat = coords[, "Y"]
    ) %>%
      group_by(track_id) %>%
      mutate(
        !!lon_name := dplyr::lag(lon),
        !!lat_name := dplyr::lag(lat)
      ) %>%
      ungroup()

    # attach back to move2 object
    x[[lon_name]] <- df_coords[[lon_name]]
    x[[lat_name]] <- df_coords[[lat_name]]

    return(x)
  }

  # Determine requested sensors present in study, returning sensor_type_id values for download.
  .wanted_sensor_ids <- function(study_info, sensor_selected) {
    study_sensor_names <- as.character(study_info$sensor_type_ids)
    study_sensor_names <- unlist(strsplit(study_sensor_names, "\\s*,\\s*"))
    study_sensor_names <- trimws(study_sensor_names)

    study_sensor_names_n <- .norm(study_sensor_names)

    sensor_selected_n <- sensor_selected %>%
      mutate(name_n = .norm(.data$name))

    wanted_ids <- sensor_selected_n %>%
      dplyr::filter(.data$name_n %in% study_sensor_names_n) %>%
      pull(.data$id)

    list(
      wanted_ids = wanted_ids,
      study_sensor_names = study_sensor_names
    )
  }

  # Add common event attributes if present; never hard-fail.
  .add_event_attrs <- function(x) {
    .safe_try({
      x <- x %>% mt_as_event_attribute("taxon_canonical_name") %>% mutate(species = taxon_canonical_name)
      x
    }, "mt_as_event_attribute(taxon_canonical_name)") %||% x

    x <- .safe_try({ x %>% mt_as_event_attribute("sex") }, "mt_as_event_attribute(sex)") %||% x
    x <- .safe_try({ x %>% mt_as_event_attribute("model") }, "mt_as_event_attribute(model)") %||% x
    x <- .safe_try({ x %>% mt_as_event_attribute("attachment_comments") }, "mt_as_event_attribute(attachment_comments)") %||% x

    x
  }

  # Infer tag_type if not provided.
  .set_tag_type <- function(x) {
    if (!is.null(tag_type) && length(tag_type) == 1) {
      x$tag_type <- tag_type
      return(x)
    }

    # default inference
    x$tag_type <- "tinyfox"
    if (!is.null(x$model)) {
      x$tag_type[grepl("Nano", x$model, ignore.case = TRUE)] <- "nanofox"
    }
    if (is.null(x$model)) {
      x$tag_type <- "uWasp"
    }
    x
  }

  # Attach lon/lat columns
  .add_lonlat <- function(x) {
    coords <- sf::st_coordinates(x)
    x$lon <- coords[, 1]
    x$lat <- coords[, 2]
    x
  }

  # calculate distance and bearing
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


  # Merge sensor_type label onto event rows
  .label_sensor_type <- function(x, sensor_selected) {
    x %>%
      left_join(
        sensor_selected %>% dplyr::select(id, sensor_type, is_location_sensor),
        by = c("sensor_type_id" = "id")
      )
  }

  # Fix track data edge case: list-columns from multiple deployments.
  .fix_track_data_lists <- function(x) {
    td <- mt_track_data(x)
    if (any(vapply(td, is.list, logical(1)))) {
      .msg("Track data contains list-columns; expanding with tidyr::unnest_longer().")
      td2 <- td %>% tidyr::unnest_longer(col = tidyselect::everything())
      x <- mt_set_track_data(x, td2)
    }
    x
  }

  # Create a location-only object and compute movement metrics, with dedupe prior to mt_speed.
  .make_location_metrics <- function(x) {
    b_loc <- x %>%
      dplyr::filter(.data$sensor_type == "location", !sf::st_is_empty(.data$geometry))

    # Strictly increasing timestamps required by mt_speed/mt_distance/mt_time_lags
    b_loc <- .dedupe_timestamps(b_loc)

    # add year
    b_loc$year <- year(b_loc$timestamp)

    # If still not time-ordered, bail with informative error
    if (!move2::mt_is_time_ordered(b_loc)) {
      stop("Location data still not strictly time-ordered within track after dedupe. ",
           "Inspect duplicates or timestamp parsing.")
    }

    b_loc <- b_loc %>%
      mutate(
        azimuth  = mt_azimuth(.),
        speed    = mt_speed(., units = "km/hr"),
        distance = mt_distance(., units = "km"),
        dt       = mt_time_lags(., "secs")
      )

    .source_local(script_mt_previous)
    b_loc <- b_loc %>%
      mt_add_prev_metrics(dist_units = "km",
                          speed_units = "km/hr",
                          time_units = "secs")

    .source_local(script_calc_displacement)
    b_loc <- calc_displacement(b_loc)

    .source_local(script_pressure_to_altitude)
    b_loc <- .safe_try(add_altitude_from_pressure(b_loc), "add_altitude_from_pressure") %||% b_loc

    b_loc
  }

  # Produce a daily (solar-noon) location dataset with daily movement + sensor summaries (bat-night aligned).
  .source_local(script_daily)         # should define mt_filter_daily_solar_noon(), mt_thin_daily_solar_noon()
  .source_local(script_daily_sensor)  # should define mt_make_daily_bat_metrics() + helpers

  # ---------------------------------------------------------------------------
  # .add_night_day_id()
  #   Adds a `night_day_id` column to a move2/sf object.
  #
  #   Format:  "<night|day>_<period>"
  #     - night/day : solar altitude at the fix location and time.
  #                   "night" when sun altitude < 0, "day" otherwise.
  #     - period    : integer days since the individual's first timestamp
  #                   (0 = capture day, 1 = next day, …). Uses calendar date
  #                   (UTC), so period increments cleanly at midnight.
  #
  #   Requires: suncalc (loaded when run_daily_metrics = TRUE),
  #             columns: timestamp, lon, lat, individual_local_identifier.
  # ---------------------------------------------------------------------------
  .add_night_day_id <- function(x) {
    if (!requireNamespace("suncalc", quietly = TRUE)) {
      warning(".add_night_day_id: suncalc not available; skipping night_day_id.")
      return(x)
    }

    # lon/lat should already exist from .add_lonlat(), but guard anyway
    if (!all(c("lon", "lat") %in% names(x))) {
      coords <- sf::st_coordinates(x)
      x$lon  <- coords[, 1]
      x$lat  <- coords[, 2]
    }

    df <- tibble::tibble(
      .row_idx   = seq_len(nrow(x)),
      individual = x$individual_local_identifier,
      timestamp  = x$timestamp,
      lon        = x$lon,
      lat        = x$lat
    ) %>%
      dplyr::filter(!is.na(.data$lon), !is.na(.data$lat), !is.na(.data$timestamp))

    if (nrow(df) == 0) {
      x$night_day_id <- NA_character_
      return(x)
    }

    # solar position — use the data= form which is robust across suncalc versions
    # and correctly handles vectorised lat/lon without the "must be unique" error.
    sun_pos <- suncalc::getSunlightPosition(
      data = data.frame(date = df$timestamp, lat = df$lat, lon = df$lon),
      keep = "altitude"
    )
    df$night_day <- ifelse(sun_pos$altitude < 0, "night", "day")

    # period: calendar days since each individual's first fix
    df <- df %>%
      dplyr::group_by(.data$individual) %>%
      dplyr::mutate(
        first_date = as.Date(min(.data$timestamp, na.rm = TRUE)),
        period     = as.integer(as.Date(.data$timestamp) - .data$first_date)
      ) %>%
      dplyr::ungroup()

    df$night_day_id <- paste(df$night_day, df$period, sep = "_")

    # attach back — NA for rows with missing coords/timestamp
    out_vec <- rep(NA_character_, nrow(x))
    out_vec[df$.row_idx] <- df$night_day_id
    x$night_day_id <- out_vec

    .msg("  night_day_id assigned. Unique labels: ",
         dplyr::n_distinct(df$night_day_id),
         " | night fixes: ", sum(df$night_day == "night"),
         " | day fixes: ",   sum(df$night_day == "day"))

    x
  }

  # ---------------------------------------------------------------------------
  # Daily location method: "daytime_only"
  #   Same as solar_noon thinning but first removes fixes between 20:00-04:00
  #   UTC, as bats are actively flying then and unlikely to represent a roost.
  # ---------------------------------------------------------------------------
  .select_daily_daytime_only <- function(x) {
    if (!inherits(x, c("move2", "sf"))) stop("x must be a move2/sf object")

    # Filter out night-flight window (20:00 to 04:00 UTC, exclusive of endpoints)
    hr <- lubridate::hour(x$timestamp)
    flying_window <- hr >= 20 | hr < 4
    n_removed <- sum(flying_window, na.rm = TRUE)
    .msg("  [daytime_only] Removing ", n_removed, " fixes in 20:00-04:00 UTC flight window.")
    x_day <- x[!flying_window, ]

    if (nrow(x_day) == 0) {
      warning("[daytime_only] No fixes remain after removing flight window. Returning NULL.")
      return(NULL)
    }

    # Then apply the standard solar-noon thinning on the filtered set
    if (!exists("mt_thin_daily_solar_noon", mode = "function")) {
      stop("mt_thin_daily_solar_noon() not found. Source script_daily first.")
    }
    mt_thin_daily_solar_noon(x_day, tz = tz)
  }

  # ---------------------------------------------------------------------------
  # Daily location method: "noon_roost"
  #   Assigns each fix to its biological noon (night fixes before midnight go
  #   to the *next* day's noon; night fixes after midnight stay on the same
  #   day's noon). Selects the fix closest to noon per individual x period,
  #   preferring daytime fixes. Drops period-0 fixes except the capture point.
  #
  #   Requires columns: night_day_id, individual_local_identifier, timestamp
  #   (night_day_id is computed by .add_night_day_id() earlier in the pipeline).
  # ---------------------------------------------------------------------------
  .select_daily_noon_roost <- function(x) {
    if (!"night_day_id" %in% names(x)) {
      stop("[noon_roost] Column 'night_day_id' not found. Ensure .add_night_day_id() ran successfully.")
    }

    # 1. Drop period-0 fixes (capture night/next day) except the true capture point
    rep_daily <- x %>%
      dplyr::filter(!stringr::str_detect(.data$night_day_id, "_0$")) %>%
      dplyr::mutate(
        night_day_split = stringr::str_split_fixed(.data$night_day_id, "_", 2),
        night_day  = night_day_split[, 1],
        period_id  = night_day_split[, 2]
      ) %>%
      dplyr::select(-night_day_split)

    .msg("  [noon_roost] Period-0 fixes removed. Remaining: ",
         nrow(rep_daily), " of ", nrow(x))

    # 2. Assign biological noon to each fix
    #    - daytime fix         -> same day's noon
    #    - night fix >= 12:00  -> next day's noon  (pre-midnight; bat still flying)
    #    - night fix <  12:00  -> same day's noon  (post-midnight; bat at roost)
    rep_daily <- rep_daily %>%
      dplyr::mutate(
        noon_date = dplyr::case_when(
          .data$night_day == "night" & lubridate::hour(.data$timestamp) >= 12 ~
            as.Date(.data$timestamp) + 1,
          TRUE ~ as.Date(.data$timestamp)
        ),
        noon_time    = as.POSIXct(paste0(.data$noon_date, " 12:00:00"), tz = "UTC"),
        dist_to_noon = abs(as.numeric(
          difftime(.data$timestamp, .data$noon_time, units = "secs")
        ))
      )

    # 3. One representative fix per individual x period
    #    Daytime fixes sorted first (FALSE < TRUE), then by proximity to noon.
    rep_daily_selected <- rep_daily %>%
      dplyr::group_by(.data$individual_local_identifier, .data$period_id) %>%
      dplyr::arrange(.data$night_day == "night", .data$dist_to_noon, .by_group = TRUE) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    .msg("  [noon_roost] Daily representative points selected.\n",
         "   Individuals: ", dplyr::n_distinct(rep_daily_selected$individual_local_identifier), "\n",
         "   Rows selected: ", nrow(rep_daily_selected))

    # Drop helper columns — keep night_day_id intact for downstream use
    rep_daily_selected <- rep_daily_selected %>%
      dplyr::select(-dplyr::any_of(c("night_day", "period_id", "noon_date",
                                     "noon_time", "dist_to_noon")))

    rep_daily_selected
  }

  # ---------------------------------------------------------------------------
  # .make_daily(): dispatcher — routes to the requested method, then appends
  #                daily sensor summaries via mt_add_daily_sensor_metrics()
  #                (same post-processing regardless of selection method).
  # ---------------------------------------------------------------------------
  .make_daily <- function(x) {
    method <- match.arg(daily_method,
                        c("solar_noon", "daytime_only", "noon_roost"))

    # --- location selection -------------------------------------------------
    b_daily <- switch(
      method,

      "solar_noon" = {
        if (!exists("mt_thin_daily_solar_noon", mode = "function")) {
          stop("mt_thin_daily_solar_noon() not found. Source script_daily first.")
        }
        .msg("Daily method: solar_noon (standard)")
        .safe_try(mt_thin_daily_solar_noon(x, tz = tz), "mt_thin_daily_solar_noon")
      },

      "daytime_only" = {
        .msg("Daily method: daytime_only (excludes 20:00-04:00 UTC)")
        .safe_try(.select_daily_daytime_only(x), "select_daily_daytime_only")
      },

      "noon_roost" = {
        .msg("Daily method: noon_roost (biological-noon assignment)")
        .safe_try(.select_daily_noon_roost(x), "select_daily_noon_roost")
      }
    )

    if (is.null(b_daily) || nrow(b_daily) == 0) {
      warning(".make_daily(): daily location selection returned no rows for method '",
              method, "'.")
      return(b_daily)
    }

    # --- daily sensor summaries (same for all methods) ----------------------
    if (exists("mt_add_daily_sensor_metrics", mode = "function")) {
      b_daily <- .safe_try(
        mt_add_daily_sensor_metrics(
          b_all   = x,
          b_daily = b_daily,
          tz      = tz,
          day_anchor_hour = 12
        ),
        "mt_add_daily_sensor_metrics"
      ) %||% b_daily
    }

    return(b_daily)
  }

  `%||%` <- function(a, b) if (!is.null(a)) a else b

  # ----------------------------
  # input checks
  # ----------------------------
  if (length(study_id) < 1) stop("Provide at least one study_id.")
  if (length(sensor_external_ids) != length(sensor_labels)) {
    stop("sensor_external_ids and sensor_labels must have the same length.")
  }

  # ----------------------------
  # sensor lookup table (Movebank tag_type)
  # ----------------------------
  sensors_tbl <- move2::movebank_retrieve(entity_type = "tag_type") %>% as_tibble()

  sensor_selected <- sensors_tbl %>%
    dplyr::filter(.data$external_id %in% sensor_external_ids) %>%
    mutate(sensor_type = sensor_labels[match(.data$external_id, sensor_external_ids)])

  if (nrow(sensor_selected) == 0) {
    stop("None of the requested sensor_external_ids were found in movebank_retrieve(tag_type).")
  }

  if (verbose) {
    .msg("Selected sensors:")
    print(sensor_selected %>% dplyr::select(id, name, external_id, sensor_type, is_location_sensor), n = 50)
  }

  # ----------------------------
  # per-study download + processing
  # ----------------------------
  download_one_study <- function(id) {
    .msg("Downloading study: ", id)

    si <- move2::movebank_download_study_info(study_id = id)

    w <- .wanted_sensor_ids(si, sensor_selected)
    wanted_ids <- w$wanted_ids

    if (length(wanted_ids) == 0) {
      stop(
        "Study ", id, " has no matching sensors.\n",
        "Study sensors: ", paste(w$study_sensor_names, collapse = ", "), "\n",
        "Requested (Movebank tag_type$name): ", paste(sensor_selected$name, collapse = ", ")
      )
    }

    b <- move2::movebank_download_study(study_id = id, sensor_type_id = wanted_ids)

    # Ensure track id column exists / set
    if (is.null(b$individual_local_identifier)) {
      # Most movebank downloads include this; but keep your guard
      mt_track_id(b) <- "individual_local_identifier"
    }

    b <- .fix_track_data_lists(b)

    # Add useful attributes
    b <- .add_event_attrs(b)

    # Add tag_type
    b <- .set_tag_type(b)

    # Add lon/lat
    b <- .add_lonlat(b)

    # Label sensor type
    b <- .label_sensor_type(b, sensor_selected)

    # Add capture/start record (your updated mt_add_start should preserve move2 class)
    b <- .safe_try(.add_start(b), "mt_add_start") %||% b

    # Attach sensor summaries to location rows
    .source_local(script_add_vedba_temp)
    b <- .safe_try(add_vedba_temp_to_locations(df = b), "add_vedba_temp_to_locations") %||% b

    .source_local(script_add_min_pressure)
    b <- .safe_try(add_min_pressure_to_locations(df = b), "add_min_pressure_to_locations") %||% b

    # convert pressure to altitude and get elevation
    .source_local(script_pressure_to_altitude)
    b <- .safe_try(
      add_altitude_from_pressure(
        df = b,
        nano_pressure_col = "min_3h_pressure",
        tiny_pressure_col = "tinyfox_pressure_min_last_24h",
        altitude_col = "altitude_m"
      ),
      "add_altitude_from_pressure"
    ) %||% b

    # Assign night_day_id: "<night|day>_<days_since_capture>" for every fix
    b <- .safe_try(.add_night_day_id(b), "add_night_day_id") %||% b

    # Location-only + metrics
    b_loc <- .make_location_metrics(b)
    b_loc <- add_prev_latlon(b_loc)

    # Daily dataset
    .source_local(script_daily)
    b_daily <- .make_daily(b)

    # Guard: if daily selection failed/returned NULL, skip metrics rather than crash
    if (is.null(b_daily) || nrow(b_daily) == 0) {
      .msg("Warning: daily dataset is empty for study ", id, " — skipping daily metrics.")
      b_daily2 <- b_daily
    } else {
      b_daily2 <- .make_location_metrics(b_daily)
      b_daily2 <- add_prev_latlon(b_daily2)
    }

    # add year, yday, season
    b$year    <- factor(year(b$timestamp))
    b_loc$year <- factor(year(b_loc$timestamp))

    b$yday    <- factor(yday(b$timestamp))
    b_loc$yday <- factor(yday(b_loc$timestamp))

    b$season    <- ifelse(month(b$timestamp) > 7, "Fall", "Spring")
    b_loc$season <- ifelse(month(b_loc$timestamp) > 7, "Fall", "Spring")

    if (!is.null(b_daily2) && nrow(b_daily2) > 0) {
      b_daily2$year   <- factor(year(b_daily2$timestamp))
      b_daily2$yday   <- factor(yday(b_daily2$timestamp))
      b_daily2$season <- ifelse(month(b_daily2$timestamp) > 7, "Fall", "Spring")
    }


    list(
      study_id  = id,
      full      = b,
      location  = b_loc,
      daily     = b_daily2
    )
  }

  res_list <- lapply(study_id, download_one_study)
  names(res_list) <- as.character(study_id)

  # ----------------------------
  # merge studies (move2 stacks)
  # ----------------------------
  merge_stack <- function(objs) {
    if (length(objs) == 1) return(objs[[1]])
    out <- objs[[1]]
    for (i in 2:length(objs)) {
      out <- move2::mt_stack(out, objs[[i]], .track_combine = track_combine)
    }
    out
  }

  full_list <- lapply(res_list, `[[`, "full")
  loc_list  <- lapply(res_list, `[[`, "location")
  daily_list  <- lapply(res_list, `[[`, "daily")

  full_merged <- if (isTRUE(merge_studies)) merge_stack(full_list) else full_list
  loc_merged  <- if (isTRUE(merge_studies)) merge_stack(loc_list)  else loc_list
  daily_merged  <- if (isTRUE(merge_studies)) merge_stack(daily_list)  else daily_list

  # ----------------------------
  # summaries
  # ----------------------------
  if (verbose) {
    if (isTRUE(merge_studies)) {
      .msg("Merged studies: ", paste(study_id, collapse = ", "))
      .msg("Sensor types present (merged full):")
      print(table(full_merged$sensor_type, useNA = "ifany"))
      .msg("Timestamp summary (merged full):")
      print(summary(full_merged$timestamp))
    } else {
      .msg("Downloaded ", length(study_id), " studies (not merged).")
    }
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


# # One study
# x <- {}
# x <- import_nanofox_movebank(study_id = 4520022960) # 7772112798)
# terra::ext(x$location)
# b_full <- x$full
# b_loc  <- x$location
# b_daily <- x$daily
# plot(abs(b_daily$dnsd), b_daily$dist_prev)

# b_daily$geometry
# b_daily$displacement
# plot(b_loc$geometry)
# names(b_daily)
# plot(b_daily$daily_total_vedba_24h)
# summary(b_full$timestamp)
# table(b_full$sensor_type)
#
# # Multiple studies -> merged stack
# y <- {}
# y <- import_nanofox_movebank(study_id = c(7772112798, 7771879004, 7771978717, 3597331705))
# leisler_full <- y$full
# leisler_loc  <- y$location
# leisler_daily <- y$daily
# plot(leisler_loc$geometry, col = leisler_loc$species)
# plot(leisler_daily$timestamp, leisler_daily$displacement)
# leisler_daily$tag_type %>% table()
# track_data <- mt_track_data(leisler_loc)
# names(leisler_loc)
# track_data$taxon_canonical_name %>% table()
