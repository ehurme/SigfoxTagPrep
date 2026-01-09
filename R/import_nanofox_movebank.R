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
    library(dplyr)
    library(move2)
    library(lubridate)
    library(sf)
    library(tidyr)
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

  # Determine requested sensors present in study, returning sensor_type_id values for download.
  .wanted_sensor_ids <- function(study_info, sensor_selected) {
    study_sensor_names <- as.character(study_info$sensor_type_ids)
    study_sensor_names <- unlist(strsplit(study_sensor_names, "\\s*,\\s*"))
    study_sensor_names <- trimws(study_sensor_names)

    study_sensor_names_n <- .norm(study_sensor_names)

    sensor_selected_n <- sensor_selected %>%
      mutate(name_n = .norm(.data$name))

    wanted_ids <- sensor_selected_n %>%
      filter(.data$name_n %in% study_sensor_names_n) %>%
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
      filter(.data$sensor_type == "location", !sf::st_is_empty(.data$geometry))

    # Strictly increasing timestamps required by mt_speed/mt_distance/mt_time_lags
    b_loc <- .dedupe_timestamps(b_loc)

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

  .make_daily <- function(x) {

    # Fallback requirement (at minimum we can thin)
    if (!exists("mt_thin_daily_solar_noon", mode = "function")) {
      stop("mt_thin_daily_solar_noon() not found in session. Source it before calling import_nanofox_movebank().")
    }
    if (exists("mt_thin_daily_solar_noon", mode = "function")) {
      b_daily <- .safe_try(
        mt_thin_daily_solar_noon(x, tz = tz)
      )
    }

    # Preferred: all-in-one daily metrics (movement + sensors) + drop raw columns
    if (exists("mt_add_daily_sensor_metrics", mode = "function")) {
      b_daily <- .safe_try(
        mt_add_daily_sensor_metrics(
          b_all = x,
          b_daily = b_daily,
          tz = tz,
          day_anchor_hour = 12   # noon->noon keeps one full night per summary
          # optionally pass your column mappings here if they differ
          # nano_pres_col = "min_3h_pressure",
          # nano_temp_col = "avg_temp",
          # nano_vedba_col = "vedba_sum"
        ),
        "mt_make_daily_sensor_metrics"
      )
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
    filter(.data$external_id %in% sensor_external_ids) %>%
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

    # Location-only + metrics
    b_loc <- .make_location_metrics(b)

    # Daily dataset
    .source_local(script_daily)
    b_daily <- .make_daily(b)

    b_daily2 <- .make_location_metrics(b_daily)

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
# x <- import_nanofox_movebank(study_id = 3597331705) # 7772112798)
# terra::ext(x$location)
# b_full <- x$full
# b_loc  <- x$location
# b_daily <- x$daily
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
