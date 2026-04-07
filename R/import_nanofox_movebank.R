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
    matched    <- sensor_selected %>%
      mutate(name_n = .norm(.data$name)) %>%
      dplyr::filter(.data$name_n %in% .norm(study_names))
    wanted_ids <- pull(matched, .data$id)

    # TinyFox studies advertise only "sigfox-geolocation"; all tinyfox_* columns
    # are delivered as extra columns on location rows, not separate sensor rows.
    unmatched <- dplyr::anti_join(sensor_selected, matched, by = "id")
    if (nrow(unmatched) > 0)
      .msg("  Note: ", nrow(unmatched), " requested sensor(s) not in study sensor_type_ids ",
           "(likely stored as flat columns): ",
           paste(unmatched$external_id, collapse = ", "))

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
    # If caller explicitly specified a single tag_type, apply uniformly.
    if (!is.null(tag_type) && length(tag_type) == 1) {
      x$tag_type <- tag_type
      return(x)
    }

    # Try to resolve model info from multiple possible sources, in priority order:
    #   1. Event-level column (from mt_as_event_attribute("model"))
    #   2. Track-data column: "tag_model", "model", or any column containing "model"
    #   3. Fallback: infer from sensor column presence on the rows themselves

    # Classify helper: maps any model/format string to a tag_type label.
    .classify_model <- function(s) {
      dplyr::case_when(
        grepl("Nano|NanoFox",              s, ignore.case = TRUE) ~ "nanofox",
        grepl("Tiny|TinyFoxBat|TinyFox",   s, ignore.case = TRUE) ~ "tinyfox",
        grepl("uWasp|SigfoxGH",            s, ignore.case = TRUE) ~ "uWasp",
        .default = "tinyfox"
      )
    }

    # Source 0: format_type on every event row — the most reliable indicator.
    # Movebank encodes the Sigfox format identifier here: "TinyFoxBatt", "NanoFox30Days".
    if ("format_type" %in% names(x) && !all(is.na(x[["format_type"]]))) {
      x$tag_type <- .classify_model(as.character(x[["format_type"]]))
      .msg("  tag_type: resolved from format_type. Distribution: ",
           paste(names(table(x$tag_type)), table(x$tag_type), sep = "=", collapse = ", "))
      return(x)
    }

    model_vec <- NULL

    # Source 1: event-level column already on x
    for (col in c("model", "tag_model")) {
      if (col %in% names(x) && !all(is.na(x[[col]]))) {
        model_vec <- as.character(x[[col]])
        .msg("  tag_type: resolved from event column '", col, "'")
        break
      }
    }

    # Source 2: track data
    if (is.null(model_vec)) {
      td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
      if (!is.null(td)) {
        td_model_col <- intersect(c("tag_model", "model"), names(td))[1]
        if (!is.na(td_model_col)) {
          track_id_vec  <- as.character(move2::mt_track_id(x))
          id_col        <- names(td)[1]   # first col is the track identifier
          td_lookup     <- stats::setNames(as.character(td[[td_model_col]]),
                                           as.character(td[[id_col]]))
          model_vec     <- td_lookup[track_id_vec]
          .msg("  tag_type: resolved from track data column '", td_model_col, "'")
        }
      }
    }

    # Apply classification from model string
    if (!is.null(model_vec)) {
      x$tag_type <- .classify_model(model_vec)
      .msg("  tag_type distribution: ",
           paste(names(table(x$tag_type)), table(x$tag_type), sep = "=", collapse = ", "))
      return(x)
    }

    # Source 3: infer from column presence (pressure column name differs by tag type)
    # tinyfox_pressure_min_last_24h → TinyFox
    # min_3h_pressure               → NanoFox
    # If neither found, default to tinyfox (most common in this project)
    has_tiny_pressure <- "tinyfox_pressure_min_last_24h" %in% names(x)
    has_nano_pressure <- "min_3h_pressure" %in% names(x)

    if (has_tiny_pressure && !has_nano_pressure) {
      .msg("  tag_type: no model info found; inferred 'tinyfox' from column presence.")
      x$tag_type <- "tinyfox"
    } else if (has_nano_pressure && !has_tiny_pressure) {
      .msg("  tag_type: no model info found; inferred 'nanofox' from column presence.")
      x$tag_type <- "nanofox"
    } else {
      .msg("  tag_type: no model info found; defaulting to 'tinyfox'. ",
           "Set tag_type= explicitly if incorrect.")
      x$tag_type <- "tinyfox"
    }
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
  # .add_tinyfox_altitude()
  #
  # TinyFox tags transmit 4 barometer messages per 22-hour activity window,
  # spaced ~30 minutes apart.  Each message reports the MINIMUM pressure
  # observed since the start of that window, so values are monotonically
  # non-increasing within a window (later messages can only hold the same
  # or lower pressure).  The final message therefore carries the truest
  # nightly minimum pressure, corresponding to the highest altitude reached.
  #
  # Steps:
  #   1. Extract barometer rows belonging to TinyFox tracks.
  #   2. Cluster messages into 22-hour windows using time gaps:
  #        - messages within a window are < 2 hours apart
  #        - successive windows are ~22 hours apart
  #      A new window is started whenever the gap to the previous message
  #      exceeds 2 hours (within each individual's track).
  #   3. Compute min(tinyfox_pressure_min_last_24h) per individual x window.
  #      This is the peak-altitude pressure for that night.
  #   4. Convert the window minimum pressure to altitude (m) using the
  #      standard hypsometric formula:
  #        altitude_m = 44330 * (1 - (P / P0) ^ (1/5.255))
  #      where P0 = 1013.25 hPa (ISA sea-level pressure).
  #   5. Propagate tinyfox_altitude_m onto every row (location and barometer)
  #      for the same individual within that window's time span, so that
  #      altitude is available when .make_location_metrics() runs.
  #
  # Output columns added to x:
  #   tinyfox_pressure_window_min  [hPa]  - minimum pressure of the window
  #   tinyfox_altitude_m           [m]    - altitude derived from window min pressure
  #   altitude_m                   [m]    - set/overwritten for TinyFox rows;
  #                                         NanoFox rows are left untouched
  # ---------------------------------------------------------------------------
  .add_tinyfox_altitude <- function(x,
                                    pressure_col    = "tinyfox_pressure_min_last_24h",
                                    gap_threshold_h = 2,
                                    P0              = 1013.25) {

    if (!pressure_col %in% names(x)) {
      .msg("  .add_tinyfox_altitude: '", pressure_col, "' column not found; skipping.")
      return(x)
    }

    # Hypsometric formula: altitude in metres from pressure in hPa
    pressure_to_alt <- function(P, p0 = P0) {
      44330 * (1 - (P / p0)^(1 / 5.255))
    }

    # ---- 1. Build a tidy barometer frame from any row with a non-NA pressure value ----
    # Do NOT gate on tag_type or sensor_type: both may be unreliable depending on
    # how Movebank stored the data and whether model metadata was available.
    # The column itself is the definitive indicator: if tinyfox_pressure_min_last_24h
    # is non-NA, this is a TinyFox pressure reading regardless of how rows are labelled.
    has_pressure <- !is.na(x[[pressure_col]])

    if (!any(has_pressure, na.rm = TRUE)) {
      .msg("  .add_tinyfox_altitude: no non-NA values in '", pressure_col, "'; skipping.")
      return(x)
    }

    n_pressure_rows <- sum(has_pressure, na.rm = TRUE)
    .msg("  .add_tinyfox_altitude: found ", n_pressure_rows, " rows with '",
         pressure_col, "' values.")

    is_tinyfox_baro <- has_pressure

    baro_df <- tibble::tibble(
      .row_idx   = which(is_tinyfox_baro),
      individual = as.character(x$individual_local_identifier[is_tinyfox_baro]),
      timestamp  = x$timestamp[is_tinyfox_baro],
      pressure   = as.numeric(x[[pressure_col]][is_tinyfox_baro])
    ) %>%
      dplyr::arrange(individual, timestamp)

    # ---- 2. Assign window IDs using time gaps within each individual ----
    # A new window begins whenever the gap to the previous message exceeds
    # gap_threshold_h hours.  The cumulative sum of gap flags gives a
    # monotonically increasing window index per individual.
    baro_df <- baro_df %>%
      dplyr::group_by(individual) %>%
      dplyr::mutate(
        gap_h      = as.numeric(difftime(timestamp,
                                         dplyr::lag(timestamp, default = timestamp[1]),
                                         units = "hours")),
        new_window = gap_h > gap_threshold_h,
        window_id  = cumsum(new_window)            # 0, 0, 0, 1, 1, 1, 1, ...
      ) %>%
      dplyr::ungroup()

    # ---- 3. Minimum pressure per individual x window ----
    window_summary <- baro_df %>%
      dplyr::group_by(individual, window_id) %>%
      dplyr::summarise(
        window_start          = min(timestamp),
        window_end            = max(timestamp),
        pressure_window_min   = min(pressure, na.rm = TRUE),
        n_messages            = dplyr::n(),
        .groups               = "drop"
      ) %>%
      dplyr::mutate(
        tinyfox_altitude_m = pressure_to_alt(pressure_window_min)
      )

    .msg("  TinyFox windows identified: ", nrow(window_summary),
         " (", round(mean(window_summary$n_messages), 1),
         " msgs/window on average)")
    .msg("  Altitude range: [",
         round(min(window_summary$tinyfox_altitude_m, na.rm = TRUE), 0), ", ",
         round(max(window_summary$tinyfox_altitude_m, na.rm = TRUE), 0), "] m")

    # ---- 4. Join window summary back to baro rows ----
    baro_df <- baro_df %>%
      dplyr::left_join(
        window_summary %>% dplyr::select(individual, window_id,
                                         pressure_window_min, tinyfox_altitude_m),
        by = c("individual", "window_id")
      )

    # Write derived columns back to the full object on barometer rows
    x$tinyfox_pressure_window_min <- NA_real_
    x$tinyfox_altitude_m          <- NA_real_
    x$tinyfox_pressure_window_min[baro_df$.row_idx] <- baro_df$pressure_window_min
    x$tinyfox_altitude_m[baro_df$.row_idx]          <- baro_df$tinyfox_altitude_m

    # ---- 5. Propagate window-minimum altitude back onto rows ----
    #
    # FLAT schema (TinyFox): pressure and location share the same row.
    #   Write window-minimum altitude directly onto the pressure row indices.
    #   No cross-join needed.
    #
    # SEPARATE schema (NanoFox): barometer rows differ from location rows.
    #   Propagate via interval join over [window_start − 22h, window_end + 1h].

    flat_schema <- !"sensor_type" %in% names(x) ||
      all(x$sensor_type[is_tinyfox_baro] %in%
            c("location", NA_character_), na.rm = TRUE)

    if (!"altitude_m" %in% names(x)) x$altitude_m <- NA_real_

    if (flat_schema) {
      x$altitude_m[baro_df$.row_idx] <- baro_df$tinyfox_altitude_m

    } else {
      all_df <- tibble::tibble(
        .row_idx   = seq_len(nrow(x)),
        individual = as.character(x$individual_local_identifier),
        timestamp  = x$timestamp
      )
      window_intervals <- window_summary %>%
        dplyr::mutate(
          window_start_buffered = window_start - lubridate::dhours(22),
          window_end_buffered   = window_end   + lubridate::dhours(1)
        ) %>%
        dplyr::select(individual, window_start, window_start_buffered,
                      window_end_buffered, tinyfox_altitude_m)

      matched <- all_df %>%
        dplyr::left_join(window_intervals, by = "individual",
                         relationship = "many-to-many") %>%
        dplyr::filter(timestamp >= window_start_buffered &
                        timestamp <= window_end_buffered) %>%
        dplyr::mutate(dist_to_window = as.numeric(abs(difftime(
          timestamp, window_start, units = "secs")))) %>%
        dplyr::group_by(.row_idx) %>%
        dplyr::slice_min(dist_to_window, n = 1, with_ties = FALSE) %>%
        dplyr::ungroup()

      already_has_alt <- !is.na(x$altitude_m)
      fill_idx <- matched$.row_idx[!already_has_alt[matched$.row_idx]]
      x$altitude_m[fill_idx] <- matched$tinyfox_altitude_m[
        match(fill_idx, matched$.row_idx)]
    }

    # Apply units
    x$tinyfox_pressure_window_min <- units::set_units(x$tinyfox_pressure_window_min, "hPa")
    x$tinyfox_altitude_m          <- units::set_units(x$tinyfox_altitude_m,          "m")
    x$altitude_m                  <- units::set_units(as.numeric(x$altitude_m),      "m")

    x
  }


  # ---------------------------------------------------------------------------
  # .expand_tinyfox_rows()
  #
  # TinyFox downloads store all sensor data as extra columns on location rows.
  # This function unpacks them into separate rows — one per sensor measurement
  # per transmission — so the full object `b` has the same tidy structure as a
  # NanoFox multi-sensor download.
  #
  # Columns processed:
  #
  #   tinyfox_vedba_NNh_ago  [m/s^2]
  #     One row per 1-hour bin.  The suffix NN gives the number of hours before
  #     transmission that the window ENDS.
  #       time_end   = timestamp - (NN - 1) hours
  #       time_start = timestamp - NN hours
  #     sensor_type = "VeDBA"
  #
  #   tinyfox_total_vedba  [m/s^2]
  #     One row covering the full 22-hour activity window.
  #       time_start = timestamp - 22 hours,  time_end = timestamp
  #     sensor_type = "VeDBA"
  #
  #   tinyfox_pressure_min_last_24h  [mbar]
  #     One row, 24-hour window.
  #     sensor_type = "min.baro.pressure"
  #
  #   tinyfox_temperature_min_last_24h / tinyfox_temperature_max_last_24h  [°C]
  #     One row each, 24-hour window.
  #     sensor_type = "avg.temp"
  #
  #   tinyfox_activity_percent_last_24h  [%]
  #     One row, 24-hour window.
  #     sensor_type = "activity"
  #
  # All new rows inherit:
  #   - geometry from their source location row
  #   - individual_local_identifier, tag_type, and all track-level fields
  #   - timestamp set to time_end (Movebank convention)
  #   - all tinyfox_* columns set to NA (those belong only to the location row)
  #
  # The original location rows also receive time_start = time_end = timestamp.
  #
  # Returns a move2/sf object with the original rows plus the new sensor rows,
  # sorted by individual and timestamp.
  # ---------------------------------------------------------------------------
  .expand_tinyfox_rows <- function(x) {

    # Only run if the characteristic column exists
    if (!"tinyfox_pressure_min_last_24h" %in% names(x) &&
        !any(grepl("^tinyfox_vedba_\\d+h_ago$", names(x)))) {
      .msg("  .expand_tinyfox_rows: no TinyFox sensor columns found; skipping.")
      return(x)
    }

    require(sf); require(dplyr); require(tidyr); require(lubridate)

    # Identify column groups
    vedba_h_cols  <- sort(names(x)[grepl("^tinyfox_vedba_\\d+h_ago$", names(x))])
    total_v_col   <- if ("tinyfox_total_vedba"                  %in% names(x)) "tinyfox_total_vedba"                  else NULL
    pres_col      <- if ("tinyfox_pressure_min_last_24h"        %in% names(x)) "tinyfox_pressure_min_last_24h"        else NULL
    tmin_col      <- if ("tinyfox_temperature_min_last_24h"     %in% names(x)) "tinyfox_temperature_min_last_24h"     else NULL
    tmax_col      <- if ("tinyfox_temperature_max_last_24h"     %in% names(x)) "tinyfox_temperature_max_last_24h"     else NULL
    act_col       <- if ("tinyfox_activity_percent_last_24h"    %in% names(x)) "tinyfox_activity_percent_last_24h"    else NULL

    # All tinyfox_* columns that will be NA'd on non-location rows
    all_tf_cols <- c(vedba_h_cols, total_v_col, pres_col, tmin_col, tmax_col, act_col,
                     "tinyfox_tag_activation") %>% intersect(names(x))

    # Helper: build a derived-sensor tibble from one scalar column
    # Each source row → one output row with its own time window
    .make_scalar_rows <- function(src_col, value_col_name, sensor_lbl,
                                  window_hours_start, window_hours_end = 0) {
      if (is.null(src_col) || !src_col %in% names(x)) return(NULL)
      vals <- as.numeric(x[[src_col]])
      keep <- !is.na(vals)
      if (!any(keep)) return(NULL)

      sf::st_as_sf(tibble::tibble(
        .src_row                    = seq_len(nrow(x))[keep],
        individual_local_identifier = x$individual_local_identifier[keep],
        timestamp                   = x$timestamp[keep] -
          lubridate::dhours(window_hours_end),
        time_start                  = x$timestamp[keep] -
          lubridate::dhours(window_hours_start),
        time_end                    = x$timestamp[keep] -
          lubridate::dhours(window_hours_end),
        sensor_type                 = sensor_lbl,
        !!value_col_name            := vals[keep],
        geometry                    = sf::st_geometry(x)[keep]
      ))
    }

    # ---- 1. Hourly VeDBA bins ----
    # tinyfox_vedba_NNh_ago → N hours before tx is when the window ends.
    # Window: [tx - NN h, tx - (NN-1) h]
    vedba_h_rows <- NULL
    if (length(vedba_h_cols) > 0) {
      vedba_h_rows <- lapply(vedba_h_cols, function(col) {
        nn  <- as.integer(sub("tinyfox_vedba_(\\d+)h_ago", "\\1", col))
        vals <- as.numeric(x[[col]])
        keep <- !is.na(vals)
        if (!any(keep)) return(NULL)

        sf::st_as_sf(tibble::tibble(
          .src_row                    = seq_len(nrow(x))[keep],
          individual_local_identifier = x$individual_local_identifier[keep],
          time_end                    = x$timestamp[keep] - lubridate::dhours(nn - 1L),
          time_start                  = x$timestamp[keep] - lubridate::dhours(nn),
          timestamp                   = x$timestamp[keep] - lubridate::dhours(nn - 1L),
          sensor_type                 = "VeDBA",
          vedba                       = vals[keep],
          geometry                    = sf::st_geometry(x)[keep]
        ))
      })
      vedba_h_rows <- dplyr::bind_rows(Filter(Negate(is.null), vedba_h_rows))
    }

    # ---- 2. Total VeDBA (full 22-hour window) ----
    total_v_rows <- .make_scalar_rows(
      total_v_col, "vedba", "VeDBA",
      window_hours_start = 22, window_hours_end = 0
    )

    # ---- 3. Pressure ----
    pres_rows <- .make_scalar_rows(
      pres_col, "barometric_pressure", "min.baro.pressure",
      window_hours_start = 24, window_hours_end = 0
    )

    # ---- 4. Temperature (min and max as separate rows) ----
    tmin_rows <- .make_scalar_rows(
      tmin_col, "external_temperature_min", "avg.temp",
      window_hours_start = 24, window_hours_end = 0
    )
    tmax_rows <- .make_scalar_rows(
      tmax_col, "external_temperature_max", "avg.temp",
      window_hours_start = 24, window_hours_end = 0
    )

    # ---- 5. Activity ----
    act_rows <- .make_scalar_rows(
      act_col, "activity_percent", "activity",
      window_hours_start = 24, window_hours_end = 0
    )

    # ---- 6. Add time_start / time_end to original location rows ----
    x$time_start <- x$timestamp
    x$time_end   <- x$timestamp
    # NA out tinyfox sensor columns on location rows is intentional —
    # they are now represented as dedicated rows.
    # (Keep them on location rows for backward compatibility; mark with a note.)

    # ---- 7. Collect all new sensor rows ----
    new_rows_list <- Filter(
      Negate(is.null),
      list(vedba_h_rows, total_v_rows, pres_rows, tmin_rows, tmax_rows, act_rows)
    )

    if (length(new_rows_list) == 0) {
      .msg("  .expand_tinyfox_rows: no non-NA sensor values to expand.")
      return(x)
    }

    # Helper: create an NA vector that matches the class/units of a source column.
    # Handles units, factors, and plain vectors safely without vctrs::vec_cast
    # (which errors on geometry and other complex column types).
    .typed_na <- function(src, n) {
      if (inherits(src, "units"))
        return(units::set_units(rep(NA_real_, n),
                                units::deparse_unit(src), mode = "standard"))
      if (is.factor(src))
        return(factor(rep(NA_character_, n), levels = levels(src)))
      if (is.integer(src))   return(rep(NA_integer_,   n))
      if (is.numeric(src))   return(rep(NA_real_,      n))
      if (is.logical(src))   return(rep(NA,            n))
      if (is.character(src)) return(rep(NA_character_,  n))
      if (inherits(src, "POSIXct"))
        return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
      if (inherits(src, "sfc"))  # geometry — copy a null/empty geometry column
        return(sf::st_sfc(lapply(seq_len(n), function(...) sf::st_point()), crs = sf::st_crs(src)))
      rep(NA, n)   # fallback
    }

    # Align all sub-tibbles in new_rows_list to the same column schema as x
    # BEFORE binding, so the internal bind_rows sees consistent types.
    x_cols      <- names(x)
    x_sf        <- sf::st_drop_geometry(x)   # drop geometry for type reference

    new_rows_list <- lapply(new_rows_list, function(sub) {
      n <- nrow(sub)
      # Add columns present in x but missing from this sub-tibble
      for (col in setdiff(x_cols, c(names(sub), "geometry"))) {
        src <- x_sf[[col]]
        if (is.null(src)) next
        sub[[col]] <- .typed_na(src, n)
      }
      # Remove any extra columns not in x (value columns like "vedba", "barometric_pressure")
      # are kept as they will become NAs in the other sub-tibbles
      sub
    })

    # Now bind — all sub-tibbles share the same base columns with matching types
    new_rows <- dplyr::bind_rows(new_rows_list)

    # Zero out tinyfox_* columns on new rows (those values live on location rows)
    for (col in intersect(all_tf_cols, names(new_rows))) {
      new_rows[[col]] <- .typed_na(x[[col]], nrow(new_rows))
    }

    # Final column alignment: add any remaining x-columns still missing
    for (col in setdiff(names(x), names(new_rows))) {
      src <- x[[col]]
      if (inherits(src, "sfc")) next   # geometry handled by st_as_sf below
      new_rows[[col]] <- .typed_na(src, nrow(new_rows))
    }
    # Reorder to match x (geometry column may differ in position — that's fine)
    shared_cols <- intersect(names(x), names(new_rows))
    new_rows    <- new_rows[, shared_cols, drop = FALSE]

    # ---- 8. Stack original + new rows and re-cast to move2 ----
    # Use only columns present in both to avoid geometry/type conflicts.
    x_out       <- sf::st_as_sf(x)[, shared_cols, drop = FALSE]
    new_rows_sf <- sf::st_as_sf(new_rows)
    # Ensure new_rows_sf has the geometry column from their source rows
    if (!"geometry" %in% names(new_rows_sf))
      sf::st_geometry(new_rows_sf) <- sf::st_geometry(x)[new_rows$.src_row %% nrow(x) + 1L]

    combined <- dplyr::bind_rows(x_out, new_rows_sf) %>%
      dplyr::arrange(individual_local_identifier, timestamp)

    # Re-establish move2 class (bind_rows strips it)
    combined <- move2::mt_as_move2(
      combined,
      track_id_column   = "individual_local_identifier",
      time_column       = "timestamp",
      crs               = sf::st_crs(x)
    )

    n_new <- nrow(combined) - nrow(x)
    .msg("  .expand_tinyfox_rows: added ", n_new, " sensor rows. ",
         "Total rows: ", nrow(combined),
         " | sensor_type distribution:")
    if (isTRUE(verbose)) print(table(combined$sensor_type, useNA = "ifany"))

    combined
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

    # TODO add cumsum distance

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

    # Diagnostic: report a safe summary of what came back from the download.
    # Never paste raw column values — only names and table summaries.
    if (isTRUE(verbose)) {
      col_names <- names(b)
      .msg("  Downloaded: ", nrow(b), " rows x ", ncol(b), " columns.")
      .msg("  Columns: ", paste(col_names, collapse = ", "))
      if ("sensor_type" %in% col_names) {
        .msg("  sensor_type distribution:")
        print(table(b$sensor_type, useNA = "ifany"))
      }
      if ("tag_type" %in% col_names)
        .msg("  tag_type: ",
             paste(names(table(b$tag_type)), table(b$tag_type), sep = "=", collapse = ", "))
      pcols <- grep("pressure|baro|altitude|tinyfox", col_names, value = TRUE, ignore.case = TRUE)
      if (length(pcols)) .msg("  TinyFox/pressure columns: ", paste(pcols, collapse = ", "))
    }
    b <- .safe_try(.add_start(b), "mt_add_start") %||% b

    .source_local(script_add_vedba_temp)
    b <- .safe_try(add_vedba_temp_to_locations(df = b),   "add_vedba_temp_to_locations")   %||% b

    .source_local(script_add_min_pressure)
    b <- .safe_try(add_min_pressure_to_locations(df = b), "add_min_pressure_to_locations") %||% b

    # TinyFox: group the 4 repeated barometer messages into 22-hour windows,
    # derive the nightly minimum pressure, convert to altitude, and propagate
    # to location rows.  Must run before add_altitude_from_pressure() so that
    # altitude_m is already populated for TinyFox rows when that function runs.
    b <- .safe_try(.add_tinyfox_altitude(b), "add_tinyfox_altitude") %||% b

    # NanoFox: convert min_3h_pressure to altitude_m.
    # For TinyFox rows, altitude_m was already set above; the sourced script
    # should skip / not overwrite rows where altitude_m is already populated —
    # pass tiny_pressure_col = NULL to signal that TinyFox is handled.
    .source_local(script_pressure_to_altitude)
    b <- .safe_try(
      add_altitude_from_pressure(df = b,
                                 nano_pressure_col = "min_3h_pressure",
                                 tiny_pressure_col = NULL,   # handled by .add_tinyfox_altitude()
                                 altitude_col      = "altitude_m"),
      "add_altitude_from_pressure"
    ) %||% b

    # Expand TinyFox flat-schema columns into separate sensor rows (one per
    # measurement type per transmission), adding time_start and time_end.
    # Must run AFTER .add_tinyfox_altitude() so altitude_m is already on rows,
    # and BEFORE .add_night_day_id() so new rows get night/day labels too.
    b <- .safe_try(.expand_tinyfox_rows(b), "expand_tinyfox_rows") %||% b

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
  # Strip units from a move2/sf object before stacking so that columns that
  # carry units in one study but not another (or carry different units) don't
  # cause bind_rows to error.  Units metadata is recorded as an attribute on
  # each column so it can be reattached if needed; stripping to double is safe
  # because the physical meaning is preserved by the column name.
  .strip_units_cols <- function(obj) {
    if (is.null(obj) || !is.data.frame(obj)) return(obj)
    for (col in names(obj)) {
      if (inherits(obj[[col]], "units"))
        obj[[col]] <- as.numeric(obj[[col]])
    }
    obj
  }

  merge_stack <- function(objs) {
    objs <- Filter(Negate(is.null), objs)
    if (length(objs) == 0) return(NULL)
    if (length(objs) == 1) return(objs[[1]])
    # Normalise units across all objects before stacking
    objs <- lapply(objs, .strip_units_cols)
    out  <- objs[[1]]
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
    # if ("speed" %in% names(loc_merged))
    #   .msg("Speed units (location): ", deparse(units::deparse_unit(loc_merged$speed)))
    # if ("delta_altitude_m" %in% names(loc_merged))
    #   .msg("Delta altitude units (location): ",
    #        deparse(units::deparse_unit(loc_merged$delta_altitude_m)))
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

# x <- {}
# x <- import_nanofox_movebank(study_id = c(4358705312, 4882437204), daily_method = "daytime_only")
# x$location$altitude_m %>% summary()
