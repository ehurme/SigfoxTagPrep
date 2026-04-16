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
    compute_cum_dist = TRUE,
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

  # Choose the most specific identifier available for movement metrics.
  # Prefer deployment_id, then tag_local_identifier, then individual_local_identifier,
  # and finally the internal move2 track id.
  .metric_group_col <- function(x) {
    for (nm in c("deployment_id", "tag_local_identifier", "individual_local_identifier")) {
      if (nm %in% names(x) && !all(is.na(x[[nm]]))) return(nm)
    }
    NULL
  }

  # Enforce strict time ordering per track (required by mt_speed / mt_distance).
  .dedupe_timestamps <- function(x) {
    grp_col <- .metric_group_col(x)
    if (is.null(grp_col)) {
      x <- x %>% dplyr::mutate(`..metric_group_id` = as.character(move2::mt_track_id(.)))
      grp_col <- "..metric_group_id"
    }

    # Some studies do not include comments; prefer non-start rows only if comments exists.
    ord_comments <- if ("comments" %in% names(x)) {
      (is.na(x$comments) | x$comments != "start")
    } else {
      rep(TRUE, nrow(x))
    }
    ord_event <- if ("event_id" %in% names(x)) (!is.na(x$event_id)) else rep(FALSE, nrow(x))
    x <- x %>% dplyr::mutate(`..dedupe_order` = ord_comments + 0.1 * ord_event)

    out <- x %>%
      dplyr::group_by(.data[[grp_col]], .data$timestamp) %>%
      dplyr::slice_max(
        order_by  = .data$`..dedupe_order`,
        n         = 1,
        with_ties = FALSE
      ) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data[[grp_col]], .data$timestamp) %>%
      dplyr::select(-dplyr::any_of(c("..dedupe_order", "..metric_group_id")))
    out
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

  .set_tag_type <- function(x, study_id_current = NULL) {
    allowed <- c("uWasp", "nanofox", "tinyfox")

    .classify_model <- function(s) {
      s <- as.character(s)
      out <- dplyr::case_when(
        grepl("uWasp|SigfoxGH", s, ignore.case = TRUE) ~ "uWasp",
        grepl("Nano|NanoFox",  s, ignore.case = TRUE) ~ "nanofox",
        grepl("Tinyfox|TinyFoxBatt",  s, ignore.case = TRUE) ~ "tinyfox",
        TRUE ~ NA_character_
      )
      out
    }

    # 1) Prefer existing event-level tag_type if it is already valid.
    if ("tag_type" %in% names(x) && any(!is.na(x$tag_type))) {
      x$tag_type <- as.character(x$tag_type)
      bad <- !is.na(x$tag_type) & !x$tag_type %in% allowed
      if (any(bad)) {
        x$tag_type[bad] <- NA_character_
      }
      if (any(!is.na(x$tag_type))) {
        .msg("  tag_type: using existing event/data column. Distribution: ",
             paste(names(table(x$tag_type, useNA = "no")),
                   table(x$tag_type, useNA = "no"), sep = "=", collapse = ", "))
        return(x)
      }
    }

    # 2) Prefer explicit per-study override only when tag_type is not inferable.
    override <- NULL
    if (!is.null(tag_type)) {
      if (length(tag_type) == 1 && !is.na(tag_type)) {
        override <- as.character(tag_type)
      } else if (!is.null(names(tag_type)) && !is.null(study_id_current)) {
        sid <- as.character(study_id_current)
        if (sid %in% names(tag_type)) override <- as.character(tag_type[[sid]])
      }
      if (!is.null(override) && !is.na(override) && !override %in% allowed) {
        stop("Invalid tag_type override for study ", study_id_current,
             ": ", override, ". Allowed values are: ",
             paste(allowed, collapse = ", "))
      }
    }

    # 3) Infer from format_type if available.
    inferred <- NULL
    if ("format_type" %in% names(x) && any(!is.na(x[["format_type"]]))) {
      inferred <- .classify_model(x[["format_type"]])
      if (any(!is.na(inferred))) {
        x$tag_type <- inferred
        .msg("  tag_type: resolved from format_type. Distribution: ",
             paste(names(table(x$tag_type, useNA = "no")),
                   table(x$tag_type, useNA = "no"), sep = "=", collapse = ", "))
        return(x)
      }
    }

    # 4) Infer from event-level or track-level model metadata.
    model_vec <- NULL
    for (col in c("model", "tag_model")) {
      if (col %in% names(x) && any(!is.na(x[[col]]))) {
        model_vec <- as.character(x[[col]])
        .msg("  tag_type: resolved from event column '", col, "'")
        break
      }
    }
    if (is.null(model_vec)) {
      td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
      if (!is.null(td)) {
        td_model_cols <- intersect(c("tag_model", "model"), names(td))
        if (length(td_model_cols) > 0) {
          td_model_col <- td_model_cols[1]
          track_id_vec <- as.character(move2::mt_track_id(x))
          id_col <- names(td)[1]
          td_lookup <- stats::setNames(as.character(td[[td_model_col]]),
                                       as.character(td[[id_col]]))
          model_vec <- td_lookup[track_id_vec]
          .msg("  tag_type: resolved from track data column '", td_model_col, "'")
        }
      }
    }
    if (!is.null(model_vec)) {
      inferred <- .classify_model(model_vec)
      if (any(!is.na(inferred))) {
        x$tag_type <- inferred
        .msg("  tag_type distribution: ",
             paste(names(table(x$tag_type, useNA = "no")),
                   table(x$tag_type, useNA = "no"), sep = "=", collapse = ", "))
        return(x)
      }
    }

    # 5) Use explicit override only for studies where metadata are insufficient.
    if (!is.null(override) && !is.na(override)) {
      x$tag_type <- rep(override, nrow(x))
      .msg("  tag_type set explicitly for unresolved study: ", override)
      return(x)
    }

    stop("tag_type could not be inferred for study ", as.character(study_id_current),
         ". Supply tag_type as a named vector for only the unresolved studies. ",
         "Allowed values are: ", paste(allowed, collapse = ", "), ".")
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
    # When individual_local_identifier is used as the track ID, Movebank may
    # bundle multiple deployments (different tags, capture dates, body mass, etc.)
    # for the same individual into list-columns in the track data.  This is
    # expected move2 behaviour. We collapse those list-columns without expanding
    # rows, and we treat geometry columns specially because unique()/paste() can
    # fail on sfc MULTIPOINT objects.

    td <- move2::mt_track_data(x)
    if (!any(vapply(td, base::is.list, logical(1)))) return(x)

    .msg("  Track data has list-columns; resolving multiple deployments per individual.")

    .is_geom_like <- function(z) {
      inherits(z, "sfc") || inherits(z, "sf") || inherits(z, "sfg") ||
        any(grepl("^XY|^POINT|^MULTIPOINT|^LINESTRING|^MULTILINESTRING|^POLYGON|^MULTIPOLYGON|^GEOMETRY", class(z)))
    }

    .canon_one <- function(v) {
      if (length(v) == 0 || all(is.na(v))) return(NA_character_)
      if (.is_geom_like(v)) {
        out <- tryCatch(as.character(sf::st_as_text(v)), error = function(e) NA_character_)
        return(paste(out, collapse = " | "))
      }
      if (inherits(v, c("POSIXct", "POSIXt"))) {
        return(paste(format(v, tz = "UTC", usetz = TRUE), collapse = " | "))
      }
      if (inherits(v, "Date")) {
        return(paste(as.character(v), collapse = " | "))
      }
      if (is.atomic(v)) {
        return(paste(as.character(v), collapse = " | "))
      }
      paste(as.character(v), collapse = " | ")
    }

    .all_same <- function(y) {
      vals <- tryCatch(vapply(y, .canon_one, character(1)), error = function(e) rep(NA_character_, length(y)))
      vals2 <- unique(stats::na.omit(vals))
      length(vals2) <= 1L
    }

    .collapse_list_entry <- function(y) {
      if (length(y) == 0) return(NA)
      if (.all_same(y)) {
        first <- y[[1]]
        if (.is_geom_like(first)) {
          txt <- tryCatch(as.character(sf::st_as_text(first)), error = function(e) NA_character_)
          return(if (length(txt)) txt[[1]] else NA_character_)
        }
        if (length(first) <= 1L) return(first)
        return(first[[1]])
      }
      paste(vapply(y, .canon_one, character(1)), collapse = ", ")
    }

    list_cols <- names(td)[vapply(td, base::is.list, logical(1))]
    for (nm in list_cols) {
      td[[nm]] <- lapply(td[[nm]], .collapse_list_entry)

      scalar_classes <- unique(vapply(td[[nm]], function(z) paste(class(z), collapse = "/"), character(1)))
      scalar_classes <- stats::na.omit(scalar_classes)

      if (length(scalar_classes) == 1L && !grepl("character", scalar_classes[1], fixed = TRUE)) {
        simplified <- tryCatch(vctrs::list_unchop(td[[nm]]), error = function(e) NULL)
        if (!is.null(simplified)) td[[nm]] <- simplified
      } else {
        td[[nm]] <- vapply(td[[nm]], function(z) {
          if (length(z) == 0 || (length(z) == 1 && is.na(z))) return(NA_character_)
          if (.is_geom_like(z)) {
            txt <- tryCatch(as.character(sf::st_as_text(z)), error = function(e) NA_character_)
            return(paste(txt, collapse = " | "))
          }
          paste(as.character(z), collapse = " | ")
        }, character(1))
      }
    }

    if (isTRUE(verbose)) {
      list_cols_after <- names(td)[vapply(td, base::is.list, logical(1))]
      if (length(list_cols_after) == 0)
        .msg("  Track data list-columns resolved.")
      else
        .msg("  Track data still has list-columns after resolution: ",
             paste(list_cols_after, collapse = ", "))
    }

    move2::mt_set_track_data(x, td)
  }

  # ---------------------------------------------------------------------------
  # .add_altitude_from_pressure_col()
  #
  # Converts any pressure column to altitude_m using the ISA hypsometric formula:
  #   altitude_m = 44330 * (1 - (P / 1013.25) ^ (1/5.255))
  #
  # Handles both tag types with a single unified approach:
  #
  #   TinyFox (flat schema): tinyfox_pressure_min_last_24h [mbar] is a column
  #     on every location row. Convert it directly — no grouping, no joining.
  #     Each row carries its own pressure value.
  #
  #   NanoFox (separate-row schema): min_3h_pressure [mbar] is joined onto
  #     location rows by add_min_pressure_to_locations() before this function.
  #     Convert it directly in the same way.
  #
  # Priority when both columns exist (mixed-tag study):
  #   NanoFox min_3h_pressure > TinyFox tinyfox_pressure_min_last_24h
  #
  # Writes altitude_m [m] to x. Never overwrites an existing non-NA value.
  # ---------------------------------------------------------------------------
  .add_altitude_from_pressure_col <- function(x, P0 = 1013.25) {
    pressure_to_alt <- function(P) 44330 * (1 - (as.numeric(P) / P0)^(1 / 5.255))

    # Initialise altitude_m if absent
    if (!"altitude_m" %in% names(x)) x$altitude_m <- NA_real_
    existing <- !is.na(as.numeric(x$altitude_m))

    # NanoFox: min_3h_pressure (joined from barometer sensor rows)
    if ("min_3h_pressure" %in% names(x)) {
      fill <- !existing & !is.na(as.numeric(x$min_3h_pressure))
      if (any(fill, na.rm = TRUE)) {
        x$altitude_m[fill] <- pressure_to_alt(x$min_3h_pressure[fill])
        existing <- existing | fill
        .msg("  altitude_m: ", sum(fill, na.rm = TRUE),
             " NanoFox rows filled from min_3h_pressure.")
      }
    }

    # TinyFox: tinyfox_pressure_min_last_24h (flat column on location rows)
    if ("tinyfox_pressure_min_last_24h" %in% names(x)) {
      fill <- !existing & !is.na(as.numeric(x$tinyfox_pressure_min_last_24h))
      if (any(fill, na.rm = TRUE)) {
        x$altitude_m[fill] <- pressure_to_alt(x$tinyfox_pressure_min_last_24h[fill])
        existing <- existing | fill
        .msg("  altitude_m: ", sum(fill, na.rm = TRUE),
             " TinyFox rows filled from tinyfox_pressure_min_last_24h.")
      }
    }

    # Attach units
    x$altitude_m <- units::set_units(as.numeric(x$altitude_m), "m")
    x
  }


  # ---------------------------------------------------------------------------
  # .add_temperature_to_locations()
  #
  # Ensures temperature columns are present on location rows for both tag types.
  # Produces three unified columns (never overwrites existing non-NA values):
  #
  #   temperature_min  [°C]  – minimum temperature over the reporting window
  #   temperature_max  [°C]  – maximum temperature over the reporting window
  #   avg_temp         [°C]  – mean of min+max (TinyFox) or per-window average
  #                            (NanoFox, from external_temperature joined by
  #                            add_vedba_temp_to_locations())
  #
  # TinyFox (flat schema):
  #   tinyfox_temperature_min_last_24h and tinyfox_temperature_max_last_24h are
  #   columns on every location row. Copied directly; no join required.
  #
  # NanoFox (separate-row schema):
  #   Temperature is stored in separate accessory-measurement sensor rows with
  #   sensor_type == "avg.temp" and value column "external_temperature".
  #   add_vedba_temp_to_locations() (sourced script) should have already joined
  #   these onto location rows. If it did, external_temperature is present.
  #   If it failed, we do a nearest-timestamp join here as a fallback.
  # ---------------------------------------------------------------------------
  .add_temperature_to_locations <- function(x) {

    # ---- TinyFox flat columns ----
    has_tmin <- "tinyfox_temperature_min_last_24h" %in% names(x)
    has_tmax <- "tinyfox_temperature_max_last_24h" %in% names(x)

    if (has_tmin || has_tmax) {
      if (!"temperature_min" %in% names(x)) x$temperature_min <- NA_real_
      if (!"temperature_max" %in% names(x)) x$temperature_max <- NA_real_
      if (!"avg_temp"        %in% names(x)) x$avg_temp        <- NA_real_

      if (has_tmin) {
        fill <- is.na(x$temperature_min) & !is.na(as.numeric(x$tinyfox_temperature_min_last_24h))
        x$temperature_min[fill] <- as.numeric(x$tinyfox_temperature_min_last_24h[fill])
      }
      if (has_tmax) {
        fill <- is.na(x$temperature_max) & !is.na(as.numeric(x$tinyfox_temperature_max_last_24h))
        x$temperature_max[fill] <- as.numeric(x$tinyfox_temperature_max_last_24h[fill])
      }

      # avg_temp from mid-range where both min and max are available
      fill_avg <- is.na(x$avg_temp) &
        !is.na(x$temperature_min) & !is.na(x$temperature_max)
      x$avg_temp[fill_avg] <- (x$temperature_min[fill_avg] + x$temperature_max[fill_avg]) / 2

      # avg_temp from whichever single value is available
      fill_min_only <- is.na(x$avg_temp) & !is.na(x$temperature_min)
      x$avg_temp[fill_min_only] <- x$temperature_min[fill_min_only]
      fill_max_only <- is.na(x$avg_temp) & !is.na(x$temperature_max)
      x$avg_temp[fill_max_only] <- x$temperature_max[fill_max_only]

      n_filled <- sum(!is.na(x$avg_temp), na.rm = TRUE)
      .msg("  temperature: ", n_filled, " TinyFox location rows have avg_temp.")
    }

    # ---- NanoFox: external_temperature already on location rows (from sourced script) ----
    if ("external_temperature" %in% names(x)) {
      if (!"avg_temp" %in% names(x)) x$avg_temp <- NA_real_
      fill <- is.na(x$avg_temp) & !is.na(as.numeric(x$external_temperature))
      if (any(fill, na.rm = TRUE)) {
        x$avg_temp[fill] <- as.numeric(x$external_temperature[fill])
        .msg("  temperature: ", sum(fill, na.rm = TRUE),
             " NanoFox location rows filled avg_temp from external_temperature.")
      }
    }

    # ---- NanoFox fallback: join from avg.temp sensor rows if still missing ----
    # If add_vedba_temp_to_locations() failed, sensor_type=="avg.temp" rows carry
    # external_temperature. Join the temporally nearest reading per individual.
    if ("sensor_type" %in% names(x) &&
        any(x$sensor_type == "avg.temp", na.rm = TRUE)) {

      if (!"avg_temp" %in% names(x)) x$avg_temp <- NA_real_

      loc_idx  <- which(x$sensor_type == "location" & is.na(x$avg_temp))
      temp_idx <- which(x$sensor_type == "avg.temp" &
                          !is.na(as.numeric(x$external_temperature)))

      if (length(loc_idx) > 0 && length(temp_idx) > 0) {
        loc_df  <- tibble::tibble(
          .loc_row   = loc_idx,
          individual = as.character(x$individual_local_identifier[loc_idx]),
          timestamp  = x$timestamp[loc_idx]
        )
        temp_df <- tibble::tibble(
          individual   = as.character(x$individual_local_identifier[temp_idx]),
          temp_ts      = x$timestamp[temp_idx],
          ext_temp_val = as.numeric(x$external_temperature[temp_idx])
        )
        joined <- loc_df %>%
          dplyr::left_join(temp_df, by = "individual",
                           relationship = "many-to-many") %>%
          dplyr::mutate(dt = abs(as.numeric(difftime(timestamp, temp_ts, units = "secs")))) %>%
          dplyr::group_by(.loc_row) %>%
          dplyr::slice_min(dt, n = 1, with_ties = FALSE) %>%
          dplyr::ungroup()

        x$avg_temp[joined$.loc_row] <- joined$ext_temp_val
        .msg("  temperature: ", nrow(joined),
             " NanoFox location rows filled avg_temp via nearest-sensor join.")
      }
    }

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

    # Group by the move2 track id (which should already be deployment_id when
    # called from .make_location_metrics after re-keying).
    group_vec <- as.character(move2::mt_track_id(x))

    delta <- tibble::tibble(
      track_id   = group_vec,
      altitude_m = as.numeric(x$altitude_m)
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
  # Speed        : km/h  — "km/h" is the correct SI-derived units symbol.
  #               "km/hr" is not recognised by the units package and will error.
  # Distance      : km
  # dt            : seconds (numeric; used as-is for climb-rate arithmetic)
  # cum_dist_km   : cumulative distance from first fix per track (km, plain numeric)
  #                 NA at the first fix; controlled by compute_cum_dist parameter.
  # delta_altitude_m: metres with explicit units (via .add_delta_altitude)
  # ---------------------------------------------------------------------------
  .make_location_metrics <- function(x) {
    b_loc <- x %>%
      dplyr::filter(.data$sensor_type == "location", !sf::st_is_empty(.data$geometry))

    b_loc <- .dedupe_timestamps(b_loc)
    b_loc$year <- lubridate::year(b_loc$timestamp)

    # ---- Re-key move2 object to deployment_id for metric computation ----
    # mt_speed(), mt_distance(), mt_time_lags(), mt_azimuth(), calc_displacement()
    # and add_delta_altitude() all group by the move2 track id.  If the track id
    # is individual_local_identifier (the merged case), an animal with multiple
    # deployments gets one continuous track and metrics bridge across tag changes.
    # Re-keying to deployment_id (when available and non-NA) fixes this: each
    # deployment is its own track for metric purposes.  We restore the original
    # track id column afterwards so the returned object remains consistent.
    original_track_col <- move2::mt_track_id_column(b_loc)

    dep_col <- .metric_group_col(b_loc)   # deployment_id > tag_local_id > individual_local_id
    if (!is.null(dep_col) && dep_col != original_track_col &&
        dep_col %in% names(b_loc) && !all(is.na(b_loc[[dep_col]]))) {
      b_loc[[dep_col]] <- as.character(b_loc[[dep_col]])
      b_loc <- tryCatch(
        move2::mt_as_move2(b_loc,
                           track_id_column = dep_col,
                           time_column     = "timestamp",
                           crs             = sf::st_crs(b_loc)
        ),
        error = function(e) {
          .msg("  Note: could not re-key to ", dep_col, " (", conditionMessage(e),
               "); using ", original_track_col, " for metrics.")
          b_loc
        }
      )
      .msg("  Metrics grouped by: ", dep_col,
           " (", dplyr::n_distinct(b_loc[[dep_col]], na.rm = TRUE), " deployments)")
    }

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
                          speed_units = "km/h",
                          time_units  = "secs")

    .source_local(script_calc_displacement)
    b_loc <- calc_displacement(b_loc)

    # ---- Cumulative distance — grouped by deployment_id ----
    if (isTRUE(compute_cum_dist) && "distance" %in% names(b_loc)) {
      # Use the current (deployment-level) track id for grouping
      track_id_vec   <- as.character(move2::mt_track_id(b_loc))
      dist_numeric   <- as.numeric(b_loc$distance)
      cum_dist       <- ave(
        ifelse(is.na(dist_numeric), 0, dist_numeric),
        track_id_vec,
        FUN = cumsum
      )
      first_fix <- !duplicated(track_id_vec)
      cum_dist[first_fix] <- NA_real_
      b_loc$cum_dist_km <- cum_dist
    }

    # ---- Altitude from pressure ----
    b_loc <- .add_altitude_from_pressure_col(b_loc)

    # ---- Temperature ----
    b_loc <- .add_temperature_to_locations(b_loc)

    # ---- Delta altitude — grouped by deployment_id ----
    b_loc <- .safe_try(.add_delta_altitude(b_loc), "add_delta_altitude") %||% b_loc

    # ---- Restore original track id column ----
    cur_track_col <- move2::mt_track_id_column(b_loc)
    if (cur_track_col != original_track_col &&
        original_track_col %in% names(b_loc) &&
        !all(is.na(b_loc[[original_track_col]]))) {
      b_loc[[original_track_col]] <- as.character(b_loc[[original_track_col]])
      b_loc <- tryCatch(
        move2::mt_as_move2(b_loc,
                           track_id_column = original_track_col,
                           time_column     = "timestamp",
                           crs             = sf::st_crs(b_loc)
        ),
        error = function(e) b_loc   # leave as deployment-keyed if restore fails
      )
    }

    b_loc
  }

  # Produce a daily (solar-noon) location dataset + bat-night sensor summaries.
  .source_local(script_daily)
  .source_local(script_pressure_to_altitude)
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

    grp_col <- .metric_group_col(rep_daily)
    if (is.null(grp_col)) {
      rep_daily$`..metric_group_id` <- as.character(move2::mt_track_id(rep_daily))
      grp_col <- "..metric_group_id"
    }

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
      dplyr::group_by(.data[[grp_col]], .data$period_id) %>%
      dplyr::arrange(.data$night_day == "night", .data$dist_to_noon, .by_group = TRUE) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    .msg("  [noon_roost] Selected ", nrow(rep_daily), " daily points across ",
         dplyr::n_distinct(rep_daily[[grp_col]]), " groups (", grp_col, ").")
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

    # Some Movebank downloads do not expose individual_local_identifier as an
    # event column. In that case, recover it from track metadata or fall back to
    # the current move2 track id so downstream grouping never breaks.
    if (!"individual_local_identifier" %in% names(b)) {
      td <- tryCatch(move2::mt_track_data(b), error = function(e) NULL)
      if (!is.null(td) && "individual_local_identifier" %in% names(td)) {
        track_key <- names(td)[1]
        lookup <- stats::setNames(as.character(td$individual_local_identifier),
                                  as.character(td[[track_key]]))
        b$individual_local_identifier <- unname(lookup[as.character(move2::mt_track_id(b))])
      }
    }
    if (!"individual_local_identifier" %in% names(b) || all(is.na(b$individual_local_identifier))) {
      b$individual_local_identifier <- as.character(move2::mt_track_id(b))
      .msg("  individual_local_identifier missing; filled from move2 track id.")
    }

    b <- .fix_track_data_lists(b)
    b <- .add_event_attrs(b)
    b <- .set_tag_type(b, study_id_current = id)
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

    # Populate temperature_min, temperature_max, avg_temp on location rows for
    # all tag types (TinyFox flat columns + NanoFox sensor-row join fallback).
    b <- .add_temperature_to_locations(b)

    # Convert pressure to altitude for all tag types.
    # NanoFox: min_3h_pressure is joined by add_min_pressure_to_locations() above.
    # TinyFox: tinyfox_pressure_min_last_24h is a flat column on location rows.
    # Both are handled by a single direct hypsometric conversion — no sourced
    # script dependency, no window grouping, no row-ordering issues.
    b <- .add_altitude_from_pressure_col(b)

    # Expand TinyFox flat-schema columns into separate sensor rows (one per
    # measurement type per transmission), adding time_start and time_end.
    # Must run AFTER .add_altitude_from_pressure_col() so altitude_m is already
    # on rows, and BEFORE .add_night_day_id() so new rows get night/day labels too.
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

  # ---------------------------------------------------------------------------
  # Column type normalisation for cross-study binding
  # ---------------------------------------------------------------------------
  # Converts an sf/tibble to a maximally bind-safe form:
  #   - units columns → plain numeric
  #   - ordered/factor columns → character (avoids level-set conflicts)
  #   - int64 (bit64) columns → character (avoids integer overflow in bind)
  # Geometry column is left untouched.
  .normalise_cols <- function(x) {
    geom_col <- if (inherits(x, "sf")) attr(x, "sf_column") else NULL
    for (nm in names(x)) {
      if (!is.null(geom_col) && nm == geom_col) next
      v <- x[[nm]]
      if (inherits(v, "units"))              { x[[nm]] <- as.numeric(v);    next }
      if (is.ordered(v) || is.factor(v))     { x[[nm]] <- as.character(v);  next }
      if (inherits(v, "integer64"))          { x[[nm]] <- as.character(v);  next }
    }
    x
  }

  # Create a typed NA vector that matches `src` in class so bind_rows
  # doesn't see a logical vs POSIXct / numeric / character clash.
  .na_like <- function(src, n) {
    if (inherits(src, "POSIXct"))
      return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
    if (is.double(src))    return(rep(NA_real_,      n))
    if (is.integer(src))   return(rep(NA_integer_,   n))
    if (is.character(src)) return(rep(NA_character_,  n))
    if (is.logical(src))   return(rep(NA,             n))
    rep(NA_character_, n)   # safe fallback
  }

  merge_stack <- function(objs) {
    objs <- Filter(Negate(is.null), objs)
    if (length(objs) == 0) return(NULL)
    if (length(objs) == 1) return(objs[[1]])

    # Step 1: re-key every object to individual_local_identifier.
    # Studies can arrive with different track_id_column values (e.g. x1 uses
    # deployment_id, x2/x3 use individual_local_identifier). After bind_rows
    # the only column guaranteed to be non-NA on every event row across all
    # studies is individual_local_identifier — so we unify on that before merging.
    # deployment_id is preserved as a plain column for within-study grouping.
    .rekey_to_individual <- function(x) {
      # Ensure individual_local_identifier is an event column (not track-data only)
      if (!"individual_local_identifier" %in% names(x)) {
        td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
        if (!is.null(td) && "individual_local_identifier" %in% names(td)) {
          track_key <- as.character(move2::mt_track_id(x))
          id_col    <- names(td)[1]
          lkp <- stats::setNames(as.character(td$individual_local_identifier),
                                 as.character(td[[id_col]]))
          x$individual_local_identifier <- unname(lkp[track_key])
        }
      }
      if (!"individual_local_identifier" %in% names(x) ||
          all(is.na(x$individual_local_identifier))) {
        x$individual_local_identifier <- as.character(move2::mt_track_id(x))
      }
      # Re-cast to move2 keyed on individual_local_identifier so the track data
      # is consistent before we strip the class for binding.
      x$individual_local_identifier <- as.character(x$individual_local_identifier)
      tryCatch(
        move2::mt_as_move2(x,
                           track_id_column = "individual_local_identifier",
                           time_column     = "timestamp",
                           crs             = sf::st_crs(x)
        ),
        error = function(e) x   # if re-keying fails, proceed with original
      )
    }
    objs <- lapply(objs, .rekey_to_individual)

    # Step 2: strip units and normalise problematic column types.
    # Explicitly drop the move2 subclass after st_as_sf() — bind_rows triggers
    # dplyr_reconstruct.move2() which calls mt_track_id() and asserts no NAs.
    # A plain sf tibble binds cleanly; we re-cast to move2 after combining.
    .to_plain_sf <- function(x) {
      x <- sf::st_as_sf(x)
      class(x) <- class(x)[!class(x) %in% c("move2")]
      x
    }
    sf_objs <- lapply(objs, function(x) .normalise_cols(.to_plain_sf(x)))

    # Step 3: build the union column set (excluding geometry)
    geom_cols <- vapply(sf_objs, function(x) attr(x, "sf_column"), character(1))
    all_cols  <- unique(unlist(mapply(
      function(x, gc) setdiff(names(x), gc),
      sf_objs, geom_cols, SIMPLIFY = FALSE
    )))

    # Step 4: align each study to the full column set with typed NAs
    # Also build a global type map: first non-null type wins per column.
    type_map <- list()
    for (x in sf_objs) {
      for (nm in names(x)) {
        if (!nm %in% names(type_map) && !inherits(x[[nm]], "sfc"))
          type_map[[nm]] <- x[[nm]]
      }
    }

    align_one <- function(x, gc) {
      n <- nrow(x)
      for (col in setdiff(all_cols, names(x))) {
        src <- type_map[[col]]
        x[[col]] <- if (!is.null(src)) .na_like(src, n) else rep(NA_character_, n)
      }
      x[, c(all_cols, gc), drop = FALSE]
    }

    sf_objs <- mapply(align_one, sf_objs, geom_cols, SIMPLIFY = FALSE)

    # Step 5: bind — all column types now match
    combined <- tryCatch(
      do.call(dplyr::bind_rows, sf_objs),
      error = function(e) {
        # Last-resort: coerce every non-geometry column to character
        .msg("  merge_stack: bind_rows failed (", conditionMessage(e),
             "); falling back to all-character coercion.")
        sf_objs2 <- lapply(sf_objs, function(x) {
          gc <- attr(x, "sf_column")
          for (nm in setdiff(names(x), gc))
            x[[nm]] <- if (!inherits(x[[nm]], "sfc")) as.character(x[[nm]]) else x[[nm]]
          x
        })
        do.call(dplyr::bind_rows, sf_objs2)
      }
    )

    # Step 6: choose a stable track id, arrange, re-cast to move2.
    # Always use individual_local_identifier because every object was re-keyed
    # to it in Step 1 — it is the only column guaranteed non-NA across all studies.
    track_col <- if ("individual_local_identifier" %in% names(combined) &&
                     !all(is.na(combined$individual_local_identifier))) {
      "individual_local_identifier"
    } else {
      combined$..metric_group_id <- as.character(seq_len(nrow(combined)))
      "..metric_group_id"
    }

    if (!"timestamp" %in% names(combined))
      stop("Cannot merge studies: timestamp column missing after harmonising schemas.")

    combined[[track_col]] <- as.character(combined[[track_col]])
    combined <- combined[order(combined[[track_col]], combined$timestamp), ]

    out <- move2::mt_as_move2(
      combined,
      track_id_column = track_col,
      time_column     = "timestamp",
      crs             = sf::st_crs(sf_objs[[1]])
    )

    # Resolve any remaining list-columns in track data
    out <- tryCatch(.fix_track_data_lists(out), error = function(e) out)
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
    # Note: units are stripped from merged objects by .strip_units_cols() before
    # mt_stack(), so deparse_unit() would error here — report class instead.
    if (!is.null(loc_merged)) {
      if ("speed" %in% names(loc_merged))
        .msg("Speed column class (location): ", class(loc_merged$speed)[1])
      if ("delta_altitude_m" %in% names(loc_merged))
        .msg("Delta altitude column class (location): ", class(loc_merged$delta_altitude_m)[1])
      if ("cum_dist_km" %in% names(loc_merged))
        .msg("Cumulative distance column present (location): yes")
    }
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
