# =============================================================================
# prepare_movebank_reupload_exports_clean.R
#
# Compare local Movebank versions to the current Movebank upload and export
# project-specific CSVs for re-upload.
#
# Outputs per project:
#   reupload_data.csv      = Sigfox geolocation / location data plus TinyFox/same-time sensor data
#   reupload_temp.csv      = NanoFox interval accessory measurements only
#   reupload_pressure.csv  = NanoFox interval barometric pressure only
#   reupload_vedba.csv     = NanoFox interval acceleration / VeDBA only
#
# Optional supplemental data sources can be used to fill missing sensor values.
# Missing values are filled automatically; conflicting non-missing values are
# exported to diagnostics for review.
#
# Important behavior:
#   - Comparisons are restricted to the current Movebank download.
#   - Tags/animals/deployments absent from the downloaded project are excluded.
#   - Diagnostics are written inside each Movebank project folder.
#
# Updated: 2026-06-24
# =============================================================================

suppressPackageStartupMessages({
  library(move2)
  library(sf)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(readr)
  library(lubridate)
  library(stringr)
  library(fs)
  library(tibble)
})

# Consolidated Functions
# source("./src/consolidated_functions.R")

# =============================================================================
# Helpers
# =============================================================================

first_existing <- function(nms, candidates) {
  out <- candidates[candidates %in% nms]
  if (length(out) == 0) NA_character_ else out[[1]]
}

drop_sfc_cols <- function(x) {
  tibble::as_tibble(x) |>
    dplyr::select(-where(~ inherits(.x, "sfc") || inherits(.x, "sfg")))
}

safe_file_name <- function(x) {
  x |>
    stringr::str_replace_all("[^A-Za-z0-9]+", "_") |>
    stringr::str_replace_all("_+", "_") |>
    stringr::str_replace_all("^_|_$", "")
}

vec_chr <- function(df, col, default = NA_character_) {
  if (!is.na(col) && col %in% names(df)) {
    as.character(df[[col]])
  } else {
    rep(default, nrow(df))
  }
}

vec_time <- function(df, col) {
  if (!is.na(col) && col %in% names(df)) {
    as.POSIXct(df[[col]], tz = "UTC")
  } else {
    as.POSIXct(rep(NA_real_, nrow(df)), origin = "1970-01-01", tz = "UTC")
  }
}

coalesce_chr_cols <- function(df, cols, default = NA_character_) {
  cols <- cols[cols %in% names(df)]

  if (length(cols) == 0) {
    return(rep(default, nrow(df)))
  }

  out <- rep(NA_character_, nrow(df))

  for (cc in cols) {
    val <- as.character(df[[cc]])
    val[val == ""] <- NA_character_
    out <- dplyr::coalesce(out, val)
  }

  dplyr::coalesce(out, rep(default, nrow(df)))
}

get_move_col <- function(x, kind = c("track", "time")) {
  kind <- match.arg(kind)

  val <- tryCatch(
    {
      if (kind == "track") {
        move2::mt_track_id_column(x)
      } else {
        move2::mt_time_column(x)
      }
    },
    error = function(e) NULL
  )

  val <- as.character(val)

  if (length(val) > 0 && val[[1]] %in% names(x)) {
    return(val[[1]])
  }

  candidates <- if (kind == "track") {
    c("deployment_id", "individual_local_identifier", "track_id")
  } else {
    c("timestamp", "event_time", "time")
  }

  first_existing(names(x), candidates)
}

fmt_time_key <- function(x) {
  format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%d %H:%M:%S", tz = "UTC")
}

fmt_time_upload <- function(x) {
  paste0(
    format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    ".000"
  )
}

as_utc_posixct <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(as.POSIXct(x, tz = "UTC"))
  }

  suppressWarnings(as.POSIXct(x, tz = "UTC"))
}

load_move_rdata <- function(path, label = fs::path_ext_remove(fs::path_file(path)), object = NULL) {
  e <- new.env(parent = emptyenv())
  load(path, envir = e)

  if (!is.null(object)) {
    obj <- get(object, envir = e)
  } else {
    obj_names <- ls(e)

    preferred <- intersect(c("bats_full", "bat_full", "bats_loc"), obj_names)

    move_names <- obj_names[
      vapply(obj_names, function(nm) inherits(get(nm, envir = e), "move2"), logical(1))
    ]

    sf_names <- obj_names[
      vapply(obj_names, function(nm) inherits(get(nm, envir = e), "sf"), logical(1))
    ]

    possible <- unique(c(preferred, move_names, sf_names))

    if (length(possible) == 0) {
      stop("No move2/sf object found in: ", path)
    }

    obj <- get(possible[[1]], envir = e)
  }

  list(
    label = label,
    path = path,
    object = obj
  )
}

# =============================================================================
# Type normalization
# =============================================================================

normalize_event_bind_types <- function(df) {
  df <- df |>
    dplyr::mutate(
      dplyr::across(where(is.factor), as.character),
      dplyr::across(where(is.ordered), as.character),
      dplyr::across(where(~ inherits(.x, "integer64")), as.character)
    )

  time_cols <- intersect(
    c(
      "timestamp",
      "transmission_timestamp",
      "start_timestamp",
      "end_timestamp",
      ".timestamp_utc",
      ".deploy_on_timestamp"
    ),
    names(df)
  )

  for (cc in time_cols) {
    df[[cc]] <- as_utc_posixct(df[[cc]])
  }

  char_cols <- names(df)[
    stringr::str_detect(
      names(df),
      stringr::regex(
        paste(
          c(
            "^id$",
            "_id$",
            "_ids$",
            "^id_",
            "_id_",
            "event_id",
            "sensor_type_id",
            "sensor_type_ids",
            "deployment_id",
            "individual_id",
            "individual_local_identifier",
            "tag_id",
            "tag_local_identifier",
            "track_id",
            "base.?station",
            "sensor_type$",
            "model$",
            "tag_type$",
            "tag_firmware$",
            "firmware",
            "model_firmware_issue",
            "sex$",
            "species$",
            "country$",
            "operator$",
            "link_quality$",
            "lqi$",
            "year$",
            "yday$",
            "season$"
          ),
          collapse = "|"
        ),
        ignore_case = TRUE
      )
    )
  ]

  df <- df |>
    dplyr::mutate(
      dplyr::across(dplyr::any_of(char_cols), as.character)
    )

  if (".source" %in% names(df)) {
    df$.source <- as.character(df$.source)
  }

  if (".source_rank" %in% names(df)) {
    df$.source_rank <- as.numeric(df$.source_rank)
  }

  if (".deployment_id" %in% names(df)) {
    df$.deployment_id <- as.character(df$.deployment_id)
  }

  if (".project_name" %in% names(df)) {
    df$.project_name <- as.character(df$.project_name)
  }

  if (".tag_id_upload" %in% names(df)) {
    df$.tag_id_upload <- as.character(df$.tag_id_upload)
  }

  if (".animal_id_upload" %in% names(df)) {
    df$.animal_id_upload <- as.character(df$.animal_id_upload)
  }

  if (".deploy_year" %in% names(df)) {
    df$.deploy_year <- as.numeric(df$.deploy_year)
  }

  if (".event_key" %in% names(df)) {
    df$.event_key <- as.character(df$.event_key)
  }

  if (".timestamp_utc" %in% names(df)) {
    df$.timestamp_utc <- as_utc_posixct(df$.timestamp_utc)
  }

  df
}

simple_col_type <- function(x) {
  if (inherits(x, "POSIXct")) return("POSIXct")
  if (inherits(x, "POSIXt")) return("POSIXt")
  if (inherits(x, "Date")) return("Date")
  if (inherits(x, "integer64")) return("integer64")
  if (is.factor(x)) return("factor")
  typeof(x)
}

bind_rows_safely <- function(dfs) {
  dfs <- purrr::map(dfs, normalize_event_bind_types)

  all_cols <- unique(unlist(purrr::map(dfs, names)))

  conflict_cols <- purrr::keep(all_cols, function(cc) {
    types <- purrr::map_chr(dfs, function(df) {
      if (cc %in% names(df)) simple_col_type(df[[cc]]) else NA_character_
    })

    types <- unique(stats::na.omit(types))
    length(types) > 1
  })

  keep_typed <- c(
    ".source_rank",
    ".deploy_year",
    ".timestamp_utc",
    ".deploy_on_timestamp",
    "timestamp",
    "transmission_timestamp",
    "start_timestamp",
    "end_timestamp"
  )

  conflict_cols <- setdiff(conflict_cols, keep_typed)

  if (length(conflict_cols) > 0) {
    dfs <- purrr::map(dfs, function(df) {
      df |>
        dplyr::mutate(
          dplyr::across(dplyr::any_of(conflict_cols), as.character)
        )
    })
  }

  dplyr::bind_rows(dfs)
}

# =============================================================================
# Extract standardized track table
# =============================================================================

extract_track_table <- function(m) {
  track_col <- get_move_col(m, "track")

  td <- move2::mt_track_data(m) |>
    drop_sfc_cols()

  if (!track_col %in% names(td)) {
    stop("Track column not found in mt_track_data(): ", track_col)
  }

  project_col <- first_existing(
    names(td),
    c("name", "names", "study_name", "project_name", "Movebank project")
  )

  deploy_on_col <- first_existing(
    names(td),
    c("deploy_on_timestamp", "deployment_start", "deployment_start_timestamp")
  )

  tag_type_col <- first_existing(names(td), c("tag_type", "model", "tag_model"))
  model_col    <- first_existing(names(td), c("model", "tag_model", "tag_type"))
  firm_col     <- first_existing(names(td), c("tag_firmware", "firmware_version", "firmware version"))
  issue_col    <- first_existing(names(td), c("model_firmware_issue"))

  deploy_on <- vec_time(td, deploy_on_col)

  tibble(
    .deployment_id = as.character(td[[track_col]]),
    .project_name  = vec_chr(td, project_col),
    .tag_id_upload = coalesce_chr_cols(
      td,
      c("tag_local_identifier", "tag ID", "tag_id", "tag_id_local", "deployment_local_identifier"),
      default = NA_character_
    ),
    .animal_id_upload = coalesce_chr_cols(
      td,
      c("individual_local_identifier", "Animal.ID", "animal_id", "individual_id"),
      default = NA_character_
    ),
    .deploy_on_timestamp = deploy_on,
    .deploy_year = lubridate::year(deploy_on),
    .tag_type_track = vec_chr(td, tag_type_col),
    .model_track = vec_chr(td, model_col),
    .tag_firmware_track = vec_chr(td, firm_col),
    .model_firmware_issue = if (!is.na(issue_col) && issue_col %in% names(td)) {
      as.character(td[[issue_col]])
    } else {
      NA_character_
    }
  ) |>
    distinct(.deployment_id, .keep_all = TRUE)
}

# =============================================================================
# Extract standardized event table
# =============================================================================

extract_event_table <- function(m, source_label, source_rank = 1) {
  track_col <- get_move_col(m, "track")
  time_col  <- get_move_col(m, "time")

  if (is.na(track_col) || is.na(time_col)) {
    stop("Could not identify track/time columns.")
  }

  ev0 <- sf::st_drop_geometry(m) |>
    tibble::as_tibble()

  coords <- tryCatch(sf::st_coordinates(m), error = function(e) NULL)

  if (!is.null(coords) && nrow(coords) == nrow(ev0)) {
    ev0$.lon_from_geom <- coords[, "X"]
    ev0$.lat_from_geom <- coords[, "Y"]
  } else {
    ev0$.lon_from_geom <- NA_real_
    ev0$.lat_from_geom <- NA_real_
  }

  td <- extract_track_table(m)

  event_project <- coalesce_chr_cols(
    ev0,
    c("name", "names", "study_name", "project_name", "Movebank project"),
    default = NA_character_
  )

  event_tag <- coalesce_chr_cols(
    ev0,
    c("tag_local_identifier", "tag ID", "tag_id", "tag_id_local"),
    default = NA_character_
  )

  event_animal <- coalesce_chr_cols(
    ev0,
    c("individual_local_identifier", "Animal.ID", "animal_id", "individual_id"),
    default = NA_character_
  )

  out <- ev0 |>
    mutate(
      .source = as.character(source_label),
      .source_rank = as.numeric(source_rank),
      .deployment_id = as.character(.data[[track_col]]),
      .timestamp_utc = as.POSIXct(.data[[time_col]], tz = "UTC")
    )

  out[[time_col]] <- out$.timestamp_utc

  out <- out |>
    left_join(td, by = ".deployment_id") |>
    mutate(
      .project_name = dplyr::coalesce(.project_name, event_project),
      .tag_id_upload = dplyr::coalesce(.tag_id_upload, event_tag, .deployment_id),
      .animal_id_upload = dplyr::coalesce(.animal_id_upload, event_animal),
      .deploy_year = dplyr::coalesce(.deploy_year, lubridate::year(.timestamp_utc)),
      .event_key = paste(.tag_id_upload, fmt_time_key(.timestamp_utc), sep = "|")
    ) |>
    normalize_event_bind_types()

  out
}

# =============================================================================
# Choose best local source per project-year
# =============================================================================

choose_best_project_year_events <- function(candidate_events) {
  candidate_events <- normalize_event_bind_types(candidate_events)

  source_summary <- candidate_events |>
    filter(!is.na(.project_name), !is.na(.deploy_year)) |>
    distinct(.source, .source_rank, .project_name, .deploy_year, .deployment_id, .event_key) |>
    group_by(.source, .source_rank, .project_name, .deploy_year) |>
    summarise(
      n_deployments = n_distinct(.deployment_id),
      n_events = n_distinct(.event_key),
      .groups = "drop"
    )

  best_source <- source_summary |>
    arrange(.project_name, .deploy_year, desc(n_deployments), desc(n_events), desc(.source_rank)) |>
    group_by(.project_name, .deploy_year) |>
    slice(1) |>
    ungroup()

  best_events <- candidate_events |>
    semi_join(
      best_source |> select(.source, .project_name, .deploy_year),
      by = c(".source", ".project_name", ".deploy_year")
    ) |>
    arrange(.project_name, .deploy_year, .tag_id_upload, .timestamp_utc, desc(.source_rank)) |>
    distinct(.project_name, .deploy_year, .event_key, .keep_all = TRUE)

  list(
    best_source = best_source,
    best_events = best_events,
    source_summary = source_summary
  )
}

# =============================================================================
# Column handling for Movebank upload CSVs
# =============================================================================

rename_for_upload <- function(df) {
  rename_map <- c(
    sequence_number = "sequence number",

    sigfox_computed_location_radius = "Sigfox computed location radius",
    sigfox_computed_location_source = "Sigfox computed location source",
    sigfox_computed_location_status = "Sigfox computed location status",
    sigfox_lqi = "Sigfox LQI",
    sigfox_link_quality = "Sigfox link quality",
    sigfox_country = "Sigfox country",
    sigfox_operator = "Sigfox operator",
    sigfox_rssi = "Sigfox RSSI",
    sigfox_payload = "Sigfox payload",
    sigfox_duplicates = "Sigfox duplicates",

    tinyfox_temperature_min_last_24h = "x24h_min_temperature_c",
    tinyfox_temperature_max_last_24h = "x24h_max_temperature_c",
    tinyfox_activity_percent_last_24h = "x24h_active_percent",
    tinyfox_pressure_min_last_24h = "x24h_min_pressure_mbar"
  )

  for (old in names(rename_map)) {
    new <- rename_map[[old]]

    if (old %in% names(df) && !new %in% names(df)) {
      names(df)[names(df) == old] <- new
    }
  }

  names(df) <- make.unique(names(df), sep = "_")
  df
}

drop_all_empty_cols <- function(df, always_keep = character()) {
  keep <- names(df) %in% always_keep |
    vapply(df, function(x) {
      any(!is.na(x) & as.character(x) != "")
    }, logical(1))

  df[, keep, drop = FALSE]
}

common_upload_cols <- function(events, include_interval = FALSE) {
  if (include_interval) {
    start_col <- first_existing(names(events), c("start_timestamp", "start timestamp"))
    end_col   <- first_existing(names(events), c("end_timestamp", "end timestamp"))

    start_time <- if (!is.na(start_col)) {
      as_utc_posixct(events[[start_col]])
    } else {
      events$.timestamp_utc
    }

    end_time <- if (!is.na(end_col)) {
      as_utc_posixct(events[[end_col]])
    } else {
      events$.timestamp_utc
    }

    start_time <- dplyr::coalesce(start_time, events$.timestamp_utc)
    end_time   <- dplyr::coalesce(end_time, events$.timestamp_utc)

    out <- tibble(
      `tag ID` = events$.tag_id_upload,
      Animal.ID = events$.animal_id_upload,
      timestamp = fmt_time_upload(events$.timestamp_utc),
      `start timestamp` = fmt_time_upload(start_time),
      `end timestamp` = fmt_time_upload(end_time)
    )
  } else {
    out <- tibble(
      `tag ID` = events$.tag_id_upload,
      Animal.ID = events$.animal_id_upload,
      timestamp = fmt_time_upload(events$.timestamp_utc)
    )
  }

  seq_col <- first_existing(names(events), c("sequence_number", "sequence number"))

  if (!is.na(seq_col)) {
    out$`sequence number` <- events[[seq_col]]
  }

  out
}

regex_cols <- function(df, pattern) {
  names(df)[stringr::str_detect(names(df), stringr::regex(pattern, ignore_case = TRUE))]
}

remove_internal_cols <- function(cols) {
  cols[!stringr::str_detect(cols, "^\\.")]
}

row_has_any_value <- function(df) {
  if (ncol(df) == 0) {
    return(rep(FALSE, nrow(df)))
  }

  apply(
    as.data.frame(df),
    1,
    function(z) any(!is.na(z) & as.character(z) != "")
  )
}


# Classify rows that should behave like TinyFox uploads.
# Rule from data cleanup: any row before 2025 is TinyFox, even if model metadata is incomplete.
is_tinyfox_event <- function(events) {
  n <- nrow(events)

  if (n == 0) {
    return(logical(0))
  }

  text_cols <- intersect(
    c(
      "tag_type",
      "model",
      "tag_model",
      "sigfox_device_type",
      "tag_firmware",
      ".tag_type_track",
      ".model_track",
      ".tag_firmware_track"
    ),
    names(events)
  )

  tag_text <- rep("", n)

  for (cc in text_cols) {
    val <- as.character(events[[cc]])
    val[is.na(val)] <- ""
    tag_text <- paste(tag_text, val)
  }

  event_year <- suppressWarnings(lubridate::year(events$.timestamp_utc))

  deploy_year <- if (".deploy_year" %in% names(events)) {
    suppressWarnings(as.numeric(events$.deploy_year))
  } else {
    rep(NA_real_, n)
  }

  stringr::str_detect(tag_text, stringr::regex("tinyfox|tiny fox|tinyfoxbatt", ignore_case = TRUE)) |
    (!is.na(event_year) & event_year < 2025) |
    (!is.na(deploy_year) & deploy_year < 2025)
}

get_interval_times <- function(events) {
  start_col <- first_existing(names(events), c("start_timestamp", "start timestamp"))
  end_col   <- first_existing(names(events), c("end_timestamp", "end timestamp"))

  start_time <- if (!is.na(start_col)) {
    as_utc_posixct(events[[start_col]])
  } else {
    as.POSIXct(rep(NA_real_, nrow(events)), origin = "1970-01-01", tz = "UTC")
  }

  end_time <- if (!is.na(end_col)) {
    as_utc_posixct(events[[end_col]])
  } else {
    as.POSIXct(rep(NA_real_, nrow(events)), origin = "1970-01-01", tz = "UTC")
  }

  list(start_time = start_time, end_time = end_time)
}

sensor_interval_differs_from_transmission <- function(events, tolerance_seconds = 1) {
  n <- nrow(events)

  if (n == 0) {
    return(logical(0))
  }

  intervals <- get_interval_times(events)
  tx_time <- as_utc_posixct(events$.timestamp_utc)

  start_diff <- !is.na(intervals$start_time) & !is.na(tx_time) &
    abs(as.numeric(difftime(intervals$start_time, tx_time, units = "secs"))) > tolerance_seconds

  end_diff <- !is.na(intervals$end_time) & !is.na(tx_time) &
    abs(as.numeric(difftime(intervals$end_time, tx_time, units = "secs"))) > tolerance_seconds

  start_diff | end_diff
}

get_sensor_value_cols <- function(events, value_pattern = "temperature|temp|activity|pressure|baro|vedba|odba|accel") {
  value_cols <- regex_cols(events, value_pattern)

  setdiff(
    value_cols,
    c(
      grep("^\\.", names(events), value = TRUE),
      "deployment_id",
      "event_id",
      "visible",
      "timestamp",
      "transmission_timestamp",
      "start_timestamp",
      "end_timestamp",
      "sensor_type_id",
      "sensor_type_ids",
      "sensor_type",
      "location_lat",
      "location_long",
      "location lat",
      "location long",
      "lat",
      "lon",
      "altitude_m",
      "sigfox_computed_location_radius",
      "sigfox_computed_location_source",
      "sigfox_computed_location_status",
      "sigfox_lqi",
      "sigfox_link_quality",
      "sigfox_country",
      "sigfox_operator",
      "sigfox_rssi",
      "sigfox_payload",
      "sigfox_duplicates"
    )
  )
}

mask_rows_to_na <- function(df, keep_rows) {
  if (nrow(df) == 0 || ncol(df) == 0) {
    return(df)
  }

  keep_rows[is.na(keep_rows)] <- FALSE

  out <- df
  out[!keep_rows, ] <- NA
  out
}

build_location_csv <- function(events) {
  lat_col <- first_existing(names(events), c("location_lat", "location lat", "lat", "latitude"))
  lon_col <- first_existing(names(events), c("location_long", "location long", "lon", "long", "longitude"))

  lat <- if (!is.na(lat_col)) events[[lat_col]] else events$.lat_from_geom
  lon <- if (!is.na(lon_col)) events[[lon_col]] else events$.lon_from_geom

  location_metric_cols <- regex_cols(
    events,
    paste(
      c(
        "sigfox",
        "base.?station",
        "basestation",
        "base_stations",
        "n_base_stations",
        "location",
        "radius",
        "lqi",
        "rssi",
        "link",
        "payload",
        "duplicate",
        "operator",
        "country",
        "altitude"
      ),
      collapse = "|"
    )
  )

  location_metric_cols <- location_metric_cols |>
    remove_internal_cols() |>
    setdiff(c(
      "location_lat",
      "location_long",
      "location lat",
      "location long",
      "lat",
      "lon",
      "timestamp",
      "transmission_timestamp",
      "start_timestamp",
      "end_timestamp",
      "sensor_type",
      "sensor_type_id",
      "sensor_type_ids"
    ))

  # Ensure the key Sigfox fields are included when present, especially RSSI.
  priority_location_cols <- intersect(
    c(
      "sigfox_rssi",
      "sigfox_lqi",
      "sigfox_link_quality",
      "sigfox_computed_location_radius",
      "sigfox_computed_location_source",
      "sigfox_computed_location_status",
      "sigfox_country",
      "sigfox_operator",
      "sigfox_payload",
      "sigfox_duplicates"
    ),
    names(events)
  )

  location_metric_cols <- unique(c(priority_location_cols, location_metric_cols))

  # Sensor columns belong in data.csv for TinyFox rows. Rows before 2025 are
  # treated as TinyFox even when model metadata are incomplete. For non-TinyFox
  # rows, sensor values stay in data.csv only when their interval is the same as
  # the transmission timestamp. Non-TinyFox interval sensors are exported to the
  # separate sensor CSVs instead.
  tinyfox_rows <- is_tinyfox_event(events)
  interval_rows <- sensor_interval_differs_from_transmission(events)
  sensor_rows_for_data <- tinyfox_rows | !interval_rows

  sensor_cols_for_data <- get_sensor_value_cols(events)
  sensor_values_for_data <- events[, sensor_cols_for_data, drop = FALSE] |>
    mask_rows_to_na(sensor_rows_for_data)

  out <- bind_cols(
    common_upload_cols(events),
    tibble(
      `location lat` = lat,
      `location long` = lon
    ),
    events[, location_metric_cols, drop = FALSE],
    sensor_values_for_data
  ) |>
    mutate(`sensor type` = "sigfox-geolocation") |>
    filter(!is.na(`location lat`), !is.na(`location long`)) |>
    rename_for_upload()

  drop_all_empty_cols(
    out,
    always_keep = c(
      "tag ID",
      "Animal.ID",
      "timestamp",
      "sequence number",
      "location lat",
      "location long",
      "sensor type"
    )
  )
}

build_sensor_csv <- function(events, sensor_type, value_pattern) {
  # TinyFox 24h measurements belong in reupload_data.csv, not in separate sensor
  # files. Rows before 2025 are treated as TinyFox. Separate sensor files are
  # only for non-TinyFox/NanoFox-style records where the sensor measurement
  # interval differs from the transmission timestamp.
  keep_sensor_file_rows <- !is_tinyfox_event(events) &
    sensor_interval_differs_from_transmission(events)

  if (!any(keep_sensor_file_rows, na.rm = TRUE)) {
    return(tibble())
  }

  events <- events[keep_sensor_file_rows, , drop = FALSE]

  value_cols <- get_sensor_value_cols(events, value_pattern)

  if (length(value_cols) == 0) {
    return(tibble())
  }

  values <- events[, value_cols, drop = FALSE]
  has_value <- row_has_any_value(values)

  out <- bind_cols(
    common_upload_cols(events, include_interval = TRUE),
    values
  ) |>
    mutate(`sensor type` = sensor_type) |>
    filter(has_value) |>
    rename_for_upload()

  drop_all_empty_cols(
    out,
    always_keep = c(
      "tag ID",
      "Animal.ID",
      "timestamp",
      "start timestamp",
      "end timestamp",
      "sequence number",
      "sensor type"
    )
  )
}

build_temp_csv <- function(events) {
  build_sensor_csv(
    events,
    sensor_type = "accessory-measurements",
    value_pattern = "temperature|temp|activity"
  )
}

build_pressure_csv <- function(events) {
  build_sensor_csv(
    events,
    sensor_type = "barometer",
    value_pattern = "pressure|baro"
  )
}

build_vedba_csv <- function(events) {
  build_sensor_csv(
    events,
    sensor_type = "acceleration",
    value_pattern = "vedba|odba|accel"
  )
}


# =============================================================================
# Supplemental sensor data repair
# =============================================================================

canonical_sensor_map <- function() {
  list(
    tinyfox_total_vedba = c(
      "tinyfox_total_vedba",
      "total_ve_dba"
    ),
    tinyfox_temperature_min_last_24h = c(
      "tinyfox_temperature_min_last_24h",
      "x24h_min_temperature_c",
      "x24h_min_temperature_c_2"
    ),
    tinyfox_temperature_max_last_24h = c(
      "tinyfox_temperature_max_last_24h",
      "x24h_max_temperature_c",
      "x24h_max_temperature_c_2"
    ),
    tinyfox_activity_percent_last_24h = c(
      "tinyfox_activity_percent_last_24h",
      "x24h_active_percent"
    ),
    tinyfox_pressure_min_last_24h = c(
      "tinyfox_pressure_min_last_24h",
      "x24h_min_pressure_mbar"
    )
  )
}

not_missing_value <- function(x) {
  !is.na(x) & as.character(x) != ""
}

first_non_missing <- function(x) {
  ok <- not_missing_value(x)

  if (!any(ok)) {
    return(NA)
  }

  x[which(ok)[1]]
}

coalesce_numeric_candidates <- function(df, candidates) {
  candidates <- candidates[candidates %in% names(df)]

  if (length(candidates) == 0) {
    return(rep(NA_real_, nrow(df)))
  }

  out <- rep(NA_real_, nrow(df))

  for (cc in candidates) {
    val <- suppressWarnings(as.numeric(df[[cc]]))
    out <- dplyr::coalesce(out, val)
  }

  out
}

standardize_supplemental_sensor_columns <- function(df) {
  if (nrow(df) == 0) {
    return(df)
  }

  sensor_map <- canonical_sensor_map()

  for (target in names(sensor_map)) {
    mapped_value <- coalesce_numeric_candidates(df, sensor_map[[target]])

    if (!target %in% names(df)) {
      df[[target]] <- mapped_value
    } else {
      current_value <- suppressWarnings(as.numeric(df[[target]]))
      df[[target]] <- dplyr::coalesce(current_value, mapped_value)
    }
  }

  df
}

values_conflict <- function(a, b, tolerance = 1e-8) {
  out <- rep(FALSE, length(a))

  both_present <- not_missing_value(a) & not_missing_value(b)

  if (!any(both_present)) {
    return(out)
  }

  a_num <- suppressWarnings(as.numeric(a))
  b_num <- suppressWarnings(as.numeric(b))
  both_numeric <- both_present & !is.na(a_num) & !is.na(b_num)
  both_non_numeric <- both_present & !both_numeric

  out[both_numeric] <- abs(a_num[both_numeric] - b_num[both_numeric]) > tolerance
  out[both_non_numeric] <- as.character(a[both_non_numeric]) != as.character(b[both_non_numeric])

  out
}

add_missing_columns <- function(df, cols, value = NA_real_) {
  for (cc in cols) {
    if (!cc %in% names(df)) {
      df[[cc]] <- value
    }
  }

  df
}

sensor_repair_columns <- function(...) {
  dfs <- list(...)

  cols <- unique(c(
    names(canonical_sensor_map()),
    unlist(purrr::map(dfs, function(df) {
      if (is.null(df) || nrow(df) == 0) {
        character()
      } else {
        get_sensor_value_cols(
          df,
          value_pattern = "temperature|temp|activity|pressure|baro|vedba|odba|accel"
        )
      }
    }))
  ))

  cols[!is.na(cols)]
}

empty_sensor_fill_log <- function() {
  tibble::tibble(
    .project_name = character(),
    .source = character(),
    .tag_id_upload = character(),
    .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
    .event_key = character(),
    column = character(),
    old_value = character(),
    new_value = character(),
    supplemental_sources = character()
  )
}

empty_sensor_conflict_log <- function() {
  tibble::tibble(
    .project_name = character(),
    .source = character(),
    .tag_id_upload = character(),
    .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
    .event_key = character(),
    column = character(),
    current_value = character(),
    proposed_value = character(),
    supplemental_sources = character()
  )
}

make_supplemental_sensor_table <- function(supplemental_events) {
  if (is.null(supplemental_events) || nrow(supplemental_events) == 0) {
    return(list(
      table = tibble::tibble(.event_key = character(), .supplemental_sources = character()),
      within_supplement_conflicts = tibble::tibble(
        .event_key = character(),
        column = character(),
        n_values = integer(),
        values = character(),
        sources = character()
      )
    ))
  }

  supplemental_events <- supplemental_events |>
    standardize_supplemental_sensor_columns() |>
    normalize_event_bind_types()

  sensor_cols <- sensor_repair_columns(supplemental_events)
  sensor_cols <- intersect(sensor_cols, names(supplemental_events))

  if (length(sensor_cols) == 0) {
    return(list(
      table = tibble::tibble(.event_key = character(), .supplemental_sources = character()),
      within_supplement_conflicts = tibble::tibble(
        .event_key = character(),
        column = character(),
        n_values = integer(),
        values = character(),
        sources = character()
      )
    ))
  }

  long_values <- supplemental_events |>
    select(.event_key, .source, any_of(sensor_cols)) |>
    tidyr::pivot_longer(
      cols = any_of(sensor_cols),
      names_to = "column",
      values_to = "value",
      values_transform = list(value = as.character)
    ) |>
    filter(not_missing_value(value))

  within_supplement_conflicts <- long_values |>
    group_by(.event_key, column) |>
    summarise(
      n_values = n_distinct(value),
      values = paste(sort(unique(value)), collapse = "; "),
      sources = paste(sort(unique(.source)), collapse = "; "),
      .groups = "drop"
    ) |>
    filter(n_values > 1)

  table <- supplemental_events |>
    select(.event_key, .source, any_of(sensor_cols)) |>
    group_by(.event_key) |>
    summarise(
      .supplemental_sources = paste(sort(unique(.source)), collapse = "; "),
      across(any_of(sensor_cols), first_non_missing),
      .groups = "drop"
    )

  list(
    table = table,
    within_supplement_conflicts = within_supplement_conflicts
  )
}

load_supplemental_events <- function(supplemental_paths) {
  if (is.null(supplemental_paths) || length(supplemental_paths) == 0) {
    return(tibble::tibble())
  }

  supplemental_objs <- purrr::imap(supplemental_paths, function(path, label) {
    load_move_rdata(path = path, label = label)
  })

  supplemental_event_list <- purrr::map2(
    supplemental_objs,
    seq_along(supplemental_objs),
    function(x, i) {
      extract_event_table(
        m = x$object,
        source_label = paste0("SUPPLEMENTAL_", x$label),
        source_rank = 10000 + i
      ) |>
        standardize_supplemental_sensor_columns() |>
        normalize_event_bind_types()
    }
  )

  bind_rows_safely(supplemental_event_list) |>
    standardize_supplemental_sensor_columns() |>
    normalize_event_bind_types()
}

fill_missing_sensor_values_from_supplemental <- function(primary_events,
                                                         supplemental_events,
                                                         tolerance = 1e-8) {
  primary_events <- primary_events |>
    standardize_supplemental_sensor_columns() |>
    normalize_event_bind_types()

  if (is.null(supplemental_events) || nrow(supplemental_events) == 0) {
    return(list(
      events = primary_events,
      fill_log = empty_sensor_fill_log(),
      conflict_log = empty_sensor_conflict_log(),
      within_supplement_conflicts = tibble::tibble(
        .event_key = character(),
        column = character(),
        n_values = integer(),
        values = character(),
        sources = character()
      )
    ))
  }

  supplemental_info <- make_supplemental_sensor_table(supplemental_events)
  supplemental_table <- supplemental_info$table

  if (nrow(supplemental_table) == 0) {
    return(list(
      events = primary_events,
      fill_log = empty_sensor_fill_log(),
      conflict_log = empty_sensor_conflict_log(),
      within_supplement_conflicts = supplemental_info$within_supplement_conflicts
    ))
  }

  sensor_cols <- sensor_repair_columns(primary_events, supplemental_table)
  sensor_cols <- intersect(sensor_cols, names(supplemental_table))
  sensor_cols <- setdiff(sensor_cols, c(".event_key", ".supplemental_sources"))

  primary_events <- add_missing_columns(primary_events, sensor_cols, value = NA_real_)

  joined <- primary_events |>
    left_join(
      supplemental_table,
      by = ".event_key",
      suffix = c("", ".supp")
    )

  fill_log <- empty_sensor_fill_log()
  conflict_log <- empty_sensor_conflict_log()

  for (cc in sensor_cols) {
    supp_col <- paste0(cc, ".supp")

    if (!supp_col %in% names(joined)) {
      next
    }

    primary_missing <- !not_missing_value(joined[[cc]])
    supp_present <- not_missing_value(joined[[supp_col]])
    fill_rows <- primary_missing & supp_present

    conflict_rows <- values_conflict(joined[[cc]], joined[[supp_col]], tolerance = tolerance)

    if (any(fill_rows, na.rm = TRUE)) {
      fill_log <- bind_rows(
        fill_log,
        joined[fill_rows, , drop = FALSE] |>
          transmute(
            .project_name = as.character(.project_name),
            .source = as.character(.source),
            .tag_id_upload = as.character(.tag_id_upload),
            .timestamp_utc = as_utc_posixct(.timestamp_utc),
            .event_key = as.character(.event_key),
            column = cc,
            old_value = as.character(.data[[cc]]),
            new_value = as.character(.data[[supp_col]]),
            supplemental_sources = as.character(.supplemental_sources)
          )
      )
    }

    if (any(conflict_rows, na.rm = TRUE)) {
      conflict_log <- bind_rows(
        conflict_log,
        joined[conflict_rows, , drop = FALSE] |>
          transmute(
            .project_name = as.character(.project_name),
            .source = as.character(.source),
            .tag_id_upload = as.character(.tag_id_upload),
            .timestamp_utc = as_utc_posixct(.timestamp_utc),
            .event_key = as.character(.event_key),
            column = cc,
            current_value = as.character(.data[[cc]]),
            proposed_value = as.character(.data[[supp_col]]),
            supplemental_sources = as.character(.supplemental_sources)
          )
      )
    }

    joined[[cc]][fill_rows] <- joined[[supp_col]][fill_rows]
  }

  drop_cols <- c(
    ".supplemental_sources",
    paste0(sensor_cols, ".supp")
  )

  repaired_events <- joined |>
    select(-any_of(drop_cols)) |>
    normalize_event_bind_types()

  list(
    events = repaired_events,
    fill_log = fill_log,
    conflict_log = conflict_log,
    within_supplement_conflicts = supplemental_info$within_supplement_conflicts
  )
}

identify_current_sensor_repairs <- function(best_events,
                                            current_events,
                                            tolerance = 1e-8) {
  best_events <- best_events |>
    standardize_supplemental_sensor_columns() |>
    normalize_event_bind_types()

  current_events <- current_events |>
    standardize_supplemental_sensor_columns() |>
    normalize_event_bind_types()

  sensor_cols <- sensor_repair_columns(best_events, current_events)
  best_events <- add_missing_columns(best_events, sensor_cols, value = NA_real_)
  current_events <- add_missing_columns(current_events, sensor_cols, value = NA_real_)

  current_small <- current_events |>
    arrange(.event_key, desc(.source_rank)) |>
    distinct(.event_key, .keep_all = TRUE) |>
    select(.event_key, any_of(sensor_cols))

  joined <- best_events |>
    left_join(
      current_small,
      by = ".event_key",
      suffix = c("", ".current")
    )

  missing_log <- tibble::tibble(
    .project_name = character(),
    .source = character(),
    .tag_id_upload = character(),
    .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
    .event_key = character(),
    column = character(),
    current_value = character(),
    export_value = character()
  )

  conflict_log <- tibble::tibble(
    .project_name = character(),
    .source = character(),
    .tag_id_upload = character(),
    .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
    .event_key = character(),
    column = character(),
    current_value = character(),
    export_value = character()
  )

  for (cc in sensor_cols) {
    current_col <- paste0(cc, ".current")

    if (!current_col %in% names(joined)) {
      next
    }

    export_present <- not_missing_value(joined[[cc]])
    current_missing <- !not_missing_value(joined[[current_col]])
    fill_needed <- export_present & current_missing

    conflict_rows <- values_conflict(joined[[current_col]], joined[[cc]], tolerance = tolerance)

    if (any(fill_needed, na.rm = TRUE)) {
      missing_log <- bind_rows(
        missing_log,
        joined[fill_needed, , drop = FALSE] |>
          transmute(
            .project_name = as.character(.project_name),
            .source = as.character(.source),
            .tag_id_upload = as.character(.tag_id_upload),
            .timestamp_utc = as_utc_posixct(.timestamp_utc),
            .event_key = as.character(.event_key),
            column = cc,
            current_value = as.character(.data[[current_col]]),
            export_value = as.character(.data[[cc]])
          )
      )
    }

    if (any(conflict_rows, na.rm = TRUE)) {
      conflict_log <- bind_rows(
        conflict_log,
        joined[conflict_rows, , drop = FALSE] |>
          transmute(
            .project_name = as.character(.project_name),
            .source = as.character(.source),
            .tag_id_upload = as.character(.tag_id_upload),
            .timestamp_utc = as_utc_posixct(.timestamp_utc),
            .event_key = as.character(.event_key),
            column = cc,
            current_value = as.character(.data[[current_col]]),
            export_value = as.character(.data[[cc]])
          )
      )
    }
  }

  repair_event_keys <- missing_log |>
    distinct(.event_key)

  list(
    missing_log = missing_log,
    conflict_log = conflict_log,
    repair_event_keys = repair_event_keys
  )
}

# =============================================================================
# Restrict comparisons to the current Movebank download
# =============================================================================

add_identity_columns <- function(df) {
  needed <- c(
    ".project_name",
    ".tag_id_upload",
    ".animal_id_upload",
    ".deployment_id",
    ".event_key",
    ".timestamp_utc"
  )

  for (cc in needed) {
    if (!cc %in% names(df)) {
      df[[cc]] <- rep(NA_character_, nrow(df))
    }
  }

  if (!inherits(df$.timestamp_utc, "POSIXt")) {
    df$.timestamp_utc <- as_utc_posixct(df$.timestamp_utc)
  }

  df |>
    mutate(
      .project_name = as.character(.project_name),
      .tag_id_upload = as.character(.tag_id_upload),
      .animal_id_upload = as.character(.animal_id_upload),
      .deployment_id = as.character(.deployment_id),
      .event_key = as.character(.event_key)
    )
}

non_empty_chr <- function(x) {
  !is.na(x) & as.character(x) != ""
}

unique_project_map <- function(current_events, id_col, project_col_name) {
  if (!id_col %in% names(current_events)) {
    return(tibble::tibble(
      !!id_col := character(),
      !!project_col_name := character()
    ))
  }

  current_events |>
    filter(non_empty_chr(.data[[id_col]]), non_empty_chr(.project_name)) |>
    transmute(
      .id_value = as.character(.data[[id_col]]),
      .project_name = as.character(.project_name)
    ) |>
    distinct() |>
    group_by(.id_value) |>
    filter(n_distinct(.project_name) == 1) |>
    summarise(
      !!project_col_name := dplyr::first(.project_name),
      .groups = "drop"
    ) |>
    rename(!!id_col := .id_value)
}

infer_project_from_current_reference <- function(events, current_events) {
  events <- add_identity_columns(events)
  current_events <- add_identity_columns(current_events)

  tag_map <- unique_project_map(current_events, ".tag_id_upload", ".project_from_current_tag")
  animal_map <- unique_project_map(current_events, ".animal_id_upload", ".project_from_current_animal")
  dep_map <- unique_project_map(current_events, ".deployment_id", ".project_from_current_deployment")

  events |>
    left_join(tag_map, by = ".tag_id_upload") |>
    left_join(animal_map, by = ".animal_id_upload") |>
    left_join(dep_map, by = ".deployment_id") |>
    mutate(
      .project_name = dplyr::coalesce(
        .project_name,
        .project_from_current_tag,
        .project_from_current_animal,
        .project_from_current_deployment
      )
    ) |>
    select(
      -any_of(c(
        ".project_from_current_tag",
        ".project_from_current_animal",
        ".project_from_current_deployment"
      ))
    )
}

filter_to_current_movebank_reference <- function(events, current_events) {
  events <- infer_project_from_current_reference(events, current_events) |>
    normalize_event_bind_types()

  current_events <- add_identity_columns(current_events) |>
    normalize_event_bind_types()

  if (nrow(events) == 0 || nrow(current_events) == 0) {
    return(events[0, , drop = FALSE])
  }

  events$.tmp_current_reference_row <- seq_len(nrow(events))
  keep_rows <- integer()

  current_tag_ref <- current_events |>
    filter(non_empty_chr(.project_name), non_empty_chr(.tag_id_upload)) |>
    distinct(.project_name, .tag_id_upload)

  if (nrow(current_tag_ref) > 0) {
    keep_rows <- c(
      keep_rows,
      events |>
        filter(non_empty_chr(.project_name), non_empty_chr(.tag_id_upload)) |>
        semi_join(current_tag_ref, by = c(".project_name", ".tag_id_upload")) |>
        pull(.tmp_current_reference_row)
    )
  }

  current_animal_ref <- current_events |>
    filter(non_empty_chr(.project_name), non_empty_chr(.animal_id_upload)) |>
    distinct(.project_name, .animal_id_upload)

  if (nrow(current_animal_ref) > 0) {
    keep_rows <- c(
      keep_rows,
      events |>
        filter(non_empty_chr(.project_name), non_empty_chr(.animal_id_upload)) |>
        semi_join(current_animal_ref, by = c(".project_name", ".animal_id_upload")) |>
        pull(.tmp_current_reference_row)
    )
  }

  current_deployment_ref <- current_events |>
    filter(non_empty_chr(.project_name), non_empty_chr(.deployment_id)) |>
    distinct(.project_name, .deployment_id)

  if (nrow(current_deployment_ref) > 0) {
    keep_rows <- c(
      keep_rows,
      events |>
        filter(non_empty_chr(.project_name), non_empty_chr(.deployment_id)) |>
        semi_join(current_deployment_ref, by = c(".project_name", ".deployment_id")) |>
        pull(.tmp_current_reference_row)
    )
  }

  keep_rows <- sort(unique(keep_rows))

  events |>
    filter(.tmp_current_reference_row %in% keep_rows) |>
    select(-.tmp_current_reference_row) |>
    normalize_event_bind_types()
}

# =============================================================================
# Project folder diagnostics
# =============================================================================

project_dir <- function(out_dir, project_name) {
  fs::path(out_dir, safe_file_name(project_name))
}

write_project_diagnostics <- function(out_dir, current_events, diagnostics) {
  current_events <- add_identity_columns(current_events)

  projects <- sort(unique(current_events$.project_name))
  projects <- projects[non_empty_chr(projects)]

  purrr::walk(projects, function(project_name) {
    p_dir <- project_dir(out_dir, project_name)
    fs::dir_create(p_dir)

    purrr::iwalk(diagnostics, function(df, file_name) {
      if (is.null(df)) {
        df <- tibble::tibble()
      }

      if (".project_name" %in% names(df)) {
        df_project <- df |>
          filter(.project_name == project_name)
      } else {
        df_project <- df
      }

      readr::write_csv(
        df_project,
        fs::path(p_dir, file_name),
        na = ""
      )
    })
  })

  invisible(TRUE)
}

# =============================================================================
# Export per project
# =============================================================================

export_project_csvs <- function(events, out_dir, current_events = NULL) {
  fs::dir_create(out_dir)

  if (nrow(events) == 0) {
    report <- tibble(
      project_name = character(),
      file = character(),
      n_rows = integer(),
      path = character()
    )

    readr::write_csv(report, fs::path(out_dir, "reupload_export_report.csv"), na = "")
    return(report)
  }

  if (!is.null(current_events) && nrow(current_events) > 0) {
    projects <- sort(unique(current_events$.project_name))
  } else {
    projects <- sort(unique(events$.project_name))
  }

  projects <- projects[non_empty_chr(projects)]

  report <- purrr::map_dfr(projects, function(project_name) {
    p_events <- events |>
      filter(.project_name == project_name) |>
      arrange(.tag_id_upload, .timestamp_utc)

    p_dir <- project_dir(out_dir, project_name)
    fs::dir_create(p_dir)

    csvs <- list(
      data = build_location_csv(p_events),
      temp = build_temp_csv(p_events),
      pressure = build_pressure_csv(p_events),
      vedba = build_vedba_csv(p_events)
    )

    purrr::imap_dfr(csvs, function(df, nm) {
      file_name <- paste0("reupload_", nm, ".csv")
      file_path <- fs::path(p_dir, file_name)

      if (nrow(df) > 0) {
        readr::write_csv(df, file_path, na = "")
      }

      tibble(
        project_name = project_name,
        file = file_name,
        n_rows = nrow(df),
        path = as.character(file_path)
      )
    })
  })

  readr::write_csv(report, fs::path(out_dir, "reupload_export_report.csv"), na = "")
  report
}

# =============================================================================
# Main wrapper
# =============================================================================

prepare_movebank_reupload_exports <- function(
    local_paths,
    current_movebank,
    out_dir,
    projects = NULL,
    export_mode = c("missing_events", "all_best"),
    supplemental_paths = NULL,
    sensor_conflict_tolerance = 1e-8
) {
  export_mode <- match.arg(export_mode)

  fs::dir_create(out_dir)

  current_events <- extract_event_table(
    m = current_movebank,
    source_label = "CURRENT_MOVEBANK",
    source_rank = 999
  ) |>
    normalize_event_bind_types()

  if (!is.null(projects)) {
    current_events <- current_events |>
      filter(.project_name %in% projects)
  }

  # Everything below is constrained by the current Movebank download.
  # A tag/animal/deployment must already exist in the downloaded project to be
  # considered for comparison, repair, diagnostics, or export.
  current_reference <- current_events |>
    distinct(.project_name, .tag_id_upload, .animal_id_upload, .deployment_id)

  local_objs <- purrr::imap(local_paths, function(path, label) {
    load_move_rdata(path = path, label = label)
  })

  candidate_event_list <- purrr::map2(local_objs, seq_along(local_objs), function(x, i) {
    extract_event_table(
      m = x$object,
      source_label = x$label,
      source_rank = i
    )
  })

  candidate_events_raw <- bind_rows_safely(candidate_event_list) |>
    normalize_event_bind_types()

  candidate_events <- candidate_events_raw |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  candidate_excluded_not_in_current <- candidate_events_raw |>
    infer_project_from_current_reference(current_events) |>
    anti_join(
      candidate_events |> distinct(.source, .event_key),
      by = c(".source", ".event_key")
    ) |>
    transmute(
      .project_name = as.character(.project_name),
      .source = as.character(.source),
      .tag_id_upload = as.character(.tag_id_upload),
      .animal_id_upload = as.character(.animal_id_upload),
      .deployment_id = as.character(.deployment_id),
      .timestamp_utc = as_utc_posixct(.timestamp_utc),
      .event_key = as.character(.event_key),
      reason = "tag_animal_or_deployment_not_in_current_movebank_download"
    ) |>
    filter(non_empty_chr(.project_name)) |>
    distinct()

  supplemental_events_raw <- load_supplemental_events(supplemental_paths)

  supplemental_events <- if (!is.null(supplemental_events_raw) && nrow(supplemental_events_raw) > 0) {
    supplemental_events_raw |>
      filter_to_current_movebank_reference(current_events) |>
      standardize_supplemental_sensor_columns() |>
      normalize_event_bind_types()
  } else {
    supplemental_events_raw
  }

  supplemental_excluded_not_in_current <- if (!is.null(supplemental_events_raw) && nrow(supplemental_events_raw) > 0) {
    supplemental_events_raw |>
      infer_project_from_current_reference(current_events) |>
      anti_join(
        supplemental_events |> distinct(.source, .event_key),
        by = c(".source", ".event_key")
      ) |>
      transmute(
        .project_name = as.character(.project_name),
        .source = as.character(.source),
        .tag_id_upload = as.character(.tag_id_upload),
        .animal_id_upload = as.character(.animal_id_upload),
        .deployment_id = as.character(.deployment_id),
        .timestamp_utc = as_utc_posixct(.timestamp_utc),
        .event_key = as.character(.event_key),
        reason = "tag_animal_or_deployment_not_in_current_movebank_download"
      ) |>
      filter(non_empty_chr(.project_name)) |>
      distinct()
  } else {
    tibble::tibble(
      .project_name = character(),
      .source = character(),
      .tag_id_upload = character(),
      .animal_id_upload = character(),
      .deployment_id = character(),
      .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
      .event_key = character(),
      reason = character()
    )
  }

  supplemental_fill <- fill_missing_sensor_values_from_supplemental(
    primary_events = candidate_events,
    supplemental_events = supplemental_events,
    tolerance = sensor_conflict_tolerance
  )

  candidate_events <- supplemental_fill$events |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  supplemental_events_not_in_current <- if (!is.null(supplemental_events) && nrow(supplemental_events) > 0) {
    supplemental_events |>
      anti_join(current_events |> distinct(.project_name, .event_key), by = c(".project_name", ".event_key")) |>
      transmute(
        .project_name = as.character(.project_name),
        .source = as.character(.source),
        .tag_id_upload = as.character(.tag_id_upload),
        .animal_id_upload = as.character(.animal_id_upload),
        .deployment_id = as.character(.deployment_id),
        .timestamp_utc = as_utc_posixct(.timestamp_utc),
        .event_key = as.character(.event_key),
        reason = "event_timestamp_not_in_current_movebank_download_for_existing_project_tag_or_animal"
      ) |>
      distinct()
  } else {
    tibble::tibble(
      .project_name = character(),
      .source = character(),
      .tag_id_upload = character(),
      .animal_id_upload = character(),
      .deployment_id = character(),
      .timestamp_utc = as.POSIXct(character(), tz = "UTC"),
      .event_key = character(),
      reason = character()
    )
  }

  best <- choose_best_project_year_events(candidate_events)
  best_events <- best$best_events |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  current_sensor_repair <- identify_current_sensor_repairs(
    best_events = best_events,
    current_events = current_events,
    tolerance = sensor_conflict_tolerance
  )

  current_keys <- current_events |>
    distinct(.project_name, .event_key)

  missing_location_events <- best_events |>
    anti_join(current_keys, by = c(".project_name", ".event_key")) |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  sensor_repair_events <- best_events |>
    semi_join(current_sensor_repair$repair_event_keys, by = ".event_key") |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  missing_events <- bind_rows_safely(list(missing_location_events, sensor_repair_events)) |>
    arrange(.project_name, .deploy_year, .tag_id_upload, .timestamp_utc, desc(.source_rank)) |>
    distinct(.project_name, .deploy_year, .event_key, .keep_all = TRUE) |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  events_to_export <- switch(
    export_mode,
    missing_events = missing_events,
    all_best = best_events
  ) |>
    filter_to_current_movebank_reference(current_events) |>
    normalize_event_bind_types()

  version_deployment_summary <- dplyr::bind_rows(
    candidate_events |>
      transmute(
        .reference = "local_candidate",
        .source = as.character(.source),
        .project_name = as.character(.project_name),
        .deploy_year = as.numeric(.deploy_year),
        .deployment_id = as.character(.deployment_id)
      ),
    current_events |>
      transmute(
        .reference = "current_movebank",
        .source = as.character(.source),
        .project_name = as.character(.project_name),
        .deploy_year = as.numeric(.deploy_year),
        .deployment_id = as.character(.deployment_id)
      )
  ) |>
    distinct() |>
    count(.reference, .source, .project_name, .deploy_year, name = "n_deployments") |>
    arrange(.project_name, .deploy_year, .reference, .source)

  version_event_summary <- dplyr::bind_rows(
    candidate_events |>
      transmute(
        .reference = "local_candidate",
        .source = as.character(.source),
        .project_name = as.character(.project_name),
        .deploy_year = as.numeric(.deploy_year),
        .event_key = as.character(.event_key)
      ),
    current_events |>
      transmute(
        .reference = "current_movebank",
        .source = as.character(.source),
        .project_name = as.character(.project_name),
        .deploy_year = as.numeric(.deploy_year),
        .event_key = as.character(.event_key)
      )
  ) |>
    distinct() |>
    count(.reference, .source, .project_name, .deploy_year, name = "n_events") |>
    arrange(.project_name, .deploy_year, .reference, .source)

  missing_deployment_summary <- missing_events |>
    distinct(.project_name, .deploy_year, .source, .deployment_id, .tag_id_upload) |>
    count(.project_name, .deploy_year, .source, name = "n_missing_deployments") |>
    arrange(.project_name, .deploy_year)

  missing_event_summary <- missing_events |>
    count(.project_name, .deploy_year, .source, name = "n_missing_events") |>
    arrange(.project_name, .deploy_year)

  diagnostics <- list(
    reupload_diagnostic_deployments_by_source_project_year.csv = version_deployment_summary,
    reupload_diagnostic_events_by_source_project_year.csv = version_event_summary,
    reupload_diagnostic_best_source_by_project_year.csv = best$best_source,
    reupload_diagnostic_missing_deployments_by_project_year.csv = missing_deployment_summary,
    reupload_diagnostic_missing_events_by_project_year.csv = missing_event_summary,
    reupload_diagnostic_supplemental_filled_missing_sensor_values.csv = supplemental_fill$fill_log,
    reupload_diagnostic_supplemental_conflicting_sensor_values_for_review.csv = supplemental_fill$conflict_log,
    reupload_diagnostic_supplemental_internal_conflicts_for_review.csv = supplemental_fill$within_supplement_conflicts,
    reupload_diagnostic_current_movebank_missing_sensor_values.csv = current_sensor_repair$missing_log,
    reupload_diagnostic_current_movebank_conflicting_sensor_values_for_review.csv = current_sensor_repair$conflict_log,
    reupload_diagnostic_supplemental_events_not_in_current_movebank.csv = supplemental_events_not_in_current,
    reupload_diagnostic_candidate_events_excluded_not_in_current_movebank_project.csv = candidate_excluded_not_in_current,
    reupload_diagnostic_supplemental_events_excluded_not_in_current_movebank_project.csv = supplemental_excluded_not_in_current
  )

  write_project_diagnostics(
    out_dir = out_dir,
    current_events = current_events,
    diagnostics = diagnostics
  )

  export_report <- export_project_csvs(
    events = events_to_export,
    out_dir = out_dir,
    current_events = current_events
  )

  list(
    current_reference = current_reference,
    candidate_events_raw = candidate_events_raw,
    candidate_events = candidate_events,
    candidate_excluded_not_in_current = candidate_excluded_not_in_current,
    current_events = current_events,
    best_source_by_project_year = best$best_source,
    source_summary = best$source_summary,
    best_events = best_events,
    missing_events = missing_events,
    version_deployment_summary = version_deployment_summary,
    version_event_summary = version_event_summary,
    missing_deployment_summary = missing_deployment_summary,
    missing_event_summary = missing_event_summary,
    supplemental_events_raw = supplemental_events_raw,
    supplemental_events = supplemental_events,
    supplemental_excluded_not_in_current = supplemental_excluded_not_in_current,
    supplemental_fill_log = supplemental_fill$fill_log,
    supplemental_conflict_log = supplemental_fill$conflict_log,
    supplemental_internal_conflicts = supplemental_fill$within_supplement_conflicts,
    supplemental_events_not_in_current = supplemental_events_not_in_current,
    current_movebank_missing_sensor_values = current_sensor_repair$missing_log,
    current_movebank_conflicting_sensor_values = current_sensor_repair$conflict_log,
    sensor_repair_events = sensor_repair_events,
    export_report = export_report
  )
}


# =============================================================================
# Optional diagnostics
# =============================================================================

check_movebank_export_types <- function(local_paths) {
  local_objs <- purrr::imap(local_paths, function(path, label) {
    load_move_rdata(path = path, label = label)
  })

  candidate_event_list <- purrr::map2(local_objs, seq_along(local_objs), function(x, i) {
    extract_event_table(
      m = x$object,
      source_label = x$label,
      source_rank = i
    ) |>
      normalize_event_bind_types()
  })

  purrr::map_dfr(candidate_event_list, function(ev) {
    tibble::tibble(
      source = unique(ev$.source),
      source_rank_class = paste(class(ev$.source_rank), collapse = " / "),
      sensor_type_id_class = if ("sensor_type_id" %in% names(ev)) {
        paste(class(ev$sensor_type_id), collapse = " / ")
      } else {
        NA_character_
      },
      n = nrow(ev)
    )
  })
}

source("../SigfoxTagPrep/R/import_nanofox_movebank.R")

z <- import_nanofox_movebank(
  study_id = 4589981234,
  daily_method = "daytime_only",
  run_daily_metrics = TRUE,
  verbose = TRUE
)

current_movebank <- z$location
current_movebank$geometry |> plot()
plot(rnaturalearth::countries110, add = TRUE)
current_movebank$geometry |> plot(add = TRUE)

local_paths <- c(
  `2026-04-16_daytime_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_daytime_daily_20260416.robj",

  `2026-06-19_solarnoon_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily.robj",

  `2026-06-22_solarnoon_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily_20260622.robj"
)

supplemental_paths <- c(
  `bats_2024` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/bats_2024.robj",
  `bats_master` = 
    "../../../Dropbox/MPI/Noctule/Data/rdata/bats_master.robj"
)

out_dir <- "../../../Dropbox/MPI/Noctule/Data/movebank_reupload_exports_20260623"

res <- prepare_movebank_reupload_exports(
  local_paths = local_paths,
  current_movebank = current_movebank,
  out_dir = out_dir,
  export_mode = "missing_events",
  supplemental_paths = supplemental_paths
)

tag <- "12102DB"
r <- res$missing_events |> filter(tag_local_identifier == tag)
b <- current_movebank |> filter(tag_local_identifier == tag)
b$geometry |> plot(type = "o")
b$geometry |> plot(add = TRUE, col = 2)
which(!(r$timestamp %in% b$timestamp))

b$tinyfox_activity_percent_last_24h

r$tinyfox_pressure_min_last_24h
b$tinyfox_pressure_min_last_24h
plot(b$tinyfox_total_vedba, col = b$tag_fell_off+1, pch = 16)
b$tinyfox_diff_vedba
b$tag_fell_off
b$tag_firmware[1]

