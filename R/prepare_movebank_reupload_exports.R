# =============================================================================
# prepare_movebank_reupload_exports.R
#
# Compare local Movebank versions to the current Movebank upload and export
# project-specific CSVs for re-upload.
#
# Outputs per project:
#   data.csv      = Sigfox geolocation / location data
#   temp.csv      = accessory measurements / temperature / activity
#   pressure.csv  = barometric pressure
#   vedba.csv     = acceleration / VeDBA
#
# Edward Hurme
# Updated: 2026-06-23
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
  # Factors and ordered factors
  df <- df |>
    dplyr::mutate(
      dplyr::across(where(is.factor), as.character),
      dplyr::across(where(is.ordered), as.character)
    )

  # bit64::integer64 IDs from Movebank
  df <- df |>
    dplyr::mutate(
      dplyr::across(where(~ inherits(.x, "integer64")), as.character)
    )

  # Time-like columns that should remain POSIXct
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

  # ID-like and categorical columns should be character
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

  # Internal columns with intended types
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

  # These are explicitly normalized and should not be coerced to character
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

  # Keep the original Movebank time column consistent.
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

common_upload_cols <- function(events) {
  out <- tibble(
    `tag ID` = events$.tag_id_upload,
    Animal.ID = events$.animal_id_upload,
    timestamp = fmt_time_upload(events$.timestamp_utc)
  )

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

build_location_csv <- function(events) {
  lat_col <- first_existing(names(events), c("location_lat", "location lat", "lat", "latitude"))
  lon_col <- first_existing(names(events), c("location_long", "location long", "lon", "long", "longitude"))

  lat <- if (!is.na(lat_col)) events[[lat_col]] else events$.lat_from_geom
  lon <- if (!is.na(lon_col)) events[[lon_col]] else events$.lon_from_geom

  sigfox_cols <- intersect(
    c(
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
    ),
    names(events)
  )

  base_station_cols <- regex_cols(events, "base.?station|basestation|base_stations")
  base_station_cols <- remove_internal_cols(base_station_cols)

  out <- bind_cols(
    common_upload_cols(events),
    tibble(
      `location lat` = lat,
      `location long` = lon
    ),
    events[, unique(c(sigfox_cols, base_station_cols)), drop = FALSE]
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
  value_cols <- regex_cols(events, value_pattern)

  value_cols <- setdiff(
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

  if (length(value_cols) == 0) {
    return(tibble())
  }

  values <- events[, value_cols, drop = FALSE]
  has_value <- row_has_any_value(values)

  out <- bind_cols(
    common_upload_cols(events),
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
# Export per project
# =============================================================================

export_project_csvs <- function(events, out_dir) {
  fs::dir_create(out_dir)

  if (nrow(events) == 0) {
    report <- tibble(
      project_name = character(),
      file = character(),
      n_rows = integer(),
      path = character()
    )

    readr::write_csv(report, fs::path(out_dir, "export_report.csv"), na = "")
    return(report)
  }

  projects <- sort(unique(events$.project_name))
  projects <- projects[!is.na(projects)]

  report <- purrr::map_dfr(projects, function(project_name) {
    p_events <- events |>
      filter(.project_name == project_name) |>
      arrange(.tag_id_upload, .timestamp_utc)

    p_dir <- fs::path(out_dir, safe_file_name(project_name))
    fs::dir_create(p_dir)

    csvs <- list(
      data = build_location_csv(p_events),
      temp = build_temp_csv(p_events),
      pressure = build_pressure_csv(p_events),
      vedba = build_vedba_csv(p_events)
    )

    purrr::imap_dfr(csvs, function(df, nm) {
      file_path <- fs::path(p_dir, paste0(nm, ".csv"))

      if (nrow(df) > 0) {
        readr::write_csv(df, file_path, na = "")
      }

      tibble(
        project_name = project_name,
        file = paste0(nm, ".csv"),
        n_rows = nrow(df),
        path = as.character(file_path)
      )
    })
  })

  readr::write_csv(report, fs::path(out_dir, "export_report.csv"), na = "")
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
    export_mode = c("missing_events", "all_best")
) {
  export_mode <- match.arg(export_mode)

  fs::dir_create(out_dir)

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

  candidate_events <- bind_rows_safely(candidate_event_list)

  current_events <- extract_event_table(
    m = current_movebank,
    source_label = "CURRENT_MOVEBANK",
    source_rank = 999
  ) |>
    normalize_event_bind_types()

  if (!is.null(projects)) {
    candidate_events <- candidate_events |>
      filter(.project_name %in% projects)

    current_events <- current_events |>
      filter(.project_name %in% projects)
  }

  best <- choose_best_project_year_events(candidate_events)
  best_events <- best$best_events

  current_keys <- current_events |>
    distinct(.project_name, .event_key)

  missing_events <- best_events |>
    anti_join(current_keys, by = c(".project_name", ".event_key"))

  events_to_export <- switch(
    export_mode,
    missing_events = missing_events,
    all_best = best_events
  )

  # Use only diagnostic columns here to avoid unrelated type conflicts.
  version_deployment_summary <- dplyr::bind_rows(
    candidate_events |>
      transmute(
        .reference = "local_candidate",
        .source,
        .project_name,
        .deploy_year,
        .deployment_id
      ),
    current_events |>
      transmute(
        .reference = "current_movebank",
        .source,
        .project_name,
        .deploy_year,
        .deployment_id
      )
  ) |>
    distinct() |>
    count(.reference, .source, .project_name, .deploy_year, name = "n_deployments") |>
    arrange(.project_name, .deploy_year, .reference, .source)

  version_event_summary <- dplyr::bind_rows(
    candidate_events |>
      transmute(
        .reference = "local_candidate",
        .source,
        .project_name,
        .deploy_year,
        .event_key
      ),
    current_events |>
      transmute(
        .reference = "current_movebank",
        .source,
        .project_name,
        .deploy_year,
        .event_key
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

  readr::write_csv(
    version_deployment_summary,
    fs::path(out_dir, "diagnostic_deployments_by_source_project_year.csv"),
    na = ""
  )

  readr::write_csv(
    version_event_summary,
    fs::path(out_dir, "diagnostic_events_by_source_project_year.csv"),
    na = ""
  )

  readr::write_csv(
    best$best_source,
    fs::path(out_dir, "diagnostic_best_source_by_project_year.csv"),
    na = ""
  )

  readr::write_csv(
    missing_deployment_summary,
    fs::path(out_dir, "diagnostic_missing_deployments_by_project_year.csv"),
    na = ""
  )

  readr::write_csv(
    missing_event_summary,
    fs::path(out_dir, "diagnostic_missing_events_by_project_year.csv"),
    na = ""
  )

  export_report <- export_project_csvs(events_to_export, out_dir)

  list(
    candidate_events = candidate_events,
    current_events = current_events,
    best_source_by_project_year = best$best_source,
    source_summary = best$source_summary,
    best_events = best_events,
    missing_events = missing_events,
    version_deployment_summary = version_deployment_summary,
    version_event_summary = version_event_summary,
    missing_deployment_summary = missing_deployment_summary,
    missing_event_summary = missing_event_summary,
    export_report = export_report
  )
}



# =============================================================================
# Run Movebank re-upload export
# =============================================================================

source("../SigfoxTagPrep/R/prepare_movebank_reupload_exports.R")
source("../SigfoxTagPrep/R/import_nanofox_movebank.R")

# Current Movebank upload
z <- import_nanofox_movebank(
  study_id = 3597331705,
  daily_method = "daytime_only",
  run_daily_metrics = TRUE,
  verbose = TRUE
)

current_movebank <- z$location

# Local versions to compare against current upload
local_paths <- c(
  `2026-04-16_daytime_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_daytime_daily_20260416.robj",

  `2026-06-19_solarnoon_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily.robj",

  `2026-06-22_solarnoon_daily` =
    "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats_solarnoon_daily_20260622.robj"
)

out_dir <- "../../../Dropbox/MPI/Noctule/Data/movebank_reupload_exports_20260623"

res <- prepare_movebank_reupload_exports(
  local_paths = local_paths,
  current_movebank = current_movebank,
  out_dir = out_dir,
  export_mode = "missing_events"
)
