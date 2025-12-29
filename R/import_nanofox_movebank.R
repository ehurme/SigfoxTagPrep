import_nanofox_movebank <- function(
    study_id,
    sensor_external_ids = c("acceleration", "accessory-measurements", "barometer", "sigfox-geolocation"),
    sensor_labels       = c("VeDBA", "avg.temp", "min.baro.pressure", "location"),
    merge_studies = TRUE,
    track_combine = "merge",
    compute_vedba_sum = TRUE,
    vedba_col = "vedba",                 # column in acceleration sensor
    vedba_sum_name = "vedba_sum",        # name to store on location rows
    verbose = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(move2)
    library(move)
    library(lubridate)
  })

  if (length(study_id) < 1) stop("Provide at least one study_id.")
  if (length(sensor_external_ids) != length(sensor_labels)) {
    stop("sensor_external_ids and sensor_labels must have the same length.")
  }

  # ---- helper: safe download study info ----
  get_study_info <- function(id) {
    move2::movebank_download_study_info(study_id = id)
  }

  # ---- helper: get Movebank sensor type lookup table ----
  # Note: move2 uses `movebank_retrieve()` from the move package.
  sensors_tbl <- move2::movebank_retrieve(entity_type = "tag_type") %>%
    as_tibble()

  sensor_selected <- sensors_tbl %>%
    filter(.data$external_id %in% sensor_external_ids) %>%
    mutate(
      sensor_type = sensor_labels[match(.data$external_id, sensor_external_ids)]
    )

  if (nrow(sensor_selected) == 0) {
    stop("None of the requested sensor_external_ids were found in movebank_retrieve(tag_type).")
  }

  if (verbose) {
    message("Selected sensors:")
    print(sensor_selected %>% dplyr::select(id, external_id, sensor_type, is_location_sensor), n = 50)
  }

  # ---- helper: per-study download and processing ----
  download_one_study <- function(id) {
    if (verbose) message("Downloading study: ", id)

    si <- get_study_info(id)

    # Determine which sensor types exist in the study
    # In your study_info, this is coming back as ONE comma-separated string:
    # "Acceleration,Accessory Measurements,Barometer,Sigfox Geolocation"
    study_sensor_names <- si$sensor_type_ids

    # Coerce to character vector and split if needed
    study_sensor_names <- as.character(study_sensor_names)
    study_sensor_names <- unlist(strsplit(study_sensor_names, "\\s*,\\s*"))
    study_sensor_names <- trimws(study_sensor_names)

    # Normalize for robust matching (case/whitespace)
    norm <- function(x) {
      x <- tolower(trimws(as.character(x)))
      x <- gsub("\\s+", " ", x)
      x
    }

    study_sensor_names_n <- norm(study_sensor_names)

    # Match against Movebank tag_type$name (e.g., "Acceleration")
    sensor_selected_n <- sensor_selected %>%
      mutate(name_n = norm(.data$name))

    # Keep only sensors present in the study, but return *IDs* for download
    wanted_ids <- sensor_selected_n %>%
      filter(.data$name_n %in% study_sensor_names_n) %>%
      pull(.data$id)

    if (length(wanted_ids) == 0) {
      stop(
        "Study ", id, " has no matching sensors. ",
        "Study sensors: ", paste(study_sensor_names, collapse = ", "),
        " | Requested: ", paste(sensor_selected$name, collapse = ", ")
      )
    }

    # Download all selected sensors for that study
    b <- move2::movebank_download_study(study_id = id, sensor_type_id = wanted_ids)

    # add capture location as first location
    source("R/mt_add_start.R")
    b <- mt_add_start(b)

    # add lat long
    coords <- st_coordinates(b)
    b$lon <- coords[,1]
    b$lat <- coords[,2]

    # Attach sensor_type label and is_location_sensor
    b <- b %>%
      left_join(
        sensor_selected %>% dplyr::select(id, sensor_type, is_location_sensor),
        by = c("sensor_type_id" = "id")
      )

    # Compute per-fix VeDBA sum and attach to location rows
    if (compute_vedba_sum) {
      if (!(vedba_col %in% names(b))) {
        warning("vedba_col '", vedba_col, "' not found in study ", id,
                ". Skipping vedba_sum computation for this study.")
      } else {
        v_sum <- b %>%
          group_by(.data$timestamp, .data$individual_local_identifier) %>%
          reframe(
            !!vedba_sum_name := sum(.data[[vedba_col]], na.rm = TRUE)
          ) %>%
          mutate(sensor_type = "location") # to join onto location rows only

        # Join vedba_sum onto the "location" sensor rows
        b <- b %>%
          dplyr::left_join(
            v_sum %>% dplyr::select(.data$timestamp, .data$individual_local_identifier, .data$sensor_type, all_of(vedba_sum_name)),
            by = c("timestamp", "individual_local_identifier", "sensor_type")
          )
      }
    }

    # Location-only subset (useful downstream)
    b_loc <- b %>% filter(.data$sensor_type == "location")

    list(
      study_id = id,
      full = b,
      location = b_loc
    )
  }

  res_list <- lapply(study_id, download_one_study)
  names(res_list) <- as.character(study_id)

  # ---- merge studies (move2 stacks) ----
  # We merge both the full object and the location-only object
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

  full_merged <- if (merge_studies) merge_stack(full_list) else full_list
  loc_merged  <- if (merge_studies) merge_stack(loc_list)  else loc_list

  # ---- convenience summaries ----
  if (verbose) {
    if (merge_studies) {
      message("Merged studies: ", paste(study_id, collapse = ", "))
      message("Sensor types present (merged full):")
      print(table(full_merged$sensor_type, useNA = "ifany"))
      message("Timestamp summary (merged full):")
      print(summary(full_merged$timestamp))
    } else {
      message("Downloaded ", length(study_id), " studies (not merged).")
    }
  }

  list(
    sensors_table = sensors_tbl,
    sensors_selected = sensor_selected,
    studies = res_list,
    full = full_merged,
    location = loc_merged
  )
}

# # One study
# x <- import_nanofox_movebank(study_id = 4358705312)
#
# b_full <- x$full
# b_loc  <- x$location
#
# plot(b_loc$geometry)
# summary(b_full$timestamp)
# table(b_full$sensor_type)
#
# # Multiple studies -> merged stack
# y <- import_nanofox_movebank(study_id = c(4358705312, 4589981234, 4737117895))
# leisler_full <- y$full
# leisler_loc  <- y$location
#
# track_data <- mt_track_data(leisler_loc)
# names(leisler_loc)
# track_data$taxon_canonical_name %>% table()
