#' Convert Sigfox Tracking Data to move2 Format
#'
#' This function processes Sigfox tracking data, prepares it for analysis,
#' and optionally plots it using move2 and related geospatial packages.
#'
#' @param data A data frame containing tracking data.
#' @param plot_tracks Logical; if TRUE, generates a plot of the tracking data.
#' @param include_legend Logical; controls the presence of a legend in the plot.
#' @param motionless Logical; check if the tag fell off
#' @return A list containing move2 objects and optionally a plot.
#' @importFrom pacman p_load
#' @importFrom dplyr filter arrange group_by
#' @importFrom mapview mapview
#' @importFrom sf st_set_crs st_bbox
#' @importFrom move2 mt_as_move2 mt_track_id mt_time mt_filter_unique
#' @importFrom janitor clean_names
#' @importFrom rnaturalearth ne_coastline
#' @importFrom ggplot2 ggplot geom_sf theme_linedraw coord_sf theme
#' @examples
#' \dontrun{
#'   # Assuming 'data' is your Sigfox data frame
#'   result <- sigfox_to_move2(data)
#'   if (plot) {
#'     print(result$plot)
#'   }
#' }
#' @export
sigfox_to_move2 <- function(tracks,
                            plot_tracks = TRUE,
                            include_legend = FALSE,
                            motionless = FALSE,
                            make_lines = FALSE) {
  # source("./R/tracking_data_processing.R")
  # Load required libraries dynamically
  pacman::p_load(tidyverse, dplyr, mapview, sf, move2, janitor,
                 rnaturalearth, update = FALSE)

  # Ensure devtools is available
  if (!requireNamespace("devtools", quietly = TRUE)) {
    install.packages("devtools")
  }

  # Check if move2 is installed and compare version
  if ("move2" %in% rownames(installed.packages())) {
    current_version <- packageVersion("move2")
    required_version <- as.package_version("0.2.7")

    if (current_version < required_version) {
      message("Updating 'move2' package to the latest version from GitLab.")
      devtools::install_git('https://gitlab.com/bartk/move2.git')
    } else {
      message("Installed 'move2' package is up to date (Version: ", as.character(current_version), ").")
    }
  } else {
    message("'move2' package is not installed. Installing the latest version from GitLab.")
    devtools::install_git('https://gitlab.com/bartk/move2.git')
  }

  # Clean column names for consistency
  tracks <- tracks |> janitor::clean_names()

  tag_type <- unique(tracks$tag_type)

  # Ensure timestamp column exists
  if (is.null(tracks$timestamp)) {
    tracks$timestamp <- tracks$datetime
  }

  # Process radius from source status
  suppressWarnings(
    try({tracks$radius <- sapply(strsplit(tracks$radius_m_source_status, split = " "), "[", 1) %>% as.numeric()})
  )

  # Additional tracks processing steps
  tracks <- determine_day_night(tracks)
  tracks$time_to_noon <- difftime(tracks$timestamp, tracks$noon, units = "hours")
  tracks <- diff_time(tracks)

  if(tag_type == "tinyfox"){
    tracks <- determine_bursts(tracks)
    tracks$total_vedba <- tracks$total_ve_dba * 3.9 / 1000  # Conversion factor

    tracks <- diff_vedba(tracks)
    tracks$vpm <- tracks$diff_vedba / tracks$diff_time
  }

  tracks <- diff_dist(tracks)
  tracks$ground_sp <- tracks$distance / (tracks$diff_time * 60)

  tracks$hour <- lubridate::hour(tracks$timestamp) + lubridate::minute(tracks$timestamp) / 60
  tracks$doy <- lubridate::yday(tracks$timestamp)

  # Check for motionless tags and determine if a tag fell off
  if(tag_type == "tinyfox"){
    motionless_tag <- 280800 / (60 * 24) * 3.9 / 1000
    tracks <- tag_fell_off(tracks, vedba_threshold = motionless_tag * 2)

    if(motionless){
      tracks <- tracks[tracks$tag_fell_off == FALSE,]
    }
  }

  # Convert to move2 object
  m <- move2::mt_as_move2(
    x = tracks,
    coords = c("longitude", "latitude"),
    time_column = "timestamp",
    na.fail = FALSE,
    track_id_column = "tag_id"
  ) |> sf::st_set_crs(4326L)

  # # Ensure data is properly ordered and filtered
  # m <- mt_preprocess(m)

  # add attributes to move2 object
  track_data <-
    m %>% group_by(tag_id) %>%
    reframe(
      deployment_id = NA,
      individual_id = NA,
      animal_life_stage = if (all(is.na(age))) NA else factor(first(na.omit(age))),
      animal_mass = if (all(is.na(capture_weight))) NA else as.numeric(first(na.omit(capture_weight))),
      animal_reproductive_condition = if (all(is.na(repro_status))) NA else factor(first(na.omit(repro_status))),
      attachment_body_part = factor("back"),
      attachment_comments = NA,
      attachment_type = if (all(is.na(attachment_type))) NA else factor(first(na.omit(attachment_type))),
      capture_method = NA,
      capture_timestamp = first(timestamp),
      deployment_comments = NA,
      deploy_on_person = NA,#factor("Edward Hurme"),
      deploy_on_timestamp = first(timestamp),
      manipulation_type = NA,
      tag_firmware = if (all(is.na(firmware))) NA else factor(first(na.omit(firmware))),
      tag_mass_total = first(tag_weight),
      tag_readout_method = factor("LPWAN"),
      tag_settings = first(tag_type), # factor("tinyfox"),
      sensor_type_ids = factor("sigfox-geolocation"),
      # capture_location = if (all(is.na(capture_latitude))) st_point(x = c(NA_real_, NA_real_)) else factor(first(na.omit(st_point(x = c(capture_longitude,
      capture_latitude = if (all(is.na(capture_latitude))) NA else as.numeric(first(na.omit(capture_latitude))),
      capture_longitude = if (all(is.na(capture_longitude))) NA else as.numeric(first(na.omit(capture_longitude))),
      deploy_on_latitude = if (all(is.na(deploy_on_latitude))) NA else as.numeric(first(na.omit(deploy_on_latitude))),
      deploy_on_longitude = if (all(is.na(deploy_on_longitude))) NA else as.numeric(first(na.omit(deploy_on_longitude))),
      # deploy_on_location = NA,
      # st_point()
      deploy_off_location = NA,
      individual_comments = NA,
      sex = first(sex),
      taxon_canonical_name = if (all(is.na(species))) NA else factor(first(na.omit(species))),
      individual_number_of_deployments = 1,
      mortality_location = NA,
      weight = first(capture_weight),
      tag_number_of_deployments = 1,
      study_id = NA,
      has_quota = TRUE,
      i_am_owner = TRUE,
      is_test = FALSE,
      license_type = factor("CC_BY"),
      name = NA, # factor("Noctule spring migration 2024"),
      study_number_of_deployments = n(),
      number_of_individuals = n(),
      number_of_tags = n(),
      principal_investigator_name = NA, #"edwardhurme (Edward Hurme)",
      study_type = factor("research"),
      suspend_license_terms = FALSE,
      i_can_see_data = TRUE,
      there_are_data_which_i_cannot_see = FALSE,
      i_have_download_access = TRUE,
      i_am_collaborator = FALSE,
      study_permission = factor("data_manager", levels = c("collaborator", "data_manager", "na")),
      timestamp_first_deployed_location = NA,
      timestamp_last_deployed_location = NA,
      number_of_deployed_locations = NA,
      taxon_ids = if (all(is.na(species))) NA else factor(first(na.omit(species))),
      contact_person_name = NA, # "Edward Hurme",
      main_location = NA #sf::st_point()
    )

  track_data <- sf::st_as_sf(track_data,
                             coords = c("capture_longitude","capture_latitude"),
                             na.fail = FALSE, remove = FALSE,
                             sf_column_name = "capture_location")

  track_data <- sf::st_as_sf(as.data.frame(track_data),
                             coords = c("deploy_on_longitude","deploy_on_latitude"),
                             na.fail = FALSE, remove = FALSE,
                             sf_column_name = "deploy_on_location")

  m <- mt_set_track_data(x = m, data = track_data)

  lat_lon <- sf::st_coordinates(m$geometry)
  m$latitude <- lat_lon[,2]
  m$longitude <- lat_lon[,1]

  m <- dplyr::arrange(m, mt_track_id(m))

  m <- m %>%
    mt_set_track_id(value = "tag_id") %>%
    mt_set_time(value = "timestamp")

  # melt temperature and vedba
  if(tag_type == "nanofox"){
    df_long <- m %>%
      pivot_longer(
        cols = matches("^(ve_dba|avg_temp)_\\d+_"),
        names_to = c("metric", "time_window"),  # Removed 'descriptor'
        names_pattern = "(ve_dba|avg_temp)_(\\d+)_.*",  # Adjusted regex to ignore descriptor
        values_to = "value",
        values_drop_na = TRUE
      ) %>%
      mutate(
        time_window = as.numeric(time_window),
        measurement_time = timestamp - lubridate::minutes(36 * time_window),
        metric = recode(metric,
                        "ve_dba" = "vedba",
                        "avg_temp" = "temperature")
      ) %>%
      pivot_wider(
        names_from = metric,
        values_from = value
      )
  }

  # Check for tags with more than one location
  ml <- {}
  if(make_lines){
    try({
      m_clean <- m[!sf::st_is_empty(m$geometry),]
      ml <-
        m_clean %>%
        dplyr::group_by(tag_id) %>%
        dplyr::filter(n() > 1) %>%  # Ensure at least 2 points per track
        mt_track_lines(
          n = dplyr::n(),           # Add count of points for each track
          start = min(timestamp), # First timestamp in track
          end = max(timestamp)  # Last timestamp in track
        )
    })
  }

  # Regularize to daily locations
  if(tag_type != "nanofox"){
    suppressWarnings(m_day <- regularize_to_daily(tracks))
  }

  if(tag_type == "nanofox"){
    suppressWarnings(m_day <- regularize_to_daily(df_long))
  }

  results <- list()
  results[[1]] <- m
  if(make_lines){
    results[[2]] <- ml
  }
  results[[3]] <- m_day
  if(plot_tracks){
    results[[4]] <- plot_tracking_data(m = m, ml = ml, plot_lines = TRUE)
  }
  if(tag_type == "nanofox"){
    results[[5]] <- df_long
  }
  return(results)
}

#' Preprocess move2 Data
#'
#' Ensures move2 data is clean and ready for analysis.
#' @param m move2 object.
#' @return Preprocessed move2 object.
mt_preprocess <- function(m) {
  if (!move2::mt_is_track_id_cleaved(m)) {
    m <- dplyr::arrange(m, move2::mt_track_id(m))
  }

  if (!move2::mt_is_time_ordered(m)) {
    m <- dplyr::arrange(m, move2::mt_track_id(m), move2::mt_time(m))
  }

  if (!move2::mt_has_unique_location_time_records(m)) {
    m <- move2::mt_filter_unique(m)
  }

  if (!move2::mt_has_no_empty_points(m)) {
    # m <- dplyr::filter(m, !sf::st_is_empty(m))
    # m[sf::st_is_empty(m),]$geometry
    m <- m[!sf::st_is_empty(m),]
  }

  return(m)
}

#' Plot Tracking Data
#'
#' Creates a plot of tracking data using ggplot2 and rnaturalearth.
#' @param m move2 object with tracking data.
#' @param ml Lines representing movements of tags.
#' @param legend Logical; whether to include a legend.
#' @return A ggplot object.
plot_tracking_data <- function(m, ml, legend, plot_lines = TRUE) {
  extent <- m$geometry %>% sf::st_bbox()

  first_points <- m %>%
    group_by(tag_id) %>%
    slice_min(order_by = timestamp, n = 1) %>%
    ungroup()

  last_points <- m %>%
    group_by(tag_id) %>%
    slice_max(order_by = timestamp, n = 1) %>%
    ungroup()

  # Base plot with naturalearth background
  p <- ggplot() +
    geom_sf(data = rnaturalearth::ne_countries(returnclass = "sf", scale = 10)) +
    theme_linedraw()

  # Add the full tracks
  p <- p + geom_sf(data = m, aes(color = tag_id))

  p <- p + geom_path(data = m, aes(longitude, latitude, color = tag_id))
  # ggplot(m, aes(longitude, latitude, group = tag_id))+geom_path()

  # Highlight first and last points
  p <- p +
    geom_sf(data = first_points, aes(fill = tag_id), shape = 21, size = 6) +
    geom_sf(data = last_points, aes(fill = tag_id), shape = 24, size = 6) +
    scale_shape_manual(
      name = "Point Type",
      values = c(First = 24, Last = 21),  # 24 is triangle, 21 is circle
      labels = c("First Point", "Last Point")
    )+
    guides(
      color = guide_legend(title = "Tag ID"),
      shape = guide_legend(title = "Point Type")
    )

  # Coordinate system setup, assuming extent is defined as xmin, xmax, ymin, ymax
  p <- p + coord_sf(xlim = c(extent$xmin, extent$xmax), ylim = c(extent$ymin, extent$ymax))

  # p <- ggplot() +
  #   geom_sf(data = rnaturalearth::ne_countries(returnclass = "sf", scale = 10)) +
  #   theme_linedraw() +
  #   geom_sf(data = m, aes(color = tag_id)) +
  #   geom_sf(data = ml, aes(color = tag_id)) +
  #   coord_sf(xlim = c(extent$xmin, extent$xmax), ylim = c(extent$ymin, extent$ymax))

  if (!legend) {
    p <- p + theme(legend.position = "none")
  }
  p
  return(p)
}

