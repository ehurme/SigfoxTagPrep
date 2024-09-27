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
sigfox_to_move2 <- function(data, plot_tracks = TRUE, include_legend = FALSE, motionless = TRUE, make_lines = TRUE) {
  # source("./R/tracking_data_processing.R")
  # Load required libraries dynamically
  pacman::p_load(tidyverse, dplyr, mapview, sf, move2, janitor, rnaturalearth, update = FALSE)

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
  data <- data |> janitor::clean_names()

  # Ensure timestamp column exists
  if (is.null(data$timestamp)) {
    data$timestamp <- data$datetime
  }

  # Process radius from source status
  suppressWarnings(
    try({data$radius <- sapply(strsplit(data$radius_m_source_status, split = " "), "[", 1) %>% as.numeric()})
  )

  # Additional data processing steps
  data <- determine_day_night(data)
  data$time_to_noon <- difftime(data$timestamp, data$noon, units = "hours")
  data <- determine_bursts(data)
  data$total_vedba <- data$total_ve_dba * 3.9 / 1000  # Conversion factor

  data <- diff_vedba(data)
  data$vpm <- data$diff_vedba / data$diff_time

  data <- diff_dist(data)
  data$ground_sp <- data$distance / (data$diff_time * 60)

  data$hour <- lubridate::hour(data$timestamp) + lubridate::minute(data$timestamp) / 60
  data$doy <- lubridate::yday(data$timestamp)

  # Check for motionless tags and determine if a tag fell off
  motionless_tag <- 280800 / (60 * 24) * 3.9 / 1000
  data <- tag_fell_off(data, vedba_threshold = motionless_tag * 2)

  if(motionless == TRUE){
    data <- data[data$tag_fell_off == FALSE,]
  }
  # Convert to move2 object
  m2 <- move2::mt_as_move2(
    x = data,
    coords = c("longitude", "latitude"),
    time_column = "timestamp",
    na.fail = FALSE,
    track_id_column = "tag_id"
  ) |> sf::st_set_crs(4326L)

  # # Ensure data is properly ordered and filtered
  # m2 <- mt_preprocess(m2)

  # add attributes to move2 object
  track_data <-
    m2 %>% group_by(tag_id) %>%
    reframe(
      deployment_id = NA,
      individual_id = NA,
      animal_life_stage = first(m2$age),
      animal_mass = first(capture_weight),
      animal_reproductive_condition = first(repro_status),
      attachment_body_part = factor("back"),
      attachment_comments = NA,
      attachment_type = first(attachment_type),
      capture_method = NA,
      capture_timestamp = first(timestamp),
      deployment_comments = NA,
      deploy_on_person = NA,#factor("Edward Hurme"),
      deploy_on_timestamp = first(timestamp),
      manipulation_type = NA,
      tag_firmware = first(firmware), # factor("V13P"),
      tag_mass_total = first(tag_weight),
      tag_readout_method = factor("LPWAN"),
      tag_settings = NA, # factor("tinyfox"),
      sensor_type_ids = factor("sigfox-geolocation"),
      capture_location = NA,
      #st_point()
      deploy_on_location = NA,
      # st_point()
      deploy_off_location = NA,
      individual_comments = NA,
      sex = first(sex),
      taxon_canonical_name = first(species),#paste0("Nyctalus ", species),
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
      taxon_ids = first(species), #paste0("Nyctalus ", first(species),
      contact_person_name = NA, # "Edward Hurme",
      main_location = NA #sf::st_point()
    )
  m2 <- mt_set_track_data(m2, track_data)

  # if(!mt_has_no_empty_points(m2)){
  #   m2 <- dplyr::filter(m2, !sf::st_is_empty(m2))
  # }
  # sf::st_is_empty(m2)

  # mt_is_track_id_cleaved(m2)
  m2 <- dplyr::arrange(m2, mt_track_id(m2))

  # Check for tags with more than one location
  ml <- {}
  if(make_lines){
    ml <- m2 %>%
      dplyr::group_by(tag_id) %>%
      dplyr::filter(n() > 1) %>%  #, st_is_empty(geometry)) %>%
      move2::mt_track_lines(
        n = dplyr::n(),
        minTime = min(timestamp),
        maxTime = max(timestamp),
      )
  }

  # Regularize to daily locations
  suppressWarnings(m_day <- regularize_to_daily(data))

  # Plotting
  if (plot_tracks) {
    p <- plot_tracking_data(m2, ml, include_legend, plot_lines = make_lines)
    m <- list(m2, ml, m_day, p)
  } else {
    m <- list(m2, ml, m_day)
  }

  return(m)
}

#' Preprocess move2 Data
#'
#' Ensures move2 data is clean and ready for analysis.
#' @param m2 move2 object.
#' @return Preprocessed move2 object.
mt_preprocess <- function(m2) {
  if (!move2::mt_is_track_id_cleaved(m2)) {
    m2 <- dplyr::arrange(m2, move2::mt_track_id(m2))
  }

  if (!move2::mt_is_time_ordered(m2)) {
    m2 <- dplyr::arrange(m2, move2::mt_track_id(m2), move2::mt_time(m2))
  }

  if (!move2::mt_has_unique_location_time_records(m2)) {
    m2 <- move2::mt_filter_unique(m2)
  }

  if (!move2::mt_has_no_empty_points(m2)) {
    # m2 <- dplyr::filter(m2, !sf::st_is_empty(m2))
    # m2[sf::st_is_empty(m2),]$geometry
    m2 <- m2[!sf::st_is_empty(m2),]
  }

  return(m2)
}

#' Plot Tracking Data
#'
#' Creates a plot of tracking data using ggplot2 and rnaturalearth.
#' @param m2 move2 object with tracking data.
#' @param ml Lines representing movements of tags.
#' @param legend Logical; whether to include a legend.
#' @return A ggplot object.
plot_tracking_data <- function(m2, ml, legend, plot_lines = TRUE) {
  extent <- m2$geometry %>% sf::st_bbox()

  first_points <- m2 %>%
    group_by(tag_id) %>%
    slice_min(order_by = timestamp, n = 1) %>%
    ungroup()

  last_points <- m2 %>%
    group_by(tag_id) %>%
    slice_max(order_by = timestamp, n = 1) %>%
    ungroup()

  # Base plot with naturalearth background
  p <- ggplot() +
    geom_sf(data = rnaturalearth::ne_countries(returnclass = "sf", scale = 10)) +
    theme_linedraw()

  # Add the full tracks
  p <- p + geom_sf(data = m2, aes(color = tag_id))

  if(plot_lines){
    # If ml is another data layer for lines between points:
    p <- p + geom_sf(data = ml, aes(color = tag_id))
  }

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
  #   geom_sf(data = m2, aes(color = tag_id)) +
  #   geom_sf(data = ml, aes(color = tag_id)) +
  #   coord_sf(xlim = c(extent$xmin, extent$xmax), ylim = c(extent$ymin, extent$ymax))
  p
  if (!legend) {
    p <- p + theme(legend.position = "none")
  }

  return(p)
}

