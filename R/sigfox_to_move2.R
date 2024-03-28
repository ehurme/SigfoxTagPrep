#' Convert Sigfox Tracking Data to move2 Format
#'
#' This function processes Sigfox tracking data, prepares it for analysis,
#' and optionally plots it using move2 and related geospatial packages.
#'
#' @param data A data frame containing tracking data.
#' @param plot Logical; if TRUE, generates a plot of the tracking data.
#' @param legend Logical; controls the presence of a legend in the plot.
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
sigfox_to_move2 <- function(data, plot = TRUE, legend = FALSE) {
  # Load required libraries dynamically
  pacman::p_load(tidyverse, dplyr, mapview, sf, move2, janitor, rnaturalearth, update = FALSE)

  # Clean column names for consistency
  data <- data |> janitor::clean_names()

  # Ensure timestamp column exists
  if (is.null(data$timestamp)) {
    data$timestamp <- data$datetime
  }

  # Process radius from source status
  data$radius <- sapply(strsplit(data$radius_m_source_status, split = " "), "[", 1) %>% as.numeric

  # Additional data processing steps
  data <- determine_day_night(data)
  data$time_to_noon <- difftime(data$timestamp, data$noon, units = "hours")
  data <- determine_bursts(data)
  data$total_vedba <- data$total_ve_dba * 3.9 / 1000  # Conversion factor
  data <- diff_dist(data)
  data$ground_sp <- data$distance / (data$diff_time * 60)
  data <- diff_vedba(data)
  data$vpm <- data$diff_vedba / data$diff_time
  data$hour <- lubridate::hour(data$timestamp) + lubridate::minute(data$timestamp) / 60
  data$doy <- lubridate::yday(data$timestamp)

  # Check for motionless tags and determine if a tag fell off
  motionless_tag <- 280800 / (60 * 24) * 3.9 / 1000
  data <- tag_fell_off(data, vedba_threshold = motionless_tag * 2)

  # Convert to move2 object
  m2 <- move2::mt_as_move2(
    x = data[data$tag_fell_off == FALSE,],
    coords = c("longitude", "latitude"),
    time_column = "timestamp",
    na.fail = FALSE,
    track_id_column = "tag_id"
  ) |> sf::st_set_crs(4326L)

  # Ensure data is properly ordered and filtered
  m2 <- mt_preprocess(m2)

  # Check for tags with more than one location
  ml <- m2 %>%
    dplyr::group_by(tag_id) %>%
    dplyr::filter(n() > 1) %>%
    move2::mt_track_lines()

  # Regularize to daily locations
  m_day <- regularize_to_daily(data[data$tag_fell_off == FALSE,])

  # Plotting
  if (plot) {
    p <- plot_tracking_data(m2, ml, legend)
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
    m2 <- dplyr::filter(m2, !sf::st_is_empty(m2))
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
plot_tracking_data <- function(m2, ml, legend) {
  extent <- m2$geometry %>% sf::st_bbox()
  p <- ggplot() +
    geom_sf(data = rnaturalearth::ne_coastline(returnclass = "sf")) +
    theme_linedraw() +
    geom_sf(data = m2, aes(color = tag_id)) +
    geom_sf(data = ml, aes(color = tag_id)) +
    coord_sf(xlim = c(extent$xmin, extent$xmax), ylim = c(extent$ymin, extent$ymax))

  if (!legend) {
    p <- p + theme(legend.position = "none")
  }

  return(p)
}