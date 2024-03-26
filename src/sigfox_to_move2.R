# format scrapped sigfox to move2
# devtools::install_git('https://gitlab.com/bartk/move2.git')

# data <- summer23[which(summer23$species == "Nyctalus leisleri"),]
# data$timestamp <- data$datetime

sigfox_to_move2 <- function(data, plot = TRUE, legend = FALSE){
  require(pacman)
  p_load(tidyverse, dplyr, mapview, sf,
         move2, janitor, rnaturalearth)
  source("./src/sourceDir.R")
  sourceDir("./src")

  data <- data |> clean_names()

  data$radius <- sapply(strsplit(data$radius_m_source_status, split = " "), "[", 1) %>% as.numeric

  # add day night
  data <- determine_day_night(data)
  data$time_to_noon <- difftime(data$timestamp, data$noon, units = "hours")

  # determine burst
  data <- determine_bursts(data)

  # convert VeDBA to G
  data$total_vedba <- data$total_ve_dba * 3.9/1000 # conversion factor

  # speed
  data <- diff_dist(data)
  data$ground_sp <- data$distance/(data$diff_time*60)

  # diff vedba
  data <- diff_vedba(data)
  data$vpm <- data$diff_vedba/data$diff_time
  # hist(data$vpm, breaks = 100)

  data$hour <- hour(data$timestamp)+minute(data$timestamp)/60
  data$doy <- yday(data$timestamp)

  motionless_tag <- 280800/(60*24) * 3.9/1000
  data <- tag_fell_off(data, vedba_threshold = motionless_tag*2)
  # table(data$tag_fell_off)

  m2 <- move2::mt_as_move2(x = data[data$tag_fell_off == FALSE,],#[which(!is.na(summer23$latitude)),],
                           coords = c("longitude", "latitude"),
                           time_column = "timestamp",
                           na.fail = FALSE,
                           track_id_column = "tag_id") |> sf::st_set_crs(4326L)

  if(!mt_is_track_id_cleaved(m2)){
    m2 <- dplyr::arrange(m2, mt_track_id(m2))
  }

  if(!mt_is_time_ordered(m2)){
    m2 <- dplyr::arrange(m2, mt_track_id(m2), mt_time(m2))
  }

  if(!mt_has_unique_location_time_records(m2)){
    m2 <- mt_filter_unique(m2)
  }

  if(!mt_has_no_empty_points(m2)){
    m2 <- dplyr::filter(m2, !sf::st_is_empty(m2))
  }

  # check that tags have more than one location
  ml <- m2 %>%
    group_by(tag_id) %>%
    filter(n() > 1) %>%
    mt_track_lines()

  # regularize to daily locations
  # choose locations closest to noon
  m_day <- regularize_to_daily(data[data$tag_fell_off == FALSE,])
  # plot(m_day$daily_distance, m_day$daily_vedba)

  m <- list(m2, ml, m_day)

  if(plot == TRUE){
    extent <- m2$geometry %>% st_bbox()

    p <- ggplot() +
      geom_sf(data = ne_coastline(returnclass = "sf", 10)) +
      theme_linedraw() +
      geom_sf(data = m2, aes(col = tag_id)) +
      geom_sf(data = ml, aes(col = tag_id)) +
      coord_sf(
        xlim = c(extent[1], extent[3]),
        ylim = c(extent[2], extent[4])
        # crs = sf::st_crs("+proj=aeqd +lon_0=51 +lat_0=17 +units=km"),
        # xlim = c(-5000, -2000),
        # ylim = c(3000, 5000)
      )
    if(legend == FALSE){
      p <- p + theme(legend.position = "none")
    }
    print(p)
    m <- list(m2, ml, m_day, p)
  }

  return(m)
}




