# format scrapped sigfox to move2
# devtools::install_git('https://gitlab.com/bartk/move2.git')

data <- summer23[which(summer23$species == "Nyctalus leisleri"),]

sigfox_to_move2 <- function(data, plot = TRUE, legend = FALSE){
  require(pacman)
  p_load(tidyverse, dplyr, mapview, sf,
         move2, janitor, rnaturalearth)
  data <- data |> clean_names()

  m2 <- move2::mt_as_move2(x = data,#[which(!is.na(summer23$latitude)),],
                           coords = c("longitude", "latitude"),
                           time_column = "datetime",
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
  }

  m <- list(m2, ml)
  return(m)
}




