# download tracks
## deployments must be a data frame with ID, deployment day,
deployments <- readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/July 2023 capture data.xlsx", sheet = 1)

ID <- deployments$bat.id
tag_ID <- deployments$transmitter
attachment_type <- deployments$`tx attachment`

sigfox_download <- function(ID = NULL, # PIT-tag/ring
                            tag_ID = NULL, # biologger ID
                            attachment_type = NULL, # glue or collar
                            capture_weight = NULL, # weight in grams
                            capture_time = NULL, # date time
                            FA_length = NULL, # length in mm
                            tag_weight = NULL, # weight in grams
                            sex = NULL,
                            age = NULL,
                            repro_status = NULL,
                            species = NULL,
                            release_time = NULL,
                            latitude = NULL,
                            longitude = NULL,
                            data = deployments){
  require(pacman)
  p_load(tidyverse, rvest, data.table,
         ggplot2, gganimate, ggpubr,
         stringr,
         sf,  arulesViz, raster,
         rnaturalearth, rnaturalearthdata, update = FALSE)

  if(!exists("countries")){
    countries <- ne_countries(scale = 10)
  }

  if(is.null(ID)){
    ID <- substr(deployments$transmitter, 2, 8)
  }
  if(is.null(tag_ID)){
    tag_ID <- deployments$transmitter
  }

  bats <- unique(tag_ID)
  # remove leading 0s from tag IDs
  bats <- str_remove(tag_ID, "^0+")
  # make sure tag names are 7 characters
  bats <- bats[nchar(bats) == 7]

  # create a data table to save the Sigfox tag data
  df <- data.table()
  i = 1
  # download data
  for(i in 1:length(bats)){
    print(paste0("bat ", bats[i], ": ", i, " out of ", length(bats)))
    url <-  paste0("https://mpiab.4lima.de/batt.php?id=", bats[i])

    d <- url %>%
      read_html() %>%
      html_nodes("table") %>%
      html_table(fill = T)

    df <- rbind(df, d[[2]])
  }

  # clean and format data
  n <- unique(df)
  n <- n[n$Device != "",]
  tags <- n$Device %>% unique
  tags[order(tags)]
  n$latitude <- sapply(n$Position %>% strsplit(","), "[", 1) %>% as.numeric
  n$longitude <- sapply(n$Position %>% strsplit(","), "[", 2) %>% as.numeric
  n$datetime <- dmy_hms(n$`Time (Paris)`)
  n$vedba <- n$`Total VeDBA`

  if(plot == TRUE){
    with(n[n$longitude < 0,], plot(longitude, latitude, col = factor(Device),
                                   pch = factor(Device) %>% as.numeric, asp = 1,
                                   xlab = "Longitude", ylab = "Latitude"))
    lines(countries)
  }



  hist(n$datetime %>% diff() %>% as.numeric()/-60,
       xlim = c(25, 35), xlab = "time diff (min)",
       breaks = 1000000, main = "")

  p <- ggplot(n[n$longitude < 0,],
              aes(longitude, latitude, group = Device))+
    coord_equal()+
    geom_path()+
    geom_point(aes(col = yday(datetime), size = `24h Active (%)`))+
    scale_colour_gradient2(
      low = "red",
      mid = "blue",
      high = "yellow",
      space = "Lab",
      midpoint = 208,
      na.value = "grey50",
      guide = "colourbar",
      aesthetics = "colour"
    )+
    facet_wrap(~Device)
  p

}