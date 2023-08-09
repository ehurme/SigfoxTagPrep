# function to download tracks and make some basic plots

sigfox_download <- function(ID = NULL, # PIT-tag
                            ring = NULL,
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
                            roost = NULL,
                            plot_map = TRUE,
                            plot_vedba = TRUE,
                            plot_activity = TRUE,
                            buffer = 1,
                            country_map = TRUE,
                            facet_location = TRUE,
                            save_maps = TRUE,
                            save_path = ""){
  require(pacman)
  p_load(tidyverse, data.table, # utilities
         ggplot2, ggpubr, # plot
         rvest, # scrape html
         stringr, # clean strings
         sf, raster, rnaturalearth, rnaturalearthdata, maps,# plot maps
         update = FALSE)

  if(!exists("countries")){
    countries <- ne_countries(scale = 10)
  }
  world_map <- map_data("world")
  keep_countries <- c("Spain", "Portugal", "France",
                      "Belgium", "Netherlands", "Denmark",
                      "Germany","Switzerland", "Italy",
                      "Liechtenstein", "Slovenia",
                      "Slovakia", "Hungary",
                      "Croatia", "Bosnia and Herzegovina",
                      "Serbia", "Kosovo", "Montenegro",
                      "North Montenegro", "Albania",
                      "North Macedonia",
                      "Austria", "Czech Republic", "Poland",
                      "Luxembourg"
  )

  data <- data.frame(ID, # PIT-tag
             ring,
             tag_ID, # biologger ID
             attachment_type, # glue or collar
             capture_weight, # weight in grams
             capture_time, # date time
             FA_length, # length in mm
             tag_weight, # weight in grams
             sex,
             age,
             repro_status,
             species,
             release_time,
             latitude,
             longitude,
             roost)

  # remove leading 0s from tag IDs
  data$tag_ID <- str_remove(data$tag_ID, "^0+")
  bats <- unique(data$tag_ID)
  # make sure tag names are 7 characters
  bats <- bats[nchar(bats) == 7] %>% na.omit()

  # create a data table to save the Sigfox tag data
  df <- data.table()
  i = 1
  # download data
  for(i in 1:length(bats)){
    print(paste0("bat ", bats[i], ": ", i, " out of ", length(bats)))
    url <-  paste0("https://mpiab.4lima.de/batt.php?id=", bats[i])

    try({
      d <- url %>%
        read_html() %>%
        html_nodes("table") %>%
        html_table(fill = T)
    })

    df <- rbind(df, d[[2]])
  }

  # clean and format data
  n <- unique(df)
  n <- n[n$Device != "",]
  tags <- n$Device %>% unique
  tags[order(tags)]

  n$latitude <- sapply(n$Position %>% strsplit(","), "[", 1) %>% as.numeric
  n$longitude <- sapply(n$Position %>% strsplit(","), "[", 2) %>% as.numeric
  n$datetime <- dmy_hms(n$`Time (Paris)`, tz = "Europe/Berlin")
  n$vedba <- n$`Total VeDBA`

  # add deployment info
  data$capture_time
  # filter data by deployments
  sp23 <- data.table()
  i = 1
  for(i in 1:nrow(data)){
    idx <- which(n$Device == data$tag_ID[i])
    temp <- n[idx,]
    temp <- temp[temp$datetime > data$capture_time[i],]
    temp$pit_tag <- data$ID[i]
    temp$ring <- data$ring[i]
    temp$attachment_type <- data$attachment_type[i]
    temp$capture_datetime <- data$capture_time[i]
    temp$tag_weight <- data$tag_weight[i]
    temp$bat_weight <- data$capture_weight[i]
    temp$FA_length <- data$FA_length[i]
    temp$sex <- data$sex[i]
    temp$age <- data$age[i]
    temp$repro_status <- data$repro_status[i]
    temp$species <- data$species[i]
    temp$capture_latitude <- data$latitude[i]
    temp$capture_longitude <- data$longitude[i]
    temp$roost <- data$roost[i]

    sp23 <- rbind(sp23, temp)
  }

  if(plot_map == TRUE){
    # with(sp23, plot(longitude, latitude, col = factor(Device),
    #                 pch = factor(Device) %>% as.numeric, asp = 1,
    #                 xlab = "Longitude", ylab = "Latitude"))
    # lines(countries)

    map <-
      ggplot() +
      geom_polygon(data = subset(world_map, region %in% keep_countries),
                   aes(x = long, y = lat, group = group), fill = "lightgrey", color = "gray") +
      xlab("Longitude")+ylab("Latitude")+
      coord_equal()+
      # geom_path(data = sp23, aes(longitude, latitude), col = "black")+
      geom_point(data = sp23,
                 aes(longitude, latitude, col = Device))+ #, shape = sex, size = age))+
      coord_map(projection="mercator",
                xlim=c(min(sp23$longitude, na.rm = TRUE)-buffer,
                       max(sp23$longitude, na.rm = TRUE)+buffer),
                ylim=c(min(sp23$latitude, na.rm = TRUE)-buffer,
                       max(sp23$latitude, na.rm = TRUE)+buffer))+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom")
    print(map)
  }

  if(plot_vedba == TRUE){
    all_vedba <- ggplot(sp23, aes(x = datetime, y = vedba, col = Device))+
      geom_path()+
      geom_point()+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom")
    if(facet_location == TRUE){
      all_vedba <- all_vedba + facet_wrap(~Operator)
    }

  }

  if(plot_activity == TRUE){
    all_activity <- ggplot(sp23, aes(x = datetime, y = sp23$`24h Active (%)`,
                                     col = Device))+
      geom_path()+
      geom_point()+ ylab("24h Activity (%)")+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom")
    if(facet_location == TRUE){
      all_activity <- all_activity + facet_wrap(~Operator)
    }
  }

  if(save_maps == TRUE){
    if(plot_activity == FALSE & plot_vedba == TRUE & plot_map == TRUE){
      ggarrange(map, all_vedba, common.legend = TRUE)
      ggsave(filename = paste0(save_path, "map_vedba.png"))
    }
    if(plot_activity == TRUE & plot_vedba == TRUE & plot_map == TRUE){
      ggarrange(ggarrange(map, legend = "right"), ggarrange(all_vedba, all_activity, legend = "none"),
                nrow = 2)
      ggsave(filename = paste0(save_path, "map_vedba_activity.png"))
    }
  }
  return(sp23)
}
