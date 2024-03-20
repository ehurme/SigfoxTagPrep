gg_sigfox_map <- function(data, facet_location = TRUE,
                          legend = TRUE, point_size = 1,
                          save_maps = TRUE, buffer = 1,
                          save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/"){
  require(pacman)
  p_load(ggplot2, ggpubr)
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
                      "Luxembourg")
    map <-
      ggplot() +
      geom_polygon(data = subset(world_map, region %in% keep_countries),
                   aes(x = long, y = lat, group = group), fill = "lightgrey", color = "gray") +
      xlab("Longitude")+ylab("Latitude")+
      coord_equal()+
      geom_path(data = data,
                aes(longitude, latitude, group = tag_ID), col = 1, alpha = 0.2)+
      geom_point(data = data,
                 aes(longitude, latitude, col = tag_ID), size = point_size)+
      coord_map(projection="mercator",
                xlim=c(min(data$longitude, na.rm = TRUE)-buffer,
                       max(data$longitude, na.rm = TRUE)+buffer),
                ylim=c(min(data$latitude, na.rm = TRUE)-buffer,
                       max(data$latitude, na.rm = TRUE)+buffer))+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom")
    if(legend == FALSE){
      map <- map+theme(legend.position = "none")
    }

    all_vedba <- ggplot(data, aes(x = datetime, y = vedba, col = tag_ID))+
      geom_path()+
      geom_point(size = point_size)+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom",
            axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
    if(facet_location == TRUE){
      all_vedba <- all_vedba + facet_wrap(~Operator)
    }
    if(legend == FALSE){
      all_vedba <- all_vedba + theme(legend.position = "none")
    }

    all_activity <- ggplot(data, aes(x = datetime, y = data$`24h Active (%)`,
                                     col = tag_ID))+
      geom_path()+
      geom_point(size = point_size)+ ylab("24h Activity (%)")+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom",
            axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))

    all_temp <- ggplot(data, aes(x = datetime, y = `24h Max. Temperature (°C)`,
                                     col = tag_ID))+
      geom_path()+
      geom_point(size = point_size)+ ylab("Temperature")+
      geom_path(aes(x = datetime, y = `24h Min. Temperature (°C)`, col = tag_ID), lty = 2)+
      geom_point(aes(x = datetime, y = `24h Min. Temperature (°C)`, col = tag_ID), size = point_size)+
      theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
            legend.position = "bottom",
            axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
    all_temp+theme_bw()
    if(facet_location == TRUE){
      all_activity <- all_activity + facet_wrap(~Operator)
    }
    if(legend == FALSE){
      all_activity <- all_activity + theme(legend.position = "none")
    }

      map_vedba <- ggarrange(map, all_vedba, common.legend = TRUE)
      map_vedba_activity <- ggarrange(ggarrange(map, legend = "bottom"),
                                      ggarrange(all_vedba, all_activity, legend = "none", nrow = 2),
                nrow = 1, widths = c(2,1))
  if(save_maps == TRUE){
      ggsave(map_vedba, filename = paste0(save_path, "map_vedba.png"))
      ggsave(map_vedba_activity, filename = paste0(save_path, "map_vedba_activity.png"))
  }
      return(list(map, all_vedba, all_activity, all_temp))
}
