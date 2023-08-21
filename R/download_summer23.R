# download and plot summer 23 data
source("./R/sigfox_download.R")

require(pacman)
p_load(tidyverse, data.table, # utilities
       ggplot2, ggpubr, # plot
       rvest, # scrape html
       stringr, # clean strings
       sf, raster, rnaturalearth, rnaturalearthdata, maps,# plot maps
       update = FALSE)

## deployments must be a data frame with ID, deployment day,
deployments = readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/July, August 2023 TinyFoxBatt deployments.xlsx", sheet = 1)
deployments$latitude = sapply(strsplit(deployments$location, ","), "[", 1) %>% as.numeric()
deployments$longitude = sapply(strsplit(deployments$location, ","), "[", 2) %>% as.numeric()

# View(deployments)
deployments = deployments[deployments$`tag ID` != "0120D1F8",]
deployments$`tag ID` %>% unique

# download all summer deployments
summer23 <- sigfox_download(ID = deployments$`PIT-tag`,
        ring = deployments$`ring #`,
        tag_ID = deployments$`tag ID`,
        attachment_type = deployments$`attachment method`,
        capture_weight = deployments$`mass of bat`,
        capture_time = strptime(paste0(deployments$`attachment date`, " ",
                                       str_sub(deployments$`attachment time`,-8,-1)),
                                format = "%d.%m.%y %H:%M:%S"),
        FA_length = deployments$`FA length`, # length in mm
        tag_weight = deployments$`tag mass`, # weight in grams
        sex = deployments$sex,
        age = deployments$age,
        repro_status = deployments$`repro status`,
        species = paste0(deployments$genus, " ", deployments$species),
        release_time = strptime(paste0(deployments$`attachment date`, " ",
                                       str_sub(deployments$`attachment time`,-8,-1)),
                                format = "%d.%m.%y %H:%M:%S"),
        latitude = sapply(strsplit(deployments$location, ","), "[", 1) %>% as.numeric(),
        longitude = sapply(strsplit(deployments$location, ","), "[", 2) %>% as.numeric(),
        roost = deployments$roost,
        plot_map = TRUE,
        buffer = 1,
        plot_vedba = TRUE,
        plot_activity = FALSE,
        facet_location = TRUE,
        save_maps = TRUE,
        save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/")

# download brittany deployments
d_brittany <- deployments[deployments$latitude > 45 & deployments$longitude < 10,]
brittany23 <- sigfox_download(ID = d_brittany$`PIT-tag`,
                            ring = d_brittany$`ring #`,
                            tag_ID = d_brittany$`tag ID`,
                            attachment_type = d_brittany$`attachment method`,
                            capture_weight = d_brittany$`mass of bat`,
                            capture_time = strptime(paste0(d_brittany$`attachment date`, " ",
                                                           str_sub(d_brittany$`attachment time`,-8,-1)),
                                                    format = "%d.%m.%y %H:%M:%S"),
                            FA_length = d_brittany$`FA length`, # length in mm
                            tag_weight = d_brittany$`tag mass`, # weight in grams
                            sex = d_brittany$sex,
                            age = d_brittany$age,
                            repro_status = d_brittany$`repro status`,
                            species = paste0(d_brittany$genus, " ", d_brittany$species),
                            release_time = strptime(paste0(d_brittany$`attachment date`, " ",
                                                           str_sub(d_brittany$`attachment time`,-8,-1)),
                                                    format = "%d.%m.%y %H:%M:%S"),
                            latitude = d_brittany$latitude,
                            longitude = d_brittany$longitude,
                            roost = d_brittany$roost,
                            plot_map = TRUE,
                            buffer = 1,
                            plot_vedba = TRUE,
                            facet_location = TRUE,
                            save_maps = TRUE,
                            save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/brittany_")

ggplot(brittany23, aes(datetime, `24h Active (%)`, col = Device))+geom_point()

d_spain <- deployments[deployments$latitude < 40,]
spain23 <- sigfox_download(ID = d_spain$`PIT-tag`,
                            ring = d_spain$`ring #`,
                            tag_ID = d_spain$`tag ID`,
                            attachment_type = d_spain$`attachment method`,
                            capture_weight = d_spain$`mass of bat`,
                            capture_time = strptime(paste0(d_spain$`attachment date`, " ",
                                                           str_sub(d_spain$`attachment time`,-8,-1)),
                                                    format = "%d.%m.%y %H:%M:%S"),
                            FA_length = d_spain$`FA length`, # length in mm
                            tag_weight = d_spain$`tag mass`, # weight in grams
                            sex = d_spain$sex,
                            age = d_spain$age,
                            repro_status = d_spain$`repro status`,
                            species = paste0(d_spain$genus, " ", d_spain$species),
                            release_time = strptime(paste0(d_spain$`attachment date`, " ",
                                                           str_sub(d_spain$`attachment time`,-8,-1)),
                                                    format = "%d.%m.%y %H:%M:%S"),
                            latitude = d_spain$latitude,
                            longitude = d_spain$longitude,
                            roost = d_spain$roost,
                            plot_map = TRUE,
                            buffer = 1,
                            plot_vedba = TRUE,
                            facet_location = TRUE,
                            save_maps = TRUE,
                            save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/spain_")

d_poland <- deployments[deployments$latitude > 50,]
poland23 <- sigfox_download(ID = d_poland$`PIT-tag`,
                           ring = d_poland$`ring #`,
                           tag_ID = d_poland$`tag ID`,
                           attachment_type = d_poland$`attachment method`,
                           capture_weight = d_poland$`mass of bat`,
                           capture_time = strptime(paste0(d_poland$`attachment date`, " ",
                                                          str_sub(d_poland$`attachment time`,-8,-1)),
                                                   format = "%d.%m.%y %H:%M:%S"),
                           FA_length = d_poland$`FA length`, # length in mm
                           tag_weight = d_poland$`tag mass`, # weight in grams
                           sex = d_poland$sex,
                           age = d_poland$age,
                           repro_status = d_poland$`repro status`,
                           species = paste0(d_poland$genus, " ", d_poland$species),
                           release_time = strptime(paste0(d_poland$`attachment date`, " ",
                                                          str_sub(d_poland$`attachment time`,-8,-1)),
                                                   format = "%d.%m.%y %H:%M:%S"),
                           latitude = d_poland$latitude,
                           longitude = d_poland$longitude,
                           roost = d_poland$roost,
                           plot_map = TRUE,
                           buffer = 1,
                           plot_vedba = TRUE,
                           facet_location = TRUE,
                           save_maps = TRUE,
                           save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/poland_")


spain23$date <- as.Date(spain23$datetime)
ggplot(spain23, aes(datetime, vedba, group = roost, col = Device))+
  geom_point()+
  facet_wrap(~roost)+theme(legend.position = "bottom")
ggplot(spain23, aes(datetime, activity, group = roost, col = Device))+
  geom_point()+
  facet_wrap(~roost)+theme(legend.position = "bottom")


spain23[spain23$`24h Active (%)` > 0,] %>% reframe(activity = mean(`24h Active (%)`),
                                                   bats = length(unique(Device)),
                                                   .by = c(roost, attachment_type, date)) -> spain_sum
ggplot(spain_sum[spain_sum$activity > 0,], aes(x = date, y = bats, col = attachment_type))+
  geom_point()+
  ylab("Number of tagged bats")+
  ylim(c(0,8))+
  facet_wrap(~roost)+
  theme_bw()+
  guides(col = guide_legend(nrow = 2))+
  theme(legend.position = "bottom", legend.key.size = unit(.2, "mm"))

alive <- spain23$Device[which(spain23$date > (Sys.Date()-3) & spain23$`24h Active (%)` > 0)] %>% unique

ggplot(spain23[spain23$Device %in% alive,], aes(x = date, y = vedba, col = attachment_type))+
  geom_point()

ggplot(spain23[spain23$Device %in% alive,], aes(x = longitude, y = latitude, col = Device))+
  geom_point()

sp23 <- spain23[spain23$Device %in% alive,]
buffer = 1
ggplot() +
  geom_polygon(data = subset(world_map, region %in% keep_countries),
               aes(x = long, y = lat, group = group), fill = "lightgrey", color = "gray") +
  xlab("Longitude")+ylab("Latitude")+
  coord_equal()+
  geom_path(data = sp23, aes(longitude, latitude, group = Device), col = "black")+
  geom_point(data = sp23,
             aes(longitude, latitude, col = Device))+
  coord_map(projection="mercator",
            xlim=c(min(sp23$longitude, na.rm = TRUE)-buffer,
                   max(sp23$longitude, na.rm = TRUE)+buffer),
            ylim=c(min(sp23$latitude, na.rm = TRUE)-buffer,
                   max(sp23$latitude, na.rm = TRUE)+buffer))+
  theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
        legend.position = "bottom")+facet_wrap(~Device)
