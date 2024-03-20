# download and plot summer 23 data
source("./src/sigfox_download.R")
source("./src/gg_sigfox_map.R")

require(pacman)
p_load(tidyverse, data.table, # utilities
       ggplot2, ggpubr, # plot
       rvest, # scrape html
       stringr, # clean strings
       sf, raster, geosphere,
       rnaturalearth, rnaturalearthdata, maps,# plot maps
       update = FALSE)

load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")

names(summer23)
library(dplyr)
library(geosphere)

summer23 <-
  summer23 %>%
  # mutate(datetime = as.POSIXct(`Time (Paris)`, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/Paris")) %>%
  arrange(tag_ID, datetime) %>% # Ensure data is sorted
  group_by(tag_ID, species, capture_latitude, capture_longitude) %>%
  mutate(
    # Calculate distance and bearing
    distance = ifelse(row_number() == n(), NA,
                      distHaversine(cbind(longitude, latitude),
                                    cbind(lead(longitude), lead(latitude)))),
    bearing = ifelse(row_number() == n(), NA,
                     bearing(cbind(longitude, latitude),
                                   cbind(lead(longitude), lead(latitude)))),
    # Calculate vpm
    n = n(),
    dt = difftime(lead(datetime), datetime, units = "mins"),
    diff_vedba = ifelse(row_number() == n(), NA,
                 (`Total VeDBA` - lag(`Total VeDBA`)))
  ) %>%
  ungroup()

summer23$vpm <- summer23$diff_vedba/as.numeric(summer23$dt)
summer23$ground_sp <- summer23$distance/(as.numeric(summer23$dt)*60)
# View the modified data frame
summary(summer23)
range(summer23$dt, na.rm = TRUE)
ggplot(summer23[which(summer23$vpm > 0 & summer23$dt > 20 & summer23$dt < 100 &
                        (hour(summer23$datetime) > 18 | hour(summer23$datetime) < 7) ),],
       aes(ground_sp, vpm/1000, col = hour(datetime)))+
  geom_point()+facet_wrap(~species)+ylab("Summed VeDBA per sample (g)") + xlab("ground sp (m/s)")+
  scale_color_viridis_c()+ylim(c(0, 250))


