source("./src/sigfox_download.R")
source("./src/gg_sigfox_map.R")

require(pacman)
p_load(tidyverse, data.table, # utilities
       ggplot2, ggpubr, # plot
       rvest, # scrape html
       stringr, # clean strings
       sf, raster, rnaturalearth, rnaturalearthdata, maps,# plot maps
       update = FALSE)

weird_bats <- c("12102E3", "120E836")
w <- sigfox_download(tag_ID = weird_bats)
wp <- gg_sigfox_map(w,facet_location = FALSE,
                   save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/weird_belgium_")
w$Device %>% table()

new_bats <- c("120F1B4", "120F576", "120D38F")
n <- sigfox_download(tag_ID = new_bats)
n$Device %>% table()
p <- gg_sigfox_map(n,facet_location = FALSE,
                   save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/new_belgium_")
d_belgium <- deployments[which(deployments$species == "leisleri"),]
o <- sigfox_download(tag_ID = d_belgium$`tag ID`)
d_belgium$`tag ID` %>% unique
o$Device %>% unique


full <- rbind(n,o)
b <- gg_sigfox_map(full[which(full$longitude < 7),],
  #full[which(full$longitude < 7 & full$datetime > Sys.Date()-7 & full$`24h Active (%)` > 0),],#[full$datetime > ymd("2023-09-05"),],
                   facet_location = FALSE,
                   save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/full_belgium_")
b[[1]]
b[[2]]
b[[3]]

save(full, file = "../../../Dropbox/MPI/Noctule/Data/rdata/leislers23.robj")
load("../../../Dropbox/MPI/Noctule/Data/rdata/leislers23.robj")

deployments$`mass of bat`[deployments$species == "leisleri"] |> summary()
deployments[which(deployments$species == "leisleri"),] |> View()
