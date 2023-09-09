source("./src/sigfox_download.R")
source("./src/gg_sigfox_map.R")

require(pacman)
p_load(tidyverse, data.table, # utilities
       ggplot2, ggpubr, # plot
       rvest, # scrape html
       stringr, # clean strings
       sf, raster, rnaturalearth, rnaturalearthdata, maps,# plot maps
       update = FALSE)


new_bats <- c("120F1B4", "120F576")
n <- sigfox_download(tag_ID = new_bats)
p <- gg_sigfox_map(n)
