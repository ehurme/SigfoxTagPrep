library(pacman)
p_load(move, janitor, data.table, lubridate, tidyverse, moveVis, basemaps)
source("./../../../Desktop/movebank_login.R")
source("src/df_to_movestack.R")

n <- df_to_movestack(belgium)
use_multicore(n_cores = 20)

# align move_data to a uniform time scale
m <- {}

# remove individuals with fewer than 2 points
bats <- namesIndiv(n)
temp <- n@trackId %>% table
keep <- names(temp[temp > 1])

m <- align_move(n[n@trackId %in% keep,], res = 12, unit = "hours")
plot(m)

# create spatial frames with a OpenStreetMap watercolour map
frames <- {}
frames <- frames_spatial(m,  # path_colours = c("red", "green", "blue"),
                         map_token = token,
                         map_service = "mapbox",
                         map_type = "satellite", alpha = 1) %>%
  # add_labels(x = "Longitude", y = "Latitude") %>% # add some customizations, such as axis labels
  # add_northarrow() %>%
  add_gg(expr(list(theme(legend.position = "none")))) %>%
    #guides(colour = guide_legend(ncol = 2))))) %>%
  add_scalebar(position = "bottomright", colour = "white") %>%
  add_timestamps(m, type = "label") %>%
  add_progress()

frames[[length(frames)]] # preview one of the frames, e.g. the 100th frame

# animate frames
try(animate_frames(frames,fps = 4, overwrite = TRUE, end_pause = 6,
                   out_file = "../../../Dropbox/MPI/Noctule/Plots/belgium_sat_12hrs_4fps.mp4"))

