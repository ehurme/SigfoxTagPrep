# download and plot summer 23 data
source("./src/sigfox_download.R")
source("./src/gg_sigfox_map.R")

require(pacman)
p_load(tidyverse, data.table, # utilities
       dplyr,
       ggplot2, ggpubr, # plot
       rvest, # scrape html
       stringr, # clean strings
       sf, raster, rnaturalearth, rnaturalearthdata, maps,# plot maps
       update = FALSE)

## deployments must be a data frame with ID, deployment day,
deployments = readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/July, August 2023 TinyFoxBatt deployments_EH.xlsx", sheet = 1)
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
        roost = deployments$roost)
summer23 <- summer23[summer23$Device != "120D1F8",]

all_plots <- gg_sigfox_map(data = summer23, facet_location = TRUE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/all_")
all_plots[[1]]+theme(legend.position = "none")

d_belgium <- deployments[which(deployments$species == "leisleri"),]
d_belgium$`tag ID` %>% unique
belgium <- summer23[summer23$species == 'Nyctalus leisleri',]
belgium$Device %>% unique
b_plots <- gg_sigfox_map(data = belgium, facet_location = FALSE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/belgium_")
b_plots <- gg_sigfox_map(data = belgium[belgium$datetime > ymd("2023-08-30"),], facet_location = FALSE,
              save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/belgium_recent_")
plot(date(belgium$datetime), belgium$datetime %>% hour)
ggplot(belgium, aes(date(datetime), hour(datetime), col = Device))+
  geom_point()+facet_wrap(~Device)+ylab("hour")+xlab("date")




str_count(belgium$`Base Stations (ID, RSSI, Reps)`, ",")/2


d_spain <- deployments[deployments$latitude < 40,]
spain <- summer23[summer23$species == 'Nyctalus lasiopterus' & summer23$latitude < 40,]
s_plots <- gg_sigfox_map(data = spain[spain$datetime > ymd("2023-08-30"),], facet_location = FALSE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/spain_")
str_count(spain$`Base Stations (ID, RSSI, Reps)`, ",")/2

# download brittany deployments
d_brittany <- deployments[deployments$latitude > 45 & deployments$longitude < 10,]
brittany <- summer23[summer23$species == 'Nyctalus noctula' & summer23$latitude > 45 & summer23$longitude < 10,]
b_plots <- gg_sigfox_map(data = brittany, facet_location = FALSE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/brittany_")

d_toulouse <- deployments[which(deployments$species == "lasiopterus" & deployments$latitude > 40),]
toulouse <- summer23[which(summer23$species == "Nyctalus lasiopterus" & summer23$latitude > 40),]
t_plots <- gg_sigfox_map(data = toulouse, facet_location = FALSE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/toulouse_")

d_poland <- deployments[deployments$latitude > 50,]



save(deployments, summer23, brittany, toulouse, spain, file = "../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")
load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")

with(belgium[belgium$Device == "120CF91",], plot(datetime, `24h Active (%)`, type = "l"))

summer23 |> reframe(country = last(Operator),
                    species = first(species),
                    .by = Device) |> View()
