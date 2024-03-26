# download leisleri data
library(pacman)
# devtools::install_git('https://gitlab.com/bartk/move2.git')
p_load(tidyverse, lubridate,
       sf, rnaturalearth,
       mapview, mapdeck,
       move2
       )
source("./src/sigfox_to_move2.R")
load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")
l <- summer23[summer23$species == "Nyctalus leisleri",]
plot(l$longitude, l$latitude, asp = 1, col = factor(l$Device))
l$Device |> table() |> length()

m <- sigfox_to_move2(l)
m2 <- m[[1]]

m2 %>% group_by(device) %>%
  mutate(diff_vedba = c(diff(total_ve_dba), NA),
         diff_vedba = c(diff(datetime), NA)) -> m2
m2
m2$diff_vedba %>% hist()

m2 %>%
  filter(!st_is_empty(.)) %>%
  mt_filter_per_interval(unit = "1 day") -> test


mapview(m2, zcol = "diff_vedba", layer.name = "24hr Activity %", color = heat.colors(8))+
  mapView(m[[2]], layer.name = "Tracks")
mapview(m[[2]])
?mapview

dat <- mt_read(mt_example());mapView(dat, zcol='eobs:used-time-to-get-fix')+ mapView(mt_track_lines(dat), zcol="individual-local-identifier",color=heat.colors(8), layer.name="Tracks")

mapview(breweries)
breweries |> is()
m[[1]] |> is()
# migrated bats
m <- l[l$latitude < 50,]
m$Device |> table() |> length()



l23 <- df[df$datetime > "2023-08-22" & df$activity > 0 & df$latitude > 45 & df$longitude < 10,]
plot(l23$longitude, l23$latitude, asp = 1, col = factor(l23$Device))
lines(countries)

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
buffer <- 1
map <-
  ggplot() +
  geom_polygon(data = subset(world_map, region %in% keep_countries),
               aes(x = long, y = lat, group = group), fill = "lightgrey", color = "gray") +
  xlab("Longitude")+ylab("Latitude")+
  coord_equal()+
  # geom_path(data = sp23, aes(longitude, latitude), col = "black")+
  geom_point(data = l23,
             aes(longitude, latitude, col = Device))+ #, shape = sex, size = age))+
  coord_map(projection="mercator",
            xlim=c(min(l23$longitude, na.rm = TRUE)-buffer,
                   max(l23$longitude, na.rm = TRUE)+buffer),
            ylim=c(min(l23$latitude, na.rm = TRUE)-buffer,
                   max(l23$latitude, na.rm = TRUE)+buffer))+
  theme(legend.key.size = unit(.1, "cm"), legend.text = element_text(size=6),
        legend.position = "bottom")
print(map)

active <- ggplot(l23, aes(x = datetime, y = `24h Active (%)`, col = Device))+
  geom_point()+geom_path()+
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
active
vedba <- ggplot(l23, aes(x = datetime, y = `Total VeDBA`/1e7, col = Device))+
  geom_point()+geom_path()+
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))

library(ggpubr)
ggarrange(map, ggarrange(vedba, active, legend = "none", nrow = 2),
          common.legend = TRUE, nrow = 1)
