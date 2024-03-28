# download leisleri data
library(pacman)
# devtools::install_git('https://gitlab.com/bartk/move2.git')
p_load(tidyverse, lubridate,
       sf, rnaturalearth,
       mapview, mapdeck,
       move2
       )
#source("./src/sigfox_to_move2.R")
load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")
l <- summer23[summer23$species == "Nyctalus leisleri",]
plot(l$longitude, l$latitude, asp = 1, col = factor(l$Device))
l$Device |> table() |> length()

m <- sigfox_to_move2(l)
ggsave(m[[4]], file = "fig/N_leisleri.png")
m2 <- m[[1]]

test <- m2 %>% filter(timestamp > ymd("2023-09-01"))

mapview(m2, zcol = "x24h_active_percent", layer.name = "24hr Activity %", color = heat.colors(8))+
  mapView(m[[2]], layer.name = "Tracks", color = rainbow(10))

m2$vpm %>% hist(breaks = 100)

m_day <- m[[3]]
ggplot(m_day, aes(daily_distance, daily_vedba, col = daily_distance > 30))+
  geom_point()+theme_minimal()

migrated <- m_day$ID[which(m_day$daily_distance > 30)] %>% unique
# what does activity leading up to migration look like
i = 1
for(i in seq_along(migrated)){
  b <- m_day %>% filter(ID == migrated[i])
  ggplot(b, aes(time, daily_vedba))+geom_path()+
    geom_point(aes(size = daily_distance, col = daily_distance > 30))+
    theme_minimal()
  b2 <- m2 %>% filter(tag_id == migrated[i])
  extent <- b2$geometry %>% st_bbox()
  ggplot() +
    geom_sf(data = ne_countries(returnclass = "sf", 10)) +
    theme_linedraw() +
    geom_sf(data = b2, shape = 21, col = 1, aes(fill = factor(burst_id),
                           # size = radius,
                           size = x24h_active_percent)) +
    scale_size_continuous(name = "24hr Activity %")+
    scale_fill_discrete(name = "Burst ID")+
    geom_sf(data = mt_track_lines(b2), col = 1)+
  coord_sf(
    xlim = c(extent[1], extent[3]),
    ylim = c(extent[2], extent[4]))
}


