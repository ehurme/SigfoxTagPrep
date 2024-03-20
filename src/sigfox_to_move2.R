# format scrapped sigfox to move2

library(pacman)
p_load(tidyverse, dplyr,
  move2, janitor, rnaturalearthdata)



n <- 5
data <- data.frame(
  x = cumsum(rnorm(n)), y = cumsum(rnorm(n)),
  time = seq(n), track = "a"
)

m2 <- mt_as_move2(data,
            coords = c("x", "y"), time_column = "time",
            track_id_column = "track"
) |> sf::st_set_crs(4326L)
plot(m2$geometry, type = "o")


sigfox_to_move2 <- function(data){}

data <- summer23 |> clean_names()
# data <- data[order(data$datetime),]

s23 <- move2::mt_as_move2(x = data,#[which(!is.na(summer23$latitude)),],
                          coords = c("longitude", "latitude"),
                   time_column = "datetime",
                   na.fail = FALSE,
                   track_id_column = "tag_id") |> sf::st_set_crs(4326L)
s23
plot(s23$geometry, col = factor(s23$device), asp = 1)

mt_track_id(s23) |> table()

s23 <- s23[order(mt_track_id(s23)),]
countries <- rnaturalearth::ne_countries(scale = 10, returnclass = "sf", type = "countries", continent = "Europe")

ggplot(data = countries)+
  geom_sf()+
  geom_sf(data = mt_track_lines(s23)) +
  geom_sf(data = s23, aes(col = species))+
  coord_sf(crs = sf::st_crs("+proj=aeqd +lon_0=51 +lat_0=17 +units=km"),
           xlim = c(-5500, -2000),
           ylim = c(3000, 6000))+
  theme_minimal()

track_attributes = "Radius (m) (Source/Status)",
                                        "Total VeDBA","24h Min. Temperature (°C)",
                                        "24h Max. Temperature (°C)",
                                        "24h Active (%)")
                   track_attributes = c("Radius (m) (Source/Status)",
                                        "Total VeDBA""24h Min. Temperature (°C)",
                                        "24h Max. Temperature (°C)",
                                        "24h Active (%)"))
