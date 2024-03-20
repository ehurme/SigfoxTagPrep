library(data.table)
library(rnaturalearth)
library(ggplot2)
library(janitor)
s <- fread("./../../../Downloads/export-device-943266-messages (2).csv")
s <- fread("./../../../Downloads/export-device-943266-messages (2) (1).csv") |> clean_names()
s |> summary()
# 47.66, 9.172
# 47.661236, 9.174303
plot(s$longitude_computed_location, s$latitude_computed_location, asp = 1,
     xlim = c(8.5, 9.9), ylim = c(47.5, 47.8))
# lines(countries)

ggplot(s, aes(longitude_computed_location, latitude_computed_location,
              col = timestamp))+
  geom_path()+
  geom_point(alpha = 0.2) #+
  # geom_hline(yintercept = 47.661, lty = 2)+
  # geom_vline(xintercept = 9.174, lty = 2)

library(leaflet)

m <- leaflet() %>%
  addTiles() %>%  # Add default OpenStreetMap map tiles
  addMarkers(lng=9.174, lat=47.661, popup="The birthplace of R")
m

s$radius_computed_location/11100
leaflet(s) %>% addCircles(lng = ~longitude_computed_location,
                          lat = ~latitude_computed_location,
                          radius = ~radius_computed_location/111,
                          popup = ~as.character(timestamp)) |>
  addTiles()
