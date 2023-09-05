# download leisleri data
df <- summer23

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
