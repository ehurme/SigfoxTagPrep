# Common noctule foraging in Brittany
load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")

# download brittany deployments
d_brittany <- deployments[deployments$latitude > 45 & deployments$longitude < 10,]
brittany <- summer23[summer23$species == 'Nyctalus noctula' & summer23$latitude > 45 & summer23$longitude < 10,]
b_plots <- gg_sigfox_map(data = brittany, facet_location = FALSE,
                         save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/brittany_")

b_sum <- brittany %>% reframe(duration = max(datetime) - capture_datetime,
                     n = n(),
                     #total_vedba = max('Total VeDBA') - min('Total VeDBA'),
  .by = c("Device", "bat_weight", "FA_length")
)
b_sum$duration %>% as.numeric %>%  summary()
write.csv(brittany, file = "../../../Dropbox/MPI/Noctule/Data/rdata/brittany23.csv", row.names = FALSE)

source("../NoctuleMigration/scr/dailydistance.R")

# Run the function on the data
brittany$tag_local_identifier <- brittany$Device
brittany$timestamp <- brittany$datetime
brittany$behavioural_classification <- brittany$`Total VeDBA`
brittany$location_long <- brittany$longitude
brittany$location_lat <- brittany$latitude
df <- brittany
n_brittany <- determine_bursts(brittany)
n_brittany <- diff_vedba(n_brittany)
n_brittany <- diff_dist(n_brittany)

# Add sunrise and sunset to the data frame
n_brittany <- determine_day_night(n_brittany)
n_brittany$sunrise <- as.POSIXct(n_brittany$sunrise, origin="1970-01-01")
n_brittany$sunset <- as.POSIXct(n_brittany$sunset, origin="1970-01-01")
n_brittany$noon <- as.POSIXct(n_brittany$noon, origin="1970-01-01")

n_brittany$day_night %>% table

n_brittany$time_to_noon <- difftime(n_brittany$timestamp, n_brittany$noon, units = "hours")
hist(n_brittany$time_to_noon |> as.numeric(), breaks=20)
n_brittany[which(n_brittany$time_to_noon > 12),c("timestamp", "sunset", "sunrise", "noon")]

# Show the first few rows of the updated data frame
head(n_brittany)

n_brittany$vpm <- n_brittany$diff_vedba/n_brittany$diff_time
n_brittany$hour <- hour(n_brittany$timestamp)+minute(n_brittany$timestamp)/60
n_brittany$doy <- yday(n_brittany$timestamp)
n_brittany$radius <- sapply(strsplit(n_brittany$`Radius (m) (Source/Status)`, split = " "), "[", 1) %>% as.numeric
n_brittany$`Link Quality`
plot(n_brittany$`Link Quality`, n_brittany$radius)

table(n_brittany$Device) %>% median
ggplot(n_brittany, aes(timestamp, hour(timestamp), col = Device))+geom_point()+theme_minimal()

ggplot(n_brittany, aes(timestamp, `Seq. Number`, col = Device))+geom_point()+theme_minimal()
