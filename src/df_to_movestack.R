df_to_movestack <- function(data, species = "Nyctalus noctula"){
  data_clean <- data[!is.na(data$latitude),]
  while(any(duplicated(data_clean[,c("Device", "datetime")]))){
    data_clean <- data_clean[which(!duplicated(data_clean[,c("Device", "datetime")])),]
  }

  # make date into a move file
  move_n <- data.frame(timestamp = data_clean$datetime,
                       location_long = data_clean$longitude,
                       location_lat = data_clean$latitude,
                       sensor_type = rep("gps", nrow(data_clean)),
                       individual_taxon_canonical_name = rep(species, nrow(data_clean)),
                       tag_local_identifier = data_clean$Device,
                       individual_local_identifier = data_clean$pit_tag,
                       behavioural_classification = data_clean$vedba,
                       temperature_min = data_clean$`24h Min. Temperature (°C)`,
                       temperature_max = data_clean$`24h Max. Temperature (°C)`,
                       gps_dop = data_clean$`24h Active (%)`
  )
  bats <- unique(move_n$tag_local_identifier)
  datal <- list()
  i = 2
  for(i in 1:length(bats)){
    b <- {}
    b <- as.data.frame(move_n[move_n$tag_local_identifier == bats[i],])
    b <- b[order(b$timestamp),]
    m <- {}
    m <- move::move(x = b$location_long, y = b$location_lat,
                    time = b$timestamp,
                    data = b,
                    proj = "+proj=longlat +datum=WGS84 +no_defs",
                    animal = b$tag_local_identifier)
    datal[[i]] <- m
  }

  datas <- move::moveStack(datal)
  return(datas)
}
