# download spring 2022 and 2023 Nyctalus noctula migration data

## Function to extract latitude and longitude from a string
extract_lat_long <- function(str) {
  parts <- unlist(strsplit(str, ", | "))
  lat <- as.numeric(parts[length(parts) - 1])
  long <- as.numeric(parts[length(parts)])
  c(latitude = lat, longitude = long)
}


# 2022
deployments <- readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2022/Nycnoc-capts20220422.xlsx", sheet = 1)
deployments <- deployments[grepl(x = deployments$transmitter, pattern = "tag"),]

# Apply the function to each string
coordinates <- lapply(deployments$Location, extract_lat_long)

# Convert the result to a data frame
coordinates_df <- data.frame(do.call(rbind, coordinates))

n22 <- sigfox_download(ID = deployments$bat.id,
                       ring = NA,
                       tag_ID = deployments$transmitter,
                       attachment_type = deployments$`tx attachment`,
                       capture_weight = deployments$`mass of bat`,
                       capture_time = strptime(paste0(deployments$Date, " ",
                                                      ifelse(deployments$time == "night",
                                                             "20:00:00", "12:00:00")),
                                               format = "%Y-%m-%d %H:%M:%S"),
                       FA_length = deployments$FA.left, # length in mm
                       tag_weight = deployments$`tx mass`, # weight in grams
                       sex = deployments$sex,
                       age = deployments$age,
                       repro_status = deployments$`repro state`,
                       species = "Nyctalus noctula",
                       release_time = strptime(paste0(deployments$Date, " ",
                                                      ifelse(deployments$time == "night",
                                                             "22:00:00", "14:00:00")),
                                               format = "%Y-%m-%d %H:%M:%S"),
                       latitude = coordinates_df$latitude,
                       longitude = coordinates_df$longitude,
                       roost = deployments$roost)
write.csv(n22[which(!is.na(n22$longitude)),], file = "../../../Dropbox/MPI/Noctule/Data/TinyFoxBattNoctules/Common_Noctule_migration_Spring22.csv", row.names = FALSE)


write.csv(n22[which(is.na(n22$longitude)),], file = "../../../Dropbox/MPI/Noctule/Data/TinyFoxBattNoctules/Common_Noctule_missing_locations_migration_Spring22.csv", row.names = FALSE)


with(n22[n22$sex == "M",], plot(longitude, latitude, asp = 1, col = factor(Device)))
lines(countries)


# 2023
deployments <- readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/20230524_TinyFoxBatt Nyctalus noctula 2023.xlsx", sheet = 1)

# Function to extract latitude and longitude from a string
extract_lat_long <- function(str) {
  parts <- unlist(strsplit(str, ", | "))
  lat <- as.numeric(parts[length(parts) - 1])
  long <- as.numeric(parts[length(parts)])
  c(latitude = lat, longitude = long)
}

# Apply the function to each string
coordinates <- lapply(deployments$Location, extract_lat_long)

# Convert the result to a data frame
coordinates_df <- data.frame(do.call(rbind, coordinates))

n23 <- sigfox_download(ID = deployments$bat.id,
                ring = NA,
                tag_ID = deployments$transmitter,
                attachment_type = deployments$`tx attachment`,
                capture_weight = deployments$`mass of bat`,
                capture_time = strptime(paste0(deployments$Date, " ",
                                               ifelse(deployments$time == "night",
                                                      "20:00:00", "12:00:00")),
                                        format = "%Y-%m-%d %H:%M:%S"),
                FA_length = deployments$FA.left, # length in mm
                tag_weight = deployments$`tx mass`, # weight in grams
                sex = deployments$sex,
                age = deployments$age,
                repro_status = deployments$`repro state`,
                species = "Nyctalus noctula",
                release_time = strptime(paste0(deployments$Date, " ",
                                               ifelse(deployments$time == "night",
                                                      "22:00:00", "14:00:00")),
                                        format = "%Y-%m-%d %H:%M:%S"),
                latitude = coordinates_df$latitude,
                longitude = coordinates_df$longitude,
                roost = deployments$roost)
write.csv(n23[which(!is.na(n23$longitude)),], file = "../../../Dropbox/MPI/Noctule/Data/TinyFoxBattNoctules/Common_Noctule_migration_Spring23.csv", row.names = FALSE)


write.csv(n23[which(is.na(n23$longitude)),], file = "../../../Dropbox/MPI/Noctule/Data/TinyFoxBattNoctules/Common_Noctule_missing_locations_migration_Spring23.csv", row.names = FALSE)


with(n23[n23$sex == "M",], plot(longitude, latitude, asp = 1, col = factor(Device)))
lines(countries)
