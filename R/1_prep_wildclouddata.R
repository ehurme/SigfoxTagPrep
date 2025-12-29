library(tidyverse)
library(lubridate)
library(stringr)
library(purrr)

source("../SigfoxTagPrep/R/wildcloud_nanofox30d_to_movebank.R")  # file containing the functions above

path <- "../../../Dropbox/MPI/Moths/Data/wildcloud/test"

df <- data.table::fread(paste0(path,"/26_11_2025_records.csv"))
names(df)

movebank_df <- {}
movebank_df <- wc_multicsv_to_dpl(path,
                                  raw2physical      = TRUE,
                                  norm_multisamples = TRUE)
summary(movebank_df)
#movebank_df$`min_temp [°C]`[which(!is.na(movebank_df$`min_temp [°C]`))]

# loc <- movebank_df %>% filter(!is.na(`latitude [°]`))
# loc %>% select(`Time start`, `Time end`, timestamp_sf, `latitude [°]`, `longitude [°]`)
# plot(loc$`longitude [°]`, loc$`latitude [°]`)

# v <- movebank_df %>% filter(!is.na(VeDBA))
# v %>% select(`Time start`, `Time end`, timestamp_sf, `VeDBA [m/s2]`)
# v$`VeDBA [m/s2]` %>% hist()
#
# t <- movebank_df %>% filter(!is.na(`temperature [°C]`))
# t %>% select(`Time start`, `Time end`, timestamp_sf, `temperature [°C]`)
# t$`temperature [°C]` %>% hist()
#
# mt <- movebank_df %>% filter(!is.na(`min_temp [°C]`))
# mt %>% select(`Time start`, `Time end`, timestamp_sf, `min_temp [°C]`)
#
# p <- movebank_df %>% filter(!is.na(`pressure [mbar]`))
# p %>% select(`Time start`, `Time end`, timestamp_sf, `pressure [mbar]`)
# p$`pressure [mbar]` %>% hist()

movebank_df <- wc_multicsv_to_dpl(
  path,
  raw2physical      = TRUE,
  norm_multisamples = TRUE
) |>
  dplyr::mutate(
    Device           = as.character(Device),
    timestamp_sf     = lubridate::as_datetime(timestamp_sf),
    `Raw Data`       = as.character(`Raw Data`),
    `Radius (m) (Source/Status)` = as.character(`Radius (m) (Source/Status)`),
    `Sequence Number` = as.numeric(`Sequence Number`),
    LQI              = as.character(LQI),
    `Link Quality`   = as.numeric(`Link Quality`),
    `Operator Name`  = as.character(`Operator Name`),
    `Country Code`   = as.numeric(`Country Code`),
    `Base Stations (ID, RSSI, Reps)` =
      as.character(`Base Stations (ID, RSSI, Reps)`),
    Compression      = as.character(Compression),
    `Time start`     = lubridate::as_datetime(`Time start`),
    `Time end`       = lubridate::as_datetime(`Time end`),
    VeDBA            = as.numeric(VeDBA),
    `VeDBA [m/s2]`   = as.numeric(`VeDBA [m/s2]`),
    `temperature [°C]` = as.numeric(`temperature [°C]`),
    #`min_temp [°C]`    = as.numeric(`min_temp [°C]`),
    `pressure [mbar]`  = as.numeric(`pressure [mbar]`),
    `latitude [°]`     = as.numeric(`latitude [°]`),
    `longitude [°]`    = as.numeric(`longitude [°]`)
  )

dplyr::glimpse(movebank_df)
plot(movebank_df$`longitude [°]`, movebank_df$`latitude [°]`, col = factor(movebank_df$Device))
movebank_df$Device %>% table()

tmp <- movebank_df[which(movebank_df$`latitude [°]` < 0),]
plot(tmp$`longitude [°]`, tmp$`latitude [°]`)
glimpse(tmp)
tmp$Device %>% unique() %>% paste

# remove tags not in the study
not_my_tags <- tmp$Device %>% unique

movebank_clean <- movebank_df[-which(movebank_df$Device %in% not_my_tags),]
plot(movebank_clean$`longitude [°]`, movebank_clean$`latitude [°]`, col = factor(movebank_clean$Device))
ggplot()+
  geom_sf(data = rnaturalearth::countries110)+
  geom_path(data = movebank_clean,
            aes(`longitude [°]`, `latitude [°]`, col = Device))+
  ylim(c(30,60))+
  xlim(c(0,30))+
  theme(legend.position ="none")

write.csv(movebank_clean, file = "../../../Dropbox/MPI/Moths/Data/movebank/movebank_fall_2025.csv")

# check number of repeat timestamps
# table(paste(movebank_clean$Device, movebank_clean$timestamp_sf))[1:10]
