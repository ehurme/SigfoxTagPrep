# function to download tracks

sigfox_download <- function(ID = NULL, # PIT-tag
                            ring = NULL,
                            tag_ID = NULL, # biologger ID
                            attachment_type = NULL, # glue or collar
                            capture_weight = NULL, # weight in grams
                            capture_time = NULL, # date time
                            FA_length = NULL, # length in mm
                            tag_weight = NULL, # weight in grams
                            sex = NULL,
                            age = NULL,
                            repro_status = NULL,
                            species = NULL,
                            release_time = NULL,
                            latitude = NULL,
                            longitude = NULL,
                            roost = NULL){
  require(pacman)
  p_load(tidyverse, data.table, # utilities
         rvest, # scrape html
         stringr, # clean strings
         update = FALSE)

  data <- data.frame(ID, # PIT-tag
             ring,
             tag_ID, # biologger ID
             attachment_type, # glue or collar
             capture_weight, # weight in grams
             capture_time, # date time
             FA_length, # length in mm
             tag_weight, # weight in grams
             sex,
             age,
             repro_status,
             species,
             release_time,
             latitude,
             longitude,
             roost)

  # remove leading 0s from tag IDs
  data$tag_ID <- str_remove(data$tag_ID, "^0+")
  bats <- unique(data$tag_ID)
  # make sure tag names are 7 characters
  bats <- bats[nchar(bats) == 7] %>% na.omit()

  # create a data table to save the Sigfox tag data
  df <- data.table()
  i = 1
  # download data
  for(i in 1:length(bats)){
    print(paste0("bat ", bats[i], ": ", i, " out of ", length(bats)))
    url <-  paste0("https://mpiab.4lima.de/batt.php?id=", bats[i])

    try({
      d <- url %>%
        read_html() %>%
        html_nodes("table") %>%
        html_table(fill = T)
    })

    df <- rbind(df, d[[2]])
  }

  # clean and format data
  n <- unique(df)
  n <- n[n$Device != "",]
  tags <- n$Device %>% unique
  tags[order(tags)]

  n$latitude <- sapply(n$Position %>% strsplit(","), "[", 1) %>% as.numeric
  n$longitude <- sapply(n$Position %>% strsplit(","), "[", 2) %>% as.numeric
  n$datetime <- dmy_hms(n$`Time (Paris)`, tz = "Europe/Berlin")
  n$vedba <- n$`Total VeDBA`

  # add deployment info
  data$capture_time
  # filter data by deployments
  sp23 <- data.table()
  i = 2
  for(i in 1:nrow(data)){
    idx <- which(n$Device == data$tag_ID[i])
    if(length(idx) > 0){
      temp <- n[idx,]
      if(!is.na(data$capture_time[i])){
        temp <- temp[temp$datetime > data$capture_time[i],]
      }
      temp$pit_tag <- data$ID[i]
      temp$ring <- data$ring[i]
      temp$attachment_type <- data$attachment_type[i]
      temp$capture_datetime <- data$capture_time[i]
      temp$tag_weight <- data$tag_weight[i]
      temp$bat_weight <- data$capture_weight[i]
      temp$FA_length <- data$FA_length[i]
      temp$sex <- data$sex[i]
      temp$age <- data$age[i]
      temp$repro_status <- data$repro_status[i]
      temp$species <- data$species[i]
      temp$capture_latitude <- data$latitude[i]
      temp$capture_longitude <- data$longitude[i]
      temp$roost <- data$roost[i]

      sp23 <- rbind(sp23, temp)
    }
  }

  return(sp23)
}
