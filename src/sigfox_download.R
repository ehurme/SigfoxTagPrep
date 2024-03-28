# function to download tracks

sigfox_download <- function(ID = NA, # PIT-tag
                            ring = NA,
                            tag_ID = NA, # biologger ID
                            attachment_type = NA, # glue or collar
                            capture_weight = NA, # weight in grams
                            capture_time = NA, # date time
                            FA_length = NA, # length in mm
                            tag_weight = NA, # weight in grams
                            sex = NA,
                            age = NA,
                            repro_status = NA,
                            species = NA,
                            release_time = NA,
                            latitude = NA,
                            longitude = NA,
                            roost = NA){
  require(pacman)
  p_load(tidyverse, data.table, lubridate, # utilities
         rvest, # scrape html
         stringr, # clean strings
         update = FALSE)

  capture_data <- data.frame(ID, # PIT-tag
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
  capture_data$tag_ID <- str_remove(capture_data$tag_ID, "^0+")
  bats <- unique(capture_data$tag_ID)
  # make sure tag names are 7 characters
  bats <- bats[nchar(bats) == 7] %>% na.omit()

  # create a data table to save the Sigfox tag data
  df <- data.table()
  i = 1
  # download data
  for(i in seq_along(bats)) {
    print(paste0("bat ", bats[i], ": ", i, " out of ", length(bats)))
    url <- paste0("https://mpiab.4lima.de/batt.php?id=", bats[i])

    # Initialize 'd' outside the try block
    d <- NULL

    # Retry mechanism
    attempt <- 1
    max_attempts <- 5
    while(is.null(d) && attempt <= max_attempts) {
      try({
        d <- url %>%
          read_html() %>%
          html_nodes("table") %>%
          html_table(fill = TRUE)
        # Assign tag_ID only if 'd' is successfully retrieved
        if (!is.null(d) && length(d) >= 2) {
          d[[2]]$tag_ID <- bats[i]
          df <- rbind(df, d[[2]])
        }
      }, silent = TRUE) # silent=TRUE to suppress warnings/errors in try

      if(is.null(d)) {
        Sys.sleep(2) # Wait for 2 seconds before retrying
        attempt <- attempt + 1
      }
    }

    if(is.null(d)) {
      message(paste("Failed to retrieve data for bat", bats[i], "after", max_attempts, "attempts."))
    }
  }

  # clean and format data
  n <- unique(df)
  n <- n[n$Device != "",]
  n$Device <- as.character(n$Device)
  tags <- n$Device %>% unique
  tags[order(tags)]

  if(!is.null(n$Position)){
    n$latitude <- sapply(n$Position %>% strsplit(","), "[", 1) %>% as.numeric
    n$longitude <- sapply(n$Position %>% strsplit(","), "[", 2) %>% as.numeric
  }

  n$timestamp <- dmy_hms(n$`Time (Paris)`, tz = "Europe/Berlin")
  n$vedba <- n$`Total VeDBA`

  # add deployment info
  # capture_data$capture_time

  # filter data by deployments
  data_deployment <- data.table()
  i = 2
  for(i in 1:nrow(capture_data)){
    idx <- which(n$tag_ID == capture_data$tag_ID[i])
    if(length(idx) > 0){
      temp <- n[idx,]
      if(!is.na(capture_data$capture_time[i])){
        temp <- temp[temp$timestamp > capture_data$capture_time[i],]
      }
      temp$pit_tag <- capture_data$ID[i]
      temp$ring <- capture_data$ring[i]
      temp$attachment_type <- capture_data$attachment_type[i]
      temp$capture_timestamp <- capture_data$capture_time[i]
      temp$tag_weight <- capture_data$tag_weight[i]
      temp$bat_weight <- capture_data$capture_weight[i]
      temp$FA_length <- capture_data$FA_length[i]
      temp$sex <- capture_data$sex[i]
      temp$age <- capture_data$age[i]
      temp$repro_status <- capture_data$repro_status[i]
      temp$species <- capture_data$species[i]
      temp$capture_latitude <- capture_data$latitude[i]
      temp$capture_longitude <- capture_data$longitude[i]
      temp$roost <- capture_data$roost[i]

      data_deployment <- rbind(data_deployment, temp)
    }
  }
  return(data_deployment)
}
