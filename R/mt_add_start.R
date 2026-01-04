# add start location to move2 tracks

mt_add_start <- function(m2){
  require(move2)
  require(magrittr)
  require(dplyr)
  require(sf)

  # create capture move object
  track_data <- mt_track_data(m2)

  if(is.null(m2$comments)){
    m2$comments <- NA
  }

  if(!any(c(st_is_empty(track_data$deploy_on_location),
     st_is_empty(track_data$capture_location)))){
    captures <- mt_as_move2(x = track_data %>%
                              dplyr::mutate(timestamp = deploy_on_timestamp,
                                            geometry = deploy_on_location,
                                            sensor_type = "location",
                                            comments = "start") %>%
                              dplyr::select(individual_local_identifier, timestamp, geometry, sensor_type, comments),
                            time_column = "timestamp",
                            track_id_column = "individual_local_identifier", sf_column_name = "geometry")
    captures <- move2::mt_set_track_data(captures, data = track_data)

    m2 <- mt_stack(m2,
                   captures,
                   .track_combine = "merge")

    # identify location duplicates
    location_idx <- which(m2$sensor_type == "location")
    idx <- which(duplicated(paste(m2$individual_local_identifier[location_idx],
                                  m2$timestamp[location_idx])))
    if(length(idx) > 0){
      for(i in idx){
        print(i)
        temp <- m2[location_idx[i],]
        # update comments for original
        original <- which(m2$individual_local_identifier == temp$individual_local_identifier &
                            m2$timestamp == temp$timestamp & m2$sensor_type == "location" & is.na(m2$comments))
        if(length(original) == 1){
          m2$comments[original] <- "start"
        }
      }
      # remove duplicates
      m2 <- m2[-location_idx[idx],]
    }

    # order data
    m2 <- m2[order(mt_time(m2)),]
    m2 <- m2[order(mt_track_id(m2)),]

  }
  return(m2)
}

