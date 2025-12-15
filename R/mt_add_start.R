# add start location to move2 tracks

mt_add_start <- function(m2){
  require(move2)
  require(magrittr)
  require(dplyr)

  # create capture move object
  track_data <- mt_track_data(m2)
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

  # order data
  m2 <- m2[order(mt_time(m2)),]
  m2 <- m2[order(mt_track_id(m2)),]
  # if(!mt_is_time_ordered(m2)){
  #   m2 <- dplyr::arrange(m2, mt_track_id(m2), mt_time(m2))
  # }

  # remove duplicates
  m2 <- unique(m2)
  return(m2)
}

