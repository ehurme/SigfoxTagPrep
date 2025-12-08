# add start location to

mt_add_start <- function(m2){
  require(move2)
  require(magrittr)
  track_data <- mt_track_data(m2)

  captures <- mt_as_move2(x = track_data %>%
                            mutate(timestamp = deploy_on_timestamp,
                                   geometry = deploy_on_location) %>%
                            select(individual_local_identifier, timestamp, geometry),
                          time_column = "timestamp",
                          track_id_column = "individual_local_identifier", sf_column_name = "geometry")
  captures <- move2::mt_set_track_data(captures, data = track_data)

  m2 <- mt_stack(m2,
                      captures,
                      .track_combine = "merge")
  return(m2)
}

