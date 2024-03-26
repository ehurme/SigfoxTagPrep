tag_fell_off <- function(data, vedba_threshold) {
  tags <- unique(data$tag_id)
  data$tag_fell_off <- FALSE # Initialize the new column

  for (tag in tags) {
    tag_data_indices <- which(data$tag_id == tag)
    tag_data <- data[tag_data_indices, ]

    # # Determine if vedba drops below threshold and never exceeds it again

    # if(all(is.na(tag_data$vpm))){
    #   data$tag_fell_off[tag_data_indices] <- FALSE
    # }
    if(nrow(tag_data) > 1){
      # if all are below mark as fell off
      if(all(na.omit(tag_data$vpm) < vedba_threshold)){
        data$tag_fell_off[tag_data_indices] <- TRUE
      }
      if(any(na.omit(tag_data$vpm) > vedba_threshold)){
        # what is the last point above the vedba threshold?
        last_time_above_vpm <- which.max(tag_data$timestamp[tag_data$vpm >= vedba_threshold])
        fell_off_index <- which(tag_data$timestamp ==
                                  tag_data$timestamp[tag_data$vpm >= vedba_threshold][last_time_above_vpm])
        if (length(fell_off_index) > 0 &&
            all(na.omit(tag_data$vpm[(fell_off_index+1):length(tag_data$vpm)]) <= vedba_threshold)) {
          data$tag_fell_off[tag_data_indices[(fell_off_index+2):length(tag_data_indices)]] <- TRUE
        }
      }
      # if only last point is vpm NA, make it FALSE
      if(is.na(last(data$vpm[tag_data_indices])) &
         data$tag_fell_off[tag_data_indices[length(tag_data_indices)-1]] == FALSE){
        data$tag_fell_off[tag_data_indices[length(tag_data_indices)]] <- FALSE
      }
    }
  }
  return(data)
}
