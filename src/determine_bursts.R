determine_bursts <- function(data) {
  require(dplyr)
  require(lubridate)
  # Convert timestamp to POSIXct type for time-based calculations
  data$timestamp <- as.POSIXct(data$timestamp, format="%Y-%m-%d %H:%M:%S")

  # Sort the dataframe by 'tag_id' and 'timestamp'
  data <- data |> arrange(tag_id, timestamp)

  # Initialize a new column for burst_id
  data$burst_id <- NA_real_

  # Loop through each tag
  for (tag in unique(data$tag_id)) {
    # Subset data for the current tag
    sub_data <- data |> filter(tag_id == tag)
    sub_data$burst_id[1] <- 1
    if(nrow(sub_data) > 1){
      # Initialize burst_id for this subset
      burst_id = 1

      # Loop through each row to assign burst_id
      for (i in 2:nrow(sub_data)) {
        time_diff <- as.numeric(difftime(sub_data$timestamp[i], sub_data$timestamp[i-1], units="mins"))

        # If time difference is more than 60 minutes, it's a new burst
        if(time_diff > 130) {
          burst_id <- burst_id + 1
        }

        sub_data$burst_id[i] <- burst_id
      }
    }
    # Update the original dataframe with the burst_ids for this tag
    data[data$tag_id == tag, ] <- sub_data
  }
  return(data)
}