calculate_bearing_for_sequential_points <- function(data) {
  # Initialize a new column for bearing
  data$bearing <- NA

  # Get the unique identifiers
  unique_tags <- unique(data$tag_id)

  # Loop through each unique identifier to calculate bearing
  for(tag in unique_tags) {
    subset_data <- data[data$tag_id == tag,]
    if(nrow(subset_data) > 1){
      for(i in 1:(nrow(subset_data)-1)) {
        row1 <- subset_data[i,]
        row2 <- subset_data[i+1,]

        # Calculate time difference
        time_diff <- as.numeric(difftime(row2$timestamp, row1$timestamp, units = "mins"))
        if(!is.na(time_diff)){
          # If time difference is less than 100, calculate bearing
          if(time_diff < 100) {
            lon1 <- row1$location_long
            lat1 <- row1$location_lat
            lon2 <- row2$location_long
            lat2 <- row2$location_lat

            bearing <- calculate_bearing(lon1, lat1, lon2, lat2)

            # Check if the subsetting actually returns rows to update
            indices_to_update <- which(data$timestamp == row2$timestamp & data$tag_id == tag)
            if (sum(indices_to_update) > 0) {
              data$bearing[indices_to_update] <- bearing
            }
          }
        }
      }
    }
  }

  return(data)
}