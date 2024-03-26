diff_dist <- function(data) {
  require(geosphere)
  # Initialize a new column for distances
  data$distance <- NA_real_

  # Loop through each unique tag
  for (tag in unique(data$tag_id)) {
    # Subset the data for the current tag
    sub_data <- data[data$tag_id == tag, ]

    if(nrow(sub_data) > 1){
      # Sort the data by timestamp if needed
      sub_data <- sub_data[order(sub_data$timestamp), ]

      # Calculate distance for this subset
      for (i in 2:nrow(sub_data)) {
        coord1 <- c(sub_data$longitude[i-1], sub_data$latitude[i-1])
        coord2 <- c(sub_data$longitude[i], sub_data$latitude[i])

        # Calculate distance
        ## distance is assigned to the first location of a segment
        sub_data$distance[i-1] <- distGeo(coord1, coord2)
      }
    }
    # Update the original dataframe with the distances for this tag
    data[data$tag_id == tag, ] <- sub_data
  }
  return(data)
}