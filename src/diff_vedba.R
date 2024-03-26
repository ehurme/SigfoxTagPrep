diff_vedba <- function(data){
  data$diff_vedba <- NA_real_
  data$diff_time <- NA_real_
  # Sort the dataframe by 'tag_id' and 'timestamp'
  data <- data |> arrange(tag_id, timestamp)

  for (tag in unique(data$tag_id)) {
    # Subset data for the current tag
    sub_data <- data |> filter(tag_id == tag)
    sub_data$diff_vedba[1] <- as.numeric(sub_data$total_vedba[1])
    if(nrow(sub_data) > 1){
      # Loop through each row to assign burst_id
      for (i in 2:nrow(sub_data)) {
        vedba_diff <- as.numeric(sub_data$total_vedba[i] - sub_data$total_vedba[i-1])
        sub_data$diff_vedba[i-1] <- vedba_diff
        sub_data$diff_time[i-1] <- difftime(sub_data$timestamp[i], sub_data$timestamp[i-1], units = "mins")
      }
    }

    # Update the original dataframe with the burst_ids for this tag
    data[data$tag_id == tag, ] <- sub_data
    data$diff_vedba[data$diff_vedba < 0] <- NA
  }
  return(data)
}
