library(magrittr)

clean_sigfox <- function(data){



}

# Function to determine if each timestamp is during day or night and add solar times
determine_day_night <- function(df) {
  require(suncalc)
  # Initialize new columns for day/night and solar times
  df$day_night <- NA
  df$sunrise <- NA
  df$sunset <- NA
  df$noon <- NA

  # Initialize progress bar
  pb <- txtProgressBar(min = 0, max = nrow(df), style = 3)

  # Loop through each row
  for (i in 1:nrow(df)) {
    # Update progress bar
    setTxtProgressBar(pb, i)
    # Ensure that sunrise, sunset, and timestamp are in POSIXct format
    current_time <- as.POSIXct(df$timestamp[i], origin="1970-01-01", tz = "CET")
    current_date <- as.Date(current_time)
    latitude <- df$location_lat[i]
    longitude <- df$location_long[i]
    if(!is.na(latitude)){
      # Get solar position for the current timestamp
      solar_position <- getSunlightPosition(date=current_time, lat=latitude, lon=longitude)

      # Use solar altitude to determine day or night
      if (solar_position$altitude > 0) {
        df$day_night[i] <- "Day"
      } else {
        df$day_night[i] <- "Night"
      }

      # Get sun times for the current, previous, and next dates
      sun_times_today <- getSunlightTimes(date=current_date, lat=latitude, lon=longitude)
      sun_times_prev <- getSunlightTimes(date=current_date - 1, lat=latitude, lon=longitude)
      sun_times_next <- getSunlightTimes(date=current_date + 1, lat=latitude, lon=longitude)

      # Separate candidate sun times for each event
      candidate_sunrise_times <- c(
        as.POSIXct(sun_times_today$sunrise, origin="1970-01-01"),
        as.POSIXct(sun_times_prev$sunrise, origin="1970-01-01"),
        as.POSIXct(sun_times_next$sunrise, origin="1970-01-01")
      )

      candidate_sunset_times <- c(
        as.POSIXct(sun_times_today$sunset, origin="1970-01-01"),
        as.POSIXct(sun_times_prev$sunset, origin="1970-01-01"),
        as.POSIXct(sun_times_next$sunset, origin="1970-01-01")
      )

      candidate_noon_times <- c(
        as.POSIXct(sun_times_today$solarNoon, origin="1970-01-01"),
        as.POSIXct(sun_times_prev$solarNoon, origin="1970-01-01"),
        as.POSIXct(sun_times_next$solarNoon, origin="1970-01-01")
      )

      # Find the nearest sunrise, sunset, and solar noon times
      nearest_sunrise <- candidate_sunrise_times[which.min(abs(difftime(candidate_sunrise_times, current_time, units="mins")))]
      nearest_sunset <- candidate_sunset_times[which.min(abs(difftime(candidate_sunset_times, current_time, units="mins")))]
      nearest_noon <- candidate_noon_times[which.min(abs(difftime(candidate_noon_times, current_time, units="mins")))]

      # Save these times to the dataframe
      df$sunrise[i] <- nearest_sunrise
      df$sunset[i] <- nearest_sunset
      df$noon[i] <- nearest_noon
    }
  }

  # Close the progress bar
  close(pb)

  return(df)
}

add_capture_data <- function(df, capture_df) {
  require(dplyr)

  # Prepare capture data
  capture_df <- capture_df |>
    dplyr::select(tag_local_identifier, animal_local_identifier,
                  animal_sex,
                  deploy_on_timestamp,
                  animal_mass, deploy_on_measurements,
                  animal_reproductive_condition,
                  attachment_type,
                  tag_weight,
                  deploy_on_latitude, deploy_on_longitude) |>
    mutate(location_lat = deploy_on_latitude,
           location_long = deploy_on_longitude,
           timestamp = ymd_hms(deploy_on_timestamp, tz = "CET"),
           sex = animal_sex,
           fa = as.numeric(str_extract(deploy_on_measurements, "\\d+\\.?\\d*")))

  # Prepare main data
  df <- df |>
    mutate(timestamp = as.POSIXct(timestamp, format = "%Y-%m-%d %H:%M:%S"))

  # Create capture location rows
  capture_location_rows <- capture_df |>
    # dplyr::select(tag_local_identifier, timestamp, location_lat, location_long) |>
    mutate(is_capture_point = TRUE)

  # Merge capture location rows with main data
  merged_df <- bind_rows(df, capture_location_rows) |>
    arrange(tag_local_identifier, timestamp)

  return(merged_df)
}

determine_bursts <- function(df) {
  # Convert timestamp to POSIXct type for time-based calculations
  df$timestamp <- as.POSIXct(df$timestamp, format="%Y-%m-%d %H:%M:%S")

  # Sort the dataframe by 'tag_local_identifier' and 'timestamp'
  df <- df |> arrange(tag_local_identifier, timestamp)

  # Initialize a new column for burst_id
  df$burst_id <- NA_real_

  # Loop through each tag
  for (tag in unique(df$tag_local_identifier)) {
    # Subset data for the current tag
    sub_df <- df |> filter(tag_local_identifier == tag)
    sub_df$burst_id[1] <- 1
    if(nrow(sub_df) > 1){
      # Initialize burst_id for this subset
      burst_id = 1

      # Loop through each row to assign burst_id
      for (i in 2:nrow(sub_df)) {
        time_diff <- as.numeric(difftime(sub_df$timestamp[i], sub_df$timestamp[i-1], units="mins"))

        # If time difference is more than 60 minutes, it's a new burst
        if(time_diff > 130) {
          burst_id <- burst_id + 1
        }

        sub_df$burst_id[i] <- burst_id
      }
    }
    # Update the original dataframe with the burst_ids for this tag
    df[df$tag_local_identifier == tag, ] <- sub_df
  }
  return(df)
}

diff_vedba <- function(df){
  df$diff_vedba <- NA_real_
  df$diff_time <- NA_real_
  # Sort the dataframe by 'tag_local_identifier' and 'timestamp'
  df <- df |> arrange(tag_local_identifier, timestamp)

  for (tag in unique(df$tag_local_identifier)) {
    # Subset data for the current tag
    sub_df <- df |> filter(tag_local_identifier == tag)
    sub_df$diff_vedba[1] <- as.numeric(sub_df$behavioural_classification[1])
    if(nrow(sub_df) > 1){
      # Loop through each row to assign burst_id
      for (i in 2:nrow(sub_df)) {
        vedba_diff <- as.numeric(sub_df$behavioural_classification[i] - sub_df$behavioural_classification[i-1])
        sub_df$diff_vedba[i-1] <- vedba_diff
        sub_df$diff_time[i-1] <- difftime(sub_df$timestamp[i], sub_df$timestamp[i-1], units = "mins")
      }
    }
    # Update the original dataframe with the burst_ids for this tag
    df[df$tag_local_identifier == tag, ] <- sub_df
  }
  return(df)
}

# Function to calculate distance between consecutive points for all tags
diff_dist <- function(df) {
  require(geosphere)
  # Initialize a new column for distances
  df$distance <- NA_real_

  # Loop through each unique tag
  for (tag in unique(df$tag_local_identifier)) {
    # Subset the data for the current tag
    sub_df <- df[df$tag_local_identifier == tag, ]

    if(nrow(sub_df) > 1){
      # Sort the data by timestamp if needed
      sub_df <- sub_df[order(sub_df$timestamp), ]

      # Calculate distance for this subset
      for (i in 2:nrow(sub_df)) {
        coord1 <- c(sub_df$location_long[i-1], sub_df$location_lat[i-1])
        coord2 <- c(sub_df$location_long[i], sub_df$location_lat[i])

        # Calculate distance
        ## distance is assigned to the first location of a segment
        sub_df$distance[i-1] <- distGeo(coord1, coord2)
      }
    }
    # Update the original dataframe with the distances for this tag
    df[df$tag_local_identifier == tag, ] <- sub_df
  }
  return(df)
}

# Function to calculate bearing for sequential points
calculate_bearing_for_sequential_points <- function(df) {
  # Initialize a new column for bearing
  df$bearing <- NA

  # Get the unique identifiers
  unique_tags <- unique(df$tag_local_identifier)

  # Loop through each unique identifier to calculate bearing
  for(tag in unique_tags) {
    subset_df <- df[df$tag_local_identifier == tag,]
    if(nrow(subset_df) > 1){
      for(i in 1:(nrow(subset_df)-1)) {
        row1 <- subset_df[i,]
        row2 <- subset_df[i+1,]

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
            indices_to_update <- which(df$timestamp == row2$timestamp & df$tag_local_identifier == tag)
            if (sum(indices_to_update) > 0) {
              df$bearing[indices_to_update] <- bearing
            }
          }
        }
      }
    }
  }

  return(df)
}

# df = b
# tag = "120ADDB"
# vedba_threshold = 280800/(60*24)*2
# with(df[tag_data_indices,], plot(timestamp, vpm, col = tag_fell_off+1))
# with(df[tag_data_indices,], plot(location_long, location_lat, col = tag_fell_off+1))
# df$vpm[tag_data_indices]
# df$tag_fell_off[tag_data_indices]
# function to detect if tag fell off
tag_fell_off <- function(df, vedba_threshold) {
  tags <- unique(df$tag_local_identifier)
  df$tag_fell_off <- FALSE # Initialize the new column

  for (tag in tags) {
    tag_data_indices <- which(df$tag_local_identifier == tag)
    tag_data <- df[tag_data_indices, ]

    # # Determine if vedba drops below threshold and never exceeds it again

    # if(all(is.na(tag_data$vpm))){
    #   df$tag_fell_off[tag_data_indices] <- FALSE
    # }
    if(nrow(tag_data) > 1){
      # if all are below mark as fell off
      if(all(na.omit(tag_data$vpm) < vedba_threshold)){
        df$tag_fell_off[tag_data_indices] <- TRUE
      }
      if(any(na.omit(tag_data$vpm) > vedba_threshold)){
        # what is the last point above the vedba threshold?
        last_time_above_vpm <- which.max(tag_data$timestamp[tag_data$vpm >= vedba_threshold])
        fell_off_index <- which(tag_data$timestamp == tag_data$timestamp[tag_data$vpm >= vedba_threshold][last_time_above_vpm])
        if (length(fell_off_index) > 0 && all(na.omit(tag_data$vpm[(fell_off_index+1):length(tag_data$vpm)]) <= vedba_threshold)) {
          df$tag_fell_off[tag_data_indices[(fell_off_index+2):length(tag_data_indices)]] <- TRUE
        }
      }
      # if only last point is vpm NA, make it FALSE
      if(is.na(last(df$vpm[tag_data_indices])) &
         df$tag_fell_off[tag_data_indices[length(tag_data_indices)-1]] == FALSE){
        df$tag_fell_off[tag_data_indices[length(tag_data_indices)]] <- FALSE
      }
    }

  }

  return(df)
}

dailydistance <- function(m){

  if(any(is(m) == "data.frame")){
    # daytime movements
    # require(suncalc)
    require(pacman)
    p_load(tidyverse, dplyr, lubridate, elevatr, raster, move, numbers)

    m |> mutate(date = date(timestamp), ID = tag_local_identifier) |>
      reframe(lat = location_lat[which.min(abs(time_to_noon))],
              lon = location_long[which.min(abs(time_to_noon))],
              diff_noon = time_to_noon[which.min(abs(time_to_noon))],
              time = timestamp[which.min(abs(time_to_noon))],
              # elevation_min = min(elevation),
              # elevation_max = max(elevation),
              # elevation_noon = elevation[which.min(abs(time_to_noon))],
              vedba_min = min(behavioural_classification, na.rm = TRUE),
              vedba_max = max(behavioural_classification, na.rm = TRUE),
              vedba_noon = behavioural_classification[which.min(abs(time_to_noon))],
              activity = gps_dop[which.min(abs(time_to_noon))],
              radius = max(radius)/1000,
              accuracy = argos_best_level[which.min(abs(time_to_noon))],
              min_temp = min(temperature_min, na.rm = TRUE),
              max_temp = max(temperature_max, na.rm = TRUE),
              capture_mass = first(animal_mass),
              fa = first(fa),
              tag_mass = first(tag_weight),
              is_capture_location = first(is_capture_point),
              day_night = first(day_night),
              .by = c(ID, burst_id)) -> m_day
    m_day$vedba_min[which(is.infinite(m_day$vedba_min))] <- NA
    m_day$vedba_max[which(is.infinite(m_day$vedba_max))] <- NA
    m_day$min_temp[which(is.infinite(m_day$min_temp))] <- NA
    m_day$max_temp[which(is.infinite(m_day$max_temp))] <- NA
    summary(m_day)

    IDs <- unique(m_day$ID)
    m_day$distance <- NA
    m_day$diff_vedba <- NA
    m_day$daily_vedba <- NA
    m_day$diff_time <- NA
    m_day$time_running <- NA
    m_day$bearing <- NA
    #i = 82

    m_day <- m_day[order(m_day$ID, m_day$time),]
    i = 1
    for(i in 1:length(IDs)){
      idx <- which(m_day$ID == IDs[i])
      k = 1
      for(k in 1:(length(idx)-1)){
        try({m_day$distance[idx[k]] <- raster::pointDistance(c(m_day$lon[idx[k+1]], m_day$lat[idx[k+1]]),
                                                             c(m_day$lon[idx[k]], m_day$lat[idx[k]]),
                                                             lonlat=TRUE)/1000
        m_day$diff_time[idx[k]] <- as.numeric(difftime(m_day$time[idx[k+1]],
                                                       m_day$time[idx[k]],
                                                       units = "days"))
        try(m_day$time_running[idx[k]] <- difftime(m_day$time[idx[k]], m_day$time[idx[1]], units = "days"))
        m_day$diff_vedba[idx[k]] <-  (as.numeric(m_day$vedba_noon[idx[k+1]] - m_day$vedba_noon[idx[k]]))
        m_day$daily_vedba[idx[k]] <-  m_day$diff_vedba[idx[k]]/(m_day$diff_time[idx[k]])
        try({m_day$bearing[idx[k]] <- calculate_bearing(lon1 = m_day$lon[idx[k]],
                                                        lat1 = m_day$lat[idx[k]],
                                                        lon2 = m_day$lon[idx[k+1]],
                                                        lat2 = m_day$lat[idx[k+1]])
        })
        })
      }
    }
    m_day <- m_day[order(m_day$ID),]
    m_day$daily_distance <- m_day$distance/m_day$diff_time
    return(m_day)

    print("gained 20,000 XP")
    if(runif(1) > 0.9){
      print("Level Up!!!")
    }
  }
  else print("D'Oh!")
}
