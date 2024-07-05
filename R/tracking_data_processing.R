# Load necessary packages
# This script assumes the necessary libraries are loaded where required.
# It is good practice to specify library dependencies in the DESCRIPTION file of your package.

#' Determine Day or Night for Tracking Data
#'
#' Calculates whether each record in the dataset occurs during the day or night
#' based on the latitude, longitude, and timestamp of the record.
#' Requires the 'suncalc' package.
#'
#' @param data A data frame containing the columns `latitude`, `longitude`, and `timestamp`.
#' @return The input data frame with additional columns for `day_night`, `sunrise`, `sunset`, and `noon`.
determine_day_night <- function(data) {
  if (!requireNamespace("suncalc", quietly = TRUE)) {
    stop("Package 'suncalc' is required but not installed.")
  }

  data$day_night <- NA
  data$sunrise <- NA
  data$sunset <- NA
  data$noon <- NA

  for (i in 1:nrow(data)) {
    current_time <- as.POSIXct(data$timestamp[i], origin="1970-01-01", tz = "CET")
    current_date <- as.Date(current_time)
    latitude <- data$latitude[i]
    longitude <- data$longitude[i]

    if (!is.na(latitude)) {
      solar_position <- suncalc::getSunlightPosition(date = current_time, lat = latitude, lon = longitude)
      data$day_night[i] <- ifelse(solar_position$altitude > 0, "Day", "Night")

      sun_times_today <- suncalc::getSunlightTimes(date = current_date, lat = latitude, lon = longitude)
      sun_times_prev <- suncalc::getSunlightTimes(date = current_date - 1, lat = latitude, lon = longitude)
      sun_times_next <- suncalc::getSunlightTimes(date = current_date + 1, lat = latitude, lon = longitude)

      candidate_sunrise_times <- c(sun_times_today$sunrise, sun_times_prev$sunrise, sun_times_next$sunrise)
      candidate_sunset_times <- c(sun_times_today$sunset, sun_times_prev$sunset, sun_times_next$sunset)
      candidate_noon_times <- c(sun_times_today$solarNoon, sun_times_prev$solarNoon, sun_times_next$solarNoon)

      nearest_sunrise <- candidate_sunrise_times[which.min(abs(difftime(candidate_sunrise_times, current_time, units = "mins")))]
      nearest_sunset <- candidate_sunset_times[which.min(abs(difftime(candidate_sunset_times, current_time, units = "mins")))]
      nearest_noon <- candidate_noon_times[which.min(abs(difftime(candidate_noon_times, current_time, units = "mins")))]

      data$sunrise[i] <- nearest_sunrise
      data$sunset[i] <- nearest_sunset
      data$noon[i] <- nearest_noon
    }
  }

  data$sunrise <- as.POSIXct(data$sunrise, origin = "1970-01-01")
  data$sunset <- as.POSIXct(data$sunset, origin = "1970-01-01")
  data$noon <- as.POSIXct(data$noon, origin = "1970-01-01")

  return(data)
}

#' Determine Bursts in Tracking Data
#'
#' Identifies bursts in the tracking data based on time intervals between consecutive records.
#' Utilizes 'dplyr' and 'lubridate' for data manipulation and time calculations.
#'
#' @param data A data frame containing the columns `tag_id` and `timestamp`.
#' @return The input data frame with an additional column for `burst_id`.
determine_bursts <- function(data) {
  # Ensure timestamp is in POSIXct format for time-based calculations
  data$timestamp <- as.POSIXct(data$timestamp, format = "%Y-%m-%d %H:%M:%S")

  # Sort the dataframe by 'tag_id' and 'timestamp'
  data <- data %>%
    arrange(tag_id, timestamp)

  # Initialize a new column for burst_id
  data$burst_id <- NA_real_

  # Loop through each tag to identify bursts
  for (tag in unique(data$tag_id)) {
    sub_data <- data %>%
      filter(tag_id == tag, !is.na(timestamp))
    #sub_data %>% View()

    if (nrow(sub_data) > 1) {
      burst_id <- 1
      sub_data$burst_id[1] <- burst_id

      for (i in 2:nrow(sub_data)) {
        time_diff <- as.numeric(difftime(sub_data$timestamp[i], sub_data$timestamp[i-1], units = "mins"))

        # Define a new burst if the time difference is more than 130 minutes
        if (time_diff > 130) {
          burst_id <- burst_id + 1
        }

        sub_data$burst_id[i] <- burst_id
      }
    } else {
      sub_data$burst_id[1] <- 1
    }

    data[data$tag_id == tag & !is.na(data$timestamp), ] <- sub_data
  }

  return(data)
}

#' Calculate Distance and Bearing Between Consecutive Points
#'
#' Auxiliary functions to calculate bearing and invoke bearing calculation for sequential points
#' in tracking data. It's a helper for other analysis functions.
#' Calculate Bearing Between Two Geographic Points
#'
#' This function calculates the bearing from one point to another using their longitude and latitude.
#'
#' @param lon1 Longitude of the first point in degrees.
#' @param lat1 Latitude of the first point in degrees.
#' @param lon2 Longitude of the second point in degrees.
#' @param lat2 Latitude of the second point in degrees.
#' @return Bearing in degrees from the North.
calculate_bearing <- function(lon1, lat1, lon2, lat2) {
  # Convert degrees to radians
  lat1_rad <- lat1 * (pi / 180)
  lat2_rad <- lat2 * (pi / 180)
  delta_lon_rad <- (lon2 - lon1) * (pi / 180)

  # Calculate bearing
  bearing_rad <- atan2(sin(delta_lon_rad) * cos(lat2_rad),
                       cos(lat1_rad) * sin(lat2_rad) - sin(lat1_rad) * cos(lat2_rad) * cos(delta_lon_rad))

  # Convert radians to degrees
  bearing_deg <- (bearing_rad * (180 / pi) + 360) %% 360

  return(bearing_deg)
}


#' Calculate Bearing for Sequential Points in Tracking Data
#'
#' This function iterates through tracking data for each unique tag ID, calculating the bearing
#' between sequential points using their geographic coordinates.
#'
#' @param data A data frame with tracking data that must include `tag_id`, `latitude`, and `longitude` columns,
#'             and a `timestamp` to ensure the points are in sequential order.
#' @return The input data frame with an additional column `bearing` indicating the bearing from each point
#'         to the next in the sequence.
calculate_bearing_for_sequential_points <- function(data) {
  # Ensure data is sorted by tag_id and timestamp
  data <- data[order(data$tag_id, data$timestamp),]

  # Initialize the bearing column with NAs
  data$bearing <- NA_real_

  # Get unique tag IDs
  unique_tags <- unique(data$tag_id)

  for (tag in unique_tags) {
    # Subset data for the current tag
    tag_data <- data[data$tag_id == tag,]

    if (nrow(tag_data) > 1) {
      for (i in 1:(nrow(tag_data) - 1)) {
        lon1 <- tag_data$longitude[i]
        lat1 <- tag_data$latitude[i]
        lon2 <- tag_data$longitude[i + 1]
        lat2 <- tag_data$latitude[i + 1]

        # Calculate bearing using the calculate_bearing function
        tag_data$bearing[i] <- calculate_bearing(lon1, lat1, lon2, lat2)
      }
    }

    # Update the original data with bearings calculated for the current tag
    data[data$tag_id == tag,] <- tag_data
  }

  return(data)
}


#' Calculate Distance Between Sequential Points in Tracking Data
#'
#' This function iterates through tracking data for each unique tag ID, calculating the distance
#' between sequential points using their geographic coordinates.
#'
#' @param data A data frame with tracking data that must include `tag_id`, `latitude`, and `longitude` columns,
#'             and a `timestamp` to ensure the points are in sequential order.
#' @return The input data frame with an additional column `distance` and `distance_from_start` indicating the distance in meters
#'         from each point to the next in the sequence and from the origin.
diff_dist <- function(data) {
  if (!requireNamespace("geosphere", quietly = TRUE)) {
    stop("Package 'geosphere' is required but not installed.")
  }

  # Ensure data is sorted by tag_id and timestamp
  data <- data[order(data$tag_id, data$timestamp),]

  # Initialize the distance column with NAs
  data$distance <- NA_real_
  df$distance_from_start <- NA_real_
  # Get unique tag IDs
  unique_tags <- unique(data$tag_id)

  for (tag in unique_tags) {
    # Subset data for the current tag
    tag_data <- data[data$tag_id == tag,]
    #tag_data$distance_from_start[1] <- 0
    if (nrow(tag_data) > 1) {
      for (i in 1:(nrow(tag_data) - 1)) {
        coord1 <- c(tag_data$longitude[i], tag_data$latitude[i])
        coord2 <- c(tag_data$longitude[i + 1], tag_data$latitude[i + 1])

        # Calculate distance using the distGeo function from the geosphere package
        tag_data$distance[i] <- geosphere::distGeo(coord1, coord2)
        tag_data$distance_from_start[i] <- geosphere::distGeo(start, coord1)
      }
      # get last location distance
      coord1 <- c(tag_data$longitude[nrow(tag_data)], tag_data$latitude[nrow(tag_data)])
      tag_data$distance_from_start[nrow(tag_data)] <- geosphere::distGeo(start, coord1)
    }

    # Update the original data with distances calculated for the current tag
    data[data$tag_id == tag,] <- tag_data
  }

  return(data)
}


#' Calculate Differential VeDBA and Time Between Sequential Points
#'
#' This function iterates through tracking data for each unique tag ID, calculating the difference
#' in VeDBA (vectorial dynamic body acceleration) between sequential records and the time difference
#' between these records.
#'
#' @param data A data frame with tracking data that must include `tag_id`, `timestamp`, and `total_vedba` columns.
#' @return The input data frame with additional columns `diff_vedba` for the VeDBA difference and
#'         `diff_time` for the time difference in minutes between each record and its successor.
diff_vedba <- function(data) {
  # Ensure data is sorted by tag_id and timestamp
  data <- data[order(data$tag_id, data$timestamp),]

  # Initialize the diff_vedba and diff_time columns with NAs
  data$diff_vedba <- NA_real_
  data$diff_time <- NA_real_

  # Get unique tag IDs
  unique_tags <- unique(data$tag_id)

  for (tag in unique_tags) {
    # Subset data for the current tag
    tag_data <- data[data$tag_id == tag,]

    if (nrow(tag_data) > 1) {
      for (i in 1:(nrow(tag_data) - 1)) {
        # Calculate difference in VeDBA
        tag_data$diff_vedba[i] <- tag_data$total_vedba[i + 1] - tag_data$total_vedba[i]

        # Calculate time difference in minutes
        tag_data$diff_time[i] <- as.numeric(difftime(tag_data$timestamp[i + 1], tag_data$timestamp[i], units = "mins"))
      }
    }

    # Update the original data with VeDBA and time differences for the current tag
    data[data$tag_id == tag,] <- tag_data
  }

  return(data)
}



#' Determine Potential Tag Detachment Based on Activity Threshold
#'
#' This function assesses tracking data for each unique tag ID to determine potential detachment
#' events, based on a specified threshold for minimum activity levels (e.g., VeDBA).
#'
#' @param data A data frame with tracking data that must include `tag_id`, `timestamp`, and `vpm` (VeDBA per minute) columns.
#' @param vedba_threshold A numeric value representing the threshold below which a tag is considered potentially detached.
#' @return The input data frame with an additional logical column `tag_fell_off` indicating whether the tag
#'         is considered to have potentially fallen off.
tag_fell_off <- function(data, vedba_threshold) {
  # Ensure data is sorted by tag_id and timestamp
  data <- data[order(data$tag_id, data$timestamp),]

  # Initialize the tag_fell_off column with FALSE
  data$tag_fell_off <- FALSE

  # Get unique tag IDs
  unique_tags <- unique(data$tag_id)

  for (tag in unique_tags) {
    # Subset data for the current tag
    tag_data <- data[data$tag_id == tag,]

    # Check for a single-row scenario with vpm = NA
    if (nrow(tag_data) == 1 && is.na(tag_data$vpm)) {
      data$tag_fell_off[data$tag_id == tag] <- TRUE
    } else {
      # Identify points where VeDBA drops below the threshold and never recovers
      below_threshold <- tag_data$vpm < vedba_threshold
      if (all(below_threshold, na.rm = TRUE)) {
        # If all VeDBA values are below the threshold, mark the entire tag as potentially fallen off
        data$tag_fell_off[data$tag_id == tag] <- TRUE
      } else {
        # Find the last index where VeDBA is above the threshold
        last_above <- max(which(!below_threshold), na.rm = TRUE)
        # Mark as fallen off after the last point where VeDBA was above the threshold
        if (last_above < nrow(tag_data)) {
          idx <- which(data$tag_id == tag)
          data$tag_fell_off[(idx[last_above + 1]):idx[length(idx)]] <- TRUE
        }
      }
    }
  }

  return(data)
}


#' Regularize Tracking Data to Daily Summaries
#'
#' Aggregates tracking data to daily summaries for each tag, calculating various metrics
#' such as distance moved, average VeDBA, and others.
#'
#' @param data A data frame with tracking data including `tag_id`, `timestamp`, `latitude`, `longitude`,
#'        and other relevant metrics like `total_vedba`.
#' @return A data frame with daily summaries for each tag.
regularize_to_daily <- function(data) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required but not installed.")
  }
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("Package 'lubridate' is required but not installed.")
  }

  # Calculate additional metrics if necessary, e.g., distance, bearing, before summarizing
  data <- data %>%
    arrange(tag_id, timestamp) %>%
    mutate(
      ID = tag_id,
      date = as.Date(timestamp),
      location_lat = sapply(strsplit(position, ","), "[", 1) %>% as.numeric(),
      location_lon = sapply(strsplit(position, ","), "[", 2) %>% as.numeric(),
      # Assuming calculate_bearing_for_sequential_points and diff_dist are already applied to the data
      # distance_moved = distance, # Uncomment or modify according to your dataset
      # bearing_change = bearing # Uncomment or modify according to your dataset
    )

  # Daily summaries
  data %>%
    group_by(ID, burst_id) %>%
    reframe(
      species = species[1],
      sex = sex[1],
      lat = location_lat[which.min(abs(time_to_noon))],
      lon = location_lon[which.min(abs(time_to_noon))],
      diff_noon = time_to_noon[which.min(abs(time_to_noon))],
      time = timestamp[which.min(abs(time_to_noon))],
      daily_distance = sum(distance, na.rm = TRUE),
      vedba_min = min(total_vedba, na.rm = TRUE),
      vedba_max = max(total_vedba, na.rm = TRUE),
      vedba_noon = total_vedba[which.min(abs(time_to_noon))],
      activity = x24h_active_percent[which.min(abs(time_to_noon))],
      radius = max(radius)/1000,
      accuracy = link_quality[which.min(abs(time_to_noon))],
      min_temp = min(x24h_min_temperature_c, na.rm = TRUE),
      max_temp = max(x24h_max_temperature_c, na.rm = TRUE),
      capture_mass = first(capture_weight),
      fa = first(fa_length),
      tag_mass = first(tag_weight),
      #is_capture_location = is.na(raw_data),
      day_night = first(day_night)
      #.groups = 'drop'
    ) -> m_day
  m_day$vedba_min[which(is.infinite(m_day$vedba_min))] <- NA
  m_day$vedba_max[which(is.infinite(m_day$vedba_max))] <- NA
  m_day$min_temp[which(is.infinite(m_day$min_temp))] <- NA
  m_day$max_temp[which(is.infinite(m_day$max_temp))] <- NA

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
}
