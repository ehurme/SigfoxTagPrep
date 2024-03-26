# load("../../../Dropbox/MPI/Noctule/Data/rdata/summer23.robj")
# summer23$timestamp <- summer23$datetime
# data <- summer23

determine_day_night <- function(data) {
  require(suncalc)
  # Initialize new columns for day/night and solar times
  data$day_night <- NA
  data$sunrise <- NA
  data$sunset <- NA
  data$noon <- NA

  # Initialize progress bar
  pb <- txtProgressBar(min = 0, max = nrow(data), style = 3)

  # Loop through each row
  for (i in 1:nrow(data)) {
    # Update progress bar
    setTxtProgressBar(pb, i)
    # Ensure that sunrise, sunset, and timestamp are in POSIXct format
    current_time <- as.POSIXct(data$timestamp[i], origin="1970-01-01", tz = "CET")
    current_date <- as.Date(current_time)
    latitude <- data$latitude[i]
    longitude <- data$longitude[i]
    if(!is.na(latitude)){
      # Get solar position for the current timestamp
      solar_position <- getSunlightPosition(date=current_time, lat=latitude, lon=longitude)

      # Use solar altitude to determine day or night
      if (solar_position$altitude > 0) {
        data$day_night[i] <- "Day"
      } else {
        data$day_night[i] <- "Night"
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
      data$sunrise[i] <- nearest_sunrise
      data$sunset[i] <- nearest_sunset
      data$noon[i] <- nearest_noon
    }
  }

  # Close the progress bar
  close(pb)
  data$sunrise <- as.POSIXct(data$sunrise, origin="1970-01-01")
  data$sunset <- as.POSIXct(data$sunset, origin="1970-01-01")
  data$noon <- as.POSIXct(data$noon, origin="1970-01-01")
  return(data)
}
