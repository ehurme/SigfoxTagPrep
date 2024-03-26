# wind support functions
# wind direction
wind_dir <- function(u_ms, v_ms) {
  wind_abs <- sqrt(u_ms^2 + v_ms^2)

  wind_dir_trig_to <- atan2(u_ms/wind_abs, v_ms/wind_abs)
  wind_dir_trig_to_degrees <- wind_dir_trig_to * 180/pi

  wind_dir_trig_from_degrees <- wind_dir_trig_to_degrees + 180

  wind_dir_cardinal <- 90 - wind_dir_trig_from_degrees
  wind_dir_cardinal <- (wind_dir_cardinal + 360) %% 360 # Ensure the result is between 0 and 360

  return(wind_dir_cardinal)
}

# Function to calculate bearing
calculate_bearing <- function(lon1, lat1, lon2, lat2) {
  lat1_rad <- lat1 * (pi/180)  # Convert latitude to radians
  lat2_rad <- lat2 * (pi/180)  # Convert latitude to radians
  delta_lon <- (lon2 - lon1) * (pi/180)  # Convert longitude difference to radians

  bearing <- atan2(sin(delta_lon) * cos(lat2_rad),
                   cos(lat1_rad) * sin(lat2_rad) - sin(lat1_rad) * cos(lat2_rad) * cos(delta_lon))

  bearing_deg <- bearing * (180/pi)  # Convert bearing to degrees

  # Ensure the bearing is between 0 and 360 degrees
  bearing_deg <- ifelse(bearing_deg < 0, bearing_deg + 360, bearing_deg)

  return(bearing_deg)
}


wind_support <- function(u,v,heading) {
  angle <- atan2(u,v) - heading/180*pi
  return(cos(angle) * sqrt(u*u+v*v))
}


cross_wind <- function(u,v,heading) {
  angle <- atan2(u,v) - heading/180*pi
  return(sin(angle) * sqrt(u*u+v*v))
}

airspeed <- function(ground_speed, ws, cw)
{
  try(va <- sqrt((ground_speed - ws)^2 + (cw)^2))
  return(va)
}

