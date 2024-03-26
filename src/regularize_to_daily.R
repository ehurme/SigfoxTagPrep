regularize_to_daily <- function(data){
  if(any(is(data) == "data.frame")){
    # daytime movements
    # require(suncalc)
    require(pacman)
    p_load(tidyverse, dplyr, lubridate, elevatr, raster, move, numbers)

    data |> mutate(date = date(timestamp), ID = tag_id) |>
      reframe(lat = latitude[which.min(abs(time_to_noon))],
              lon = longitude[which.min(abs(time_to_noon))],
              diff_noon = time_to_noon[which.min(abs(time_to_noon))],
              time = timestamp[which.min(abs(time_to_noon))],
              # elevation_min = min(elevation),
              # elevation_max = max(elevation),
              # elevation_noon = elevation[which.min(abs(time_to_noon))],
              vedba_min = min(total_vedba, na.rm = TRUE),
              vedba_max = max(total_vedba, na.rm = TRUE),
              vedba_noon = total_vedba[which.min(abs(time_to_noon))],
              activity = x24h_active_percent[which.min(abs(time_to_noon))],
              radius = max(radius)/1000,
              accuracy = x24h_active_percent[which.min(abs(time_to_noon))],
              min_temp = min(x24h_min_temperature_c, na.rm = TRUE),
              max_temp = max(x24h_max_temperature_c, na.rm = TRUE),
              capture_mass = first(bat_weight),
              fa = first(fa_length),
              tag_mass = first(tag_weight),
              #is_capture_location = first(is_capture_point),
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
