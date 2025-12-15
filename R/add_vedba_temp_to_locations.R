
add_vedba_temp_to_locations <- function(df,
                                        track_col = individual_local_identifier,
                                        time_col  = timestamp) {
  require(dplyr)
  require(rlang)
  require(sf)

  track_col <- enquo(track_col)
  time_col  <- enquo(time_col)

  track_name <- quo_name(track_col)
  time_name  <- quo_name(time_col)

  # 1. Summarise VeDBA and avg.temp per (track, time) from sensor rows
  summaries <- df %>%
    st_drop_geometry() %>%                       # don't let summarise mess with geometry
    group_by(!!track_col, !!time_col) %>%
    summarise(
      vedba_sum = {
        v <- vedba[sensor_type == "VeDBA"]
        if (length(v) == 0L || all(is.na(v))) {
          NA_real_
        } else {
          sum(as.numeric(v), na.rm = TRUE)      # ensure plain numeric
        }
      },
      avg_temp = {
        t <- external_temperature[sensor_type == "avg.temp"]
        if (length(t) == 0L || all(is.na(t))) {
          NA_real_
        } else {
          mean(as.numeric(t), na.rm = TRUE)     # ensure plain numeric
        }
      },
      .groups = "drop"
    )

  # 2. Join summaries back to full df
  out <- df %>%
    left_join(
      summaries,
      by = setNames(c(track_name, time_name),
                    c(track_name, time_name))
    ) %>%
    # 3. Keep summary values only for location rows
    mutate(
      vedba_sum = if_else(sensor_type == "location", vedba_sum, NA_real_),
      avg_temp  = if_else(sensor_type == "location", avg_temp,  NA_real_)
    )

  out
}
