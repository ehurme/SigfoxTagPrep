add_vedba_temp_to_locations <- function(df,
                                        track_col = individual_local_identifier,
                                        time_col  = timestamp) {
  require(dplyr)
  require(rlang)

  track_col <- enquo(track_col)
  time_col  <- enquo(time_col)

  track_name <- quo_name(track_col)
  time_name  <- quo_name(time_col)

  # --- sanity checks ---
  needed <- c(track_name, time_name,
              "sensor_type", "vedba", "external_temperature")
  missing <- setdiff(needed, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }

  # Drop existing outputs to avoid .x/.y join issues
  df2 <- df %>%
    dplyr::select(-any_of(c("vedba_sum", "avg_temp",
                     "vedba_sum_join", "avg_temp_join")))

  # 1) Aggregate sensor data per (track, time)
  summaries <- df2 %>%
    group_by(!!track_col, !!time_col) %>%
    reframe(
      vedba_sum_join = {
        v <- vedba[sensor_type == "VeDBA"]
        if (length(v) == 0L || all(is.na(v))) NA_real_
        else sum(as.numeric(v), na.rm = TRUE)
      },
      avg_temp_join = {
        t <- external_temperature[sensor_type == "avg.temp"]
        if (length(t) == 0L || all(is.na(t))) NA_real_
        else mean(as.numeric(t), na.rm = TRUE)
      }
    )

  # 2) Join back + keep values only on location rows
  out <- df2 %>%
    left_join(
      summaries,
      by = setNames(c(track_name, time_name),
                    c(track_name, time_name))
    ) %>%
    mutate(
      vedba_sum = if_else(sensor_type == "location",
                          vedba_sum_join, NA_real_),
      avg_temp  = if_else(sensor_type == "location",
                          avg_temp_join,  NA_real_)
    ) %>%
    dplyr::select(-vedba_sum_join, -avg_temp_join)

  out
}
