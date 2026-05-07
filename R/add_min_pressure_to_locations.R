#' Join minimum 3-hour barometric pressure onto location rows
#'
#' Aggregates minimum barometric pressure from sensor rows
#' (\code{sensor_type == "min.baro.pressure"}) per track/timestamp and joins
#' the result back onto location rows as a new column \code{min_3h_pressure}.
#' Designed for NanoFox multi-sensor Movebank downloads.
#'
#' @param df A data frame or move2 object with columns \code{sensor_type} and
#'   \code{barometric_pressure}, as downloaded from Movebank.
#' @param track_col Unquoted name of the track/individual identifier column.
#'   Default: \code{individual_local_identifier}.
#' @param time_col Unquoted name of the timestamp column. Default: \code{timestamp}.
#' @return The input data frame with an additional column \code{min_3h_pressure}
#'   (numeric, hPa) populated only on rows where \code{sensor_type == "location"}.
#' @seealso \code{\link{pressure_to_altitude_m}}, \code{\link{add_altitude_from_pressure}}
add_min_pressure_to_locations <- function(df,
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
              "sensor_type", "barometric_pressure")
  missing <- setdiff(needed, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }

  # Drop existing outputs to avoid join suffix issues
  df2 <- df %>%
    dplyr::select(-any_of(c("min.3h.pressure", "min_3h_pressure_join")))

  # 1) Extract min barometric pressure per (track, time)
  summaries <- df2 %>%
    group_by(!!track_col, !!time_col) %>%
    reframe(
      min_3h_pressure_join = {
        p <- barometric_pressure[sensor_type == "min.baro.pressure"]
        if (length(p) == 0L || all(is.na(p))) NA_real_
        else min(as.numeric(p), na.rm = TRUE)
      }
    )

  # 2) Join back + keep only on location rows
  out <- df2 %>%
    left_join(
      summaries,
      by = setNames(c(track_name, time_name),
                    c(track_name, time_name))
    ) %>%
    mutate(
      min_3h_pressure = if_else(sensor_type == "location",
                                min_3h_pressure_join,
                                NA_real_)
    ) %>%
    dplyr::select(-min_3h_pressure_join)

  out
}

