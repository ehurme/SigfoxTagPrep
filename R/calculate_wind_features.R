################# Wind Support Functions

# Function: Calculate Wind Direction
wind_dir <- function(u_ms, v_ms) {
  wind_abs <- sqrt(u_ms^2 + v_ms^2)
  wind_dir_trig_to <- atan2(u_ms / wind_abs, v_ms / wind_abs) * (180 / pi)
  wind_dir_trig_from <- (wind_dir_trig_to + 180) %% 360
  wind_dir_cardinal <- (90 - wind_dir_trig_from + 360) %% 360

  return(wind_dir_cardinal)
}

# Function: Calculate Bearing
calculate_bearing <- function(lon1, lat1, lon2, lat2) {
  lat1_rad <- lat1 * (pi / 180)
  lat2_rad <- lat2 * (pi / 180)
  delta_lon <- (lon2 - lon1) * (pi / 180)

  bearing <- atan2(
    sin(delta_lon) * cos(lat2_rad),
    cos(lat1_rad) * sin(lat2_rad) - sin(lat1_rad) * cos(lat2_rad) * cos(delta_lon)
  ) * (180 / pi)

  return((bearing + 360) %% 360)
}

# Function: Calculate Wind Support
wind_support <- function(u, v, heading) {
  angle <- atan2(u, v) - heading * (pi / 180)
  return(cos(angle) * sqrt(u^2 + v^2))
}

# Function: Calculate Cross Wind
cross_wind <- function(u, v, heading) {
  angle <- atan2(u, v) - heading * (pi / 180)
  return(sin(angle) * sqrt(u^2 + v^2))
}

# Function: Calculate Airspeed
airspeed <- function(ground_speed, ws, cw) {
  return(sqrt((ground_speed - ws)^2 + cw^2))
}

suffix = "h"
env_name <- function(base, off_h) paste0(base, "_", off_h, suffix)

calculate_wind_features <- function(
    data,
    u_col_base, v_col_base,
    windsp_col_base = NULL,
    winddir_col_base = NULL,
    distance_col, time_diff_col, bearing_col,
    offsets,                     # offsets in HOURS by default (e.g., -48, 0, 48)
    offset_units = c("hours", "days"),
    time_diff_units = c("minutes", "seconds"),
    suffix = "h",
    quiet = FALSE
) {
  require(stringr)

  offset_units <- match.arg(offset_units)
  time_diff_units <- match.arg(time_diff_units)

  # offsets can be days or hours; convert to integer hours
  offsets_h <- if (offset_units == "days") as.integer(offsets * 24) else as.integer(offsets)

  # infer elevation from base name like u10/u100
  elevation <- str_extract(v_col_base, "[0-9]+$") %||% ""

  # sensible default output bases if not given
  if (is.null(windsp_col_base))  windsp_col_base  <- paste0("windsp", elevation)
  if (is.null(winddir_col_base)) winddir_col_base <- paste0("winddir", elevation)

  # helper to build env column names
  env_name <- function(base, off_h) paste0(base, "_", off_h, suffix)

  # compute ground speed once (vector)
  dt <- data[[time_diff_col]]
  if (time_diff_units == "minutes") dt_sec <- dt * 60
  if (time_diff_units == "seconds") dt_sec <- dt
  ground_speed <- data[[distance_col]] / dt_sec

  # loop offsets
  for (off_h in offsets_h) {
    u_name <- env_name(u_col_base, off_h)
    v_name <- env_name(v_col_base, off_h)

    # skip if missing
    if (!(u_name %in% names(data)) || !(v_name %in% names(data))) {
      if (!quiet) message("Skipping offset ", off_h, "h (missing: ",
                          paste(setdiff(c(u_name, v_name), names(data)), collapse = ", "), ")")
      next
    }

    windsp_name  <- env_name(windsp_col_base, off_h)
    winddir_name <- env_name(winddir_col_base, off_h)

    ws_name       <- env_name(paste0("ws", elevation), off_h)
    cw_name       <- env_name(paste0("cw", elevation), off_h)
    airspeed_name <- env_name(paste0("airspeed", elevation), off_h)

    u <- data[[u_name]]
    v <- data[[v_name]]

    # wind speed and direction
    data[[windsp_name]]  <- sqrt(u^2 + v^2)
    data[[winddir_name]] <- wind_dir(u_ms = u, v_ms = v)

    # wind support and crosswind (heading = bearing_col)
    heading <- data[[bearing_col]] %>% as.numeric()
    data[[ws_name]] <- wind_support(u = u, v = v, heading = heading)
    data[[cw_name]] <- cross_wind(u = u, v = v, heading = heading)

    # airspeed
    data[[airspeed_name]] <- airspeed(
      ground_speed = ground_speed %>% as.numeric(),
      ws = data[[ws_name]],
      cw = data[[cw_name]]
    )
  }

  data
}

# small infix helper (keeps function self-contained)
`%||%` <- function(a, b) if (!is.null(a) && length(a) && !is.na(a)) a else b

# n_env <- calculate_wind_features(
#   data = n_env,
#   u_col_base = "u100",
#   v_col_base = "v100",
#   distance_col = "distance",
#   time_diff_col = "diff_time",
#   bearing_col = "bearing",
#   offsets = -2:2,
#   offset_units = "days",
#   time_diff_units = "minutes"
# )
