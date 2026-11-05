# Small offline move2 track: one deployment, hourly location fixes.
make_track <- function(n = 3, start = as.POSIXct("2026-04-01 22:00:00", tz = "UTC"),
                       id = "d1", lon0 = 8, lat0 = 47) {
  df <- data.frame(
    deployment_id = id,
    timestamp = start + 3600 * (seq_len(n) - 1),
    sensor_type = "location",
    lon = lon0 + 0.1 * (seq_len(n) - 1),
    lat = lat0
  )
  sfx <- sf::st_as_sf(df, coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  move2::mt_as_move2(sfx, time_column = "timestamp", track_id_column = "deployment_id")
}

set_deploy_info <- function(x, deploy_ts, deploy_loc, capture_loc) {
  td <- move2::mt_track_data(x)
  td$deploy_on_timestamp <- deploy_ts
  td$deploy_on_location <- deploy_loc
  td$capture_location <- capture_loc
  move2::mt_set_track_data(x, td)
}
