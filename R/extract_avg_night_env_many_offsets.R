add_avg_night_to_move2 <- function(m, avg_night_data, row_id = ".row_id") {
  require(sf)
  require(dplyr)

  stopifnot(inherits(m, "move2"))
  stopifnot(row_id %in% names(avg_night_data))

  # ensure row id exists on m
  if (!(row_id %in% names(sf::st_drop_geometry(m)))) {
    m[[row_id]] <- seq_len(nrow(m))
  }

  # keep only row_id + columns that are not already in m (optional but nice)
  existing <- names(sf::st_drop_geometry(m))
  add_cols <- setdiff(names(avg_night_data), existing)
  join_df  <- avg_night_data[, c(row_id, add_cols), drop = FALSE]

  # join onto attributes (data.frame), then write columns back safely
  attr_df2 <- sf::st_drop_geometry(m) %>%
    dplyr::left_join(join_df, by = row_id)

  m_out <- m
  geom_col <- attr(m_out, "sf_column")
  non_geom <- setdiff(names(attr_df2), geom_col)

  for (nm in non_geom) {
    m_out[[nm]] <- attr_df2[[nm]]
  }

  m_out
}

extract_avg_night_env_many_offsets <- function(
    timestamps,
    latitudes,
    longitudes,
    IDs = NULL,
    offsets_days,
    raster_by_year,
    var_names,
    tz = "UTC",
    coord_crs = "EPSG:4326",
    keep_night_long = FALSE,
    verbose = TRUE
) {
  require(dplyr)

  # stable id per fix (same across offsets)
  base <- data.frame(
    .row_id = seq_along(timestamps),
    timestamp = as.POSIXct(timestamps, tz = tz),
    latitude = latitudes,
    longitude = longitudes,
    ID = if (is.null(IDs)) NA_character_ else as.character(IDs),
    stringsAsFactors = FALSE
  )

  avg_list <- vector("list", length(offsets_days))
  night_long <- if (keep_night_long) vector("list", length(offsets_days)) else NULL

  for (k in seq_along(offsets_days)) {
    d <- offsets_days[k]
    if (verbose) message("Night offset days: ", d)

    res <- extract_avg_night_env_from_year_stacks(
      timestamps    = base$timestamp,
      latitudes     = base$latitude,
      longitudes    = base$longitude,
      IDs           = base$ID,
      shift_hours   = d * 24,
      raster_by_year = raster_by_year,
      var_names     = var_names,
      tz            = tz,
      coord_crs     = coord_crs,
      verbose       = verbose
    )

    # keep ONLY row_id + new env columns (and optionally night_n)
    new_cols <- paste0(var_names, "_", d * 24, "h")
    keep_cols <- c(".row_id", "night_n", new_cols)

    avg_k <- res$average_night_data %>%
      dplyr::select(dplyr::any_of(keep_cols))

    # ensure .row_id is present; if not, rebuild from order
    if (!(".row_id" %in% names(avg_k))) avg_k$.row_id <- base$.row_id

    avg_list[[k]] <- avg_k

    if (keep_night_long) night_long[[k]] <- res$night_data
  }

  # merge wide by row_id only (no giant multi-key full_join)
  avg_wide <- Reduce(function(x, y) dplyr::left_join(x, y, by = ".row_id"), avg_list)

  # optionally carry base metadata (timestamp/coords/ID)
  avg_wide <- dplyr::left_join(base, avg_wide, by = ".row_id")

  list(
    avg_night_wide = avg_wide,
    night_long_list = night_long
  )
}

days <- -2:2

# b <- b_daily #%>%
  # filter(individual_local_identifier == "Nnoc24_swiss_133_120CC37")

# extract all offsets (wide table)
out <- extract_avg_night_env_many_offsets(
  timestamps = b_daily$timestamp,
  latitudes  = b_daily$lat,
  longitudes = b_daily$lon,
  IDs        = b_daily$individual_local_identifier,
  offsets_days = days,
  raster_by_year = raster_by_year,
  var_names = vars,
  keep_night_long = FALSE,
  verbose = TRUE
)

# add to move2 once
bats_daily_env <- add_avg_night_to_move2(
  m = b_daily,
  avg_night_data = out$avg_night_wide,
  row_id = ".row_id"
)

avg_night <- out$avg_night_wide


# calculate wind features
source("./R/calculate_wind_features.R")
bats_daily_wind <- calculate_wind_features(
  data = bats_daily_env,
  u_col_base = "u100",
  v_col_base = "v100",
  distance_col = "distance",
  time_diff_col = "dt_prev",
  bearing_col = "azimuth",
  offsets = -2:2,
  offset_units = "days",
  time_diff_units = "seconds"
)

# bats_daily_wind$airspeed100_0h %>% plot()
save(bats_daily_env, bats_daily_wind, out, avg_night, file = "../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_avg_night_env.robj")
# load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_avg_night_env.robj")
# library(tidyverse)
# library(units)
# ggplot(bats_daily_wind)+
#   geom_point(aes(airspeed100_0h,distance/as.numeric(dt), col = species))
