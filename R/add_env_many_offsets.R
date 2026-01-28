# add_env_many_offsets <- function(
#     m,
#     offsets_days,
#     raster_by_year,
#     var_names,
#     time_round = "hour",
#     tz = "UTC",
#     id_col = ".row_id",
#     coord_crs = "EPSG:4326",
#     verbose = TRUE
# ) {
#   require(dplyr)
#   require(sf)
#
#   # add stable row id once
#   base_df <- sf::st_drop_geometry(m)
#   if (!(id_col %in% names(base_df))) {
#     m[[id_col]] <- seq_len(nrow(base_df))
#   }
#
#   # collect env-only data.frames keyed by row_id
#   env_list <- vector("list", length(offsets_days))
#
#   for (k in seq_along(offsets_days)) {
#     d <- offsets_days[k]
#     if (verbose) message("Offset days: ", d)
#
#     m_tmp <- add_env_to_move2(
#       m = m,
#       raster_by_year = raster_by_year,
#       var_names = var_names,
#       shift_hours = d * 24,
#       time_round = time_round,
#       tz = tz,
#       id_col = id_col,
#       coord_crs = coord_crs,
#       verbose = verbose
#     )
#
#     # keep only newly created env columns + id
#     new_cols <- paste0(var_names, "_", d * 24, "h")
#     env_list[[k]] <- sf::st_drop_geometry(m_tmp) |>
#       dplyr::select(dplyr::all_of(c(id_col, new_cols)))
#   }
#
#   # merge all env columns into one table
#   env_df <- Reduce(function(x, y) dplyr::left_join(x, y, by = id_col), env_list)
#
#   # add merged columns to m once
#   m_out <- m
#   geom_col <- attr(m_out, "sf_column")
#   non_geom <- setdiff(names(env_df), c(geom_col, id_col))
#
#   for (nm in non_geom) {
#     m_out[[nm]] <- env_df[[nm]]
#   }
#
#   m_out
# }

add_env_many_offsets <- function(
    m,
    offsets_days,
    raster_by_year,
    var_names,
    time_round = "hour",
    tz = "UTC",
    id_col = ".row_id",
    coord_crs = "EPSG:4326",
    drop_all_na_cols = TRUE,
    verbose = TRUE
) {
  require(dplyr)
  require(sf)

  stopifnot(inherits(m, "move2"))

  # ensure stable row id once
  if (!(id_col %in% names(sf::st_drop_geometry(m)))) {
    m[[id_col]] <- seq_len(nrow(m))
  }

  env_list <- vector("list", length(offsets_days))

  for (k in seq_along(offsets_days)) {
    d <- offsets_days[k]
    if (verbose) message("Offset days: ", d)

    m_tmp <- add_env_to_move2(
      m = m,
      raster_by_year = raster_by_year,
      var_names = var_names,
      shift_hours = d * 24,
      time_round = time_round,
      tz = tz,
      id_col = id_col,
      coord_crs = coord_crs,
      verbose = verbose
    )

    env_cols <- paste0(var_names, "_", d * 24, "h")

    # keep ONLY id + env cols (prevents timestamp.x / ID.x duplicates)
    env_list[[k]] <- sf::st_drop_geometry(m_tmp) %>%
      dplyr::select(dplyr::all_of(c(id_col, env_cols)))
  }

  # merge wide by id only
  env_df <- Reduce(function(x, y) dplyr::left_join(x, y, by = id_col), env_list)

  # optionally drop env columns that are all NA (common when offset/year not covered)
  if (drop_all_na_cols) {
    keep <- vapply(env_df, function(col) !all(is.na(col)), logical(1))
    keep[[id_col]] <- TRUE
    env_df <- env_df[, keep, drop = FALSE]
  }

  # add to move2 once
  m_out <- m
  geom_col <- attr(m_out, "sf_column")
  for (nm in setdiff(names(env_df), c(geom_col, id_col))) {
    m_out[[nm]] <- env_df[[nm]]
  }

  return(list(m = m_out, env_df = env_df))
}

# raster_by_year <- list(
#   "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
#   "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
#   "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
#   "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
# )
# r <- terra::rast("../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib")
# r
# vars <- c("u10","v10","u100","v100","tp","t2m","msl","i10fg","tcc")
#
# days <- -2:2
# res <- add_env_many_offsets(
#   m = bats_loc, # %>% filter(individual_local_identifier == "Nlei25_freinat_Tier111_272A99C"),
#   offsets_days = days,
#   raster_by_year = raster_by_year,
#   var_names = vars,
#   tz = "UTC",
#   verbose = TRUE
# )
#
# try(m2_env <- res)
#
# # calculate wind features
# source("./R/calculate_wind_features.R")
# m2_wind <- calculate_wind_features(
#     data = m2_env,
#     u_col_base = "u100",
#     v_col_base = "v100",
#     distance_col = "distance",
#     time_diff_col = "dt_prev",
#     bearing_col = "azimuth",
#     offsets = -2:2,
#     offset_units = "days",
#     time_diff_units = "seconds"
#   )
#
# # Year 2024: matched rows = 9252, unique raster hours = 2635; 33%
# save(m2_env, m2_wind, file = "../../../Dropbox/MPI/Noctule/Data/rdata/m2_env.robj")
#
# # do we need to sort weather by time of previous message to correlate with speed?
# library(tidyverse)
# library(units)
# # as.numeric(m2_env$dt) %>% hist(xlim = c(2*3600, 4*3600), breaks = 20000)
# ggplot(m2_env %>% filter(as.numeric(dt) > 2.5*3600, as.numeric(dt) < 3.5 * 3600),
#        aes(distance, u100_0h))+
#   geom_point()
#
# plot(m2_env$distance/m2_env$dt_prev, m2_env$u100_0h)
# plot(m2_env$dist_prev/m2_env$dt_prev, m2_env$altitude_m)
#
# m2_wind$ws100_0h %>% hist()
# ggplot(m2_wind)+
#   geom_histogram(aes(ws100_0h), )+
#   facet_wrap(~species)
