add_env_to_move2_fast <- function(
    m,
    raster_by_year = list(
      "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
      "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
      "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
      "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
    ),
    var_names = c("u10","v10","t2m",
                "msl","sp","tp",
                "u100","v100",
                "i10fg","tcc"),
    shift_hours = 0,
    time_round = "hour",
    tz = "UTC",
    id_col = ".row_id",
    coord_crs = "EPSG:4326",
    verbose = TRUE
) {
  require(move2)
  require(sf)
  require(dplyr)
  require(lubridate)
  require(terra)

  stopifnot(inherits(m, "move2"))
  if (length(var_names) < 1) stop("var_names must be provided and in GRIB order.")

  # ---- stable row id once ----
  if (!(id_col %in% names(sf::st_drop_geometry(m)))) {
    m[[id_col]] <- seq_len(nrow(m))
  }

  # ---- coords + time ----
  geom <- sf::st_geometry(m)
  coords <- sf::st_coordinates(geom)
  if (!all(c("X", "Y") %in% colnames(coords))) stop("Could not get X/Y coordinates from geometry.")
  if (!("timestamp" %in% names(m))) stop("Expected a 'timestamp' column on the move2 object.")

  df <- sf::st_drop_geometry(m) |>
    mutate(
      .lon = coords[, "X"],
      .lat = coords[, "Y"],
      .time_adj   = as.POSIXct(.data$timestamp, tz = tz) + hours(shift_hours),
      .time_round = round_date(.time_adj, unit = time_round),
      .year       = year(.time_round)
    )

  # ---- open raster lazily ----
  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }
  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be a named list with names like '2024', '2025', ...")
  }

  out_names <- paste0(var_names, "_", shift_hours, "h")
  for (nm in out_names) df[[nm]] <- NA_real_

  years_needed <- sort(unique(df$.year))
  if (verbose) message("Years in track: ", paste(years_needed, collapse = ", "))

  for (yr in years_needed) {
    yr_chr <- as.character(yr)
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping year ", yr_chr, " (no raster provided).")
      next
    }

    r <- open_raster(raster_by_year[[yr_chr]])

    n_var <- length(var_names)
    if (terra::nlyr(r) %% n_var != 0) {
      stop("Year ", yr_chr, ": nlyr(r) (", terra::nlyr(r),
           ") not divisible by n_var (", n_var, "). var_names/order mismatch.")
    }

    rt <- as.POSIXct(terra::time(r), tz = tz)
    if (is.null(rt)) stop("Year ", yr_chr, ": terra::time(r) is NULL.")

    block_start <- seq.int(1L, terra::nlyr(r), by = n_var)
    rt_hour <- lubridate::round_date(rt[block_start], unit = time_round)

    idx_rows <- which(df$.year == yr)
    if (!length(idx_rows)) next

    t_req <- as.POSIXct(df$.time_round[idx_rows], tz = tz)
    hour_idx <- match(as.numeric(t_req), as.numeric(rt_hour))

    ok <- which(!is.na(hour_idx))
    if (!length(ok)) {
      if (verbose) message("No matching times in raster for year ", yr_chr)
      next
    }

    sub_rows <- idx_rows[ok]
    hour_idx <- hour_idx[ok]

    # points for all matched rows (one per fix)
    pts_all <- terra::vect(
      data.frame(x = df$.lon[sub_rows], y = df$.lat[sub_rows]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts_all, proj = TRUE) != r_crs) {
      pts_all <- terra::project(pts_all, r_crs)
    }

    # vectorized over unique hours: still loops, but tight + correct + minimal I/O
    uniq_hours <- sort(unique(hour_idx))

    if (verbose) {
      message("Year ", yr_chr, ": matched rows=", length(sub_rows),
              ", unique hours=", length(uniq_hours), ", vars/hour=", n_var)
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_hours), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    for (i in seq_along(uniq_hours)) {
      if (verbose) utils::setTxtProgressBar(pb, i)
      h <- uniq_hours[i]
      p_idx <- which(hour_idx == h)
      if (!length(p_idx)) next

      # slice full variable block for this hour
      start_layer <- block_start[h]
      layer_idx <- start_layer:(start_layer + n_var - 1L)

      rr <- r[[layer_idx]]
      names(rr) <- var_names

      vals <- terra::extract(rr, pts_all[p_idx], ID = FALSE)
      if (ncol(vals) != n_var) {
        stop("Year ", yr_chr, ": extraction returned ", ncol(vals),
             " cols, expected ", n_var, ". Wrong block indexing.")
      }

      df[sub_rows[p_idx], out_names] <- as.matrix(vals)
    }
  }

  # attach only the new columns back to move2 (no st_drop_geometry<-)
  m_out <- m
  for (nm in out_names) m_out[[nm]] <- df[[nm]]

  m_out
}

add_env_many_offsets_fast <- function(
    m,
    offsets_days,
    raster_by_year = list(
      "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
      "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
      "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
      "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
    ),
    var_names = c("u10","v10","t2m",
                  "msl","sp","tp",
                  "u100","v100",
                  "i10fg","tcc"),
    time_round = "hour",
    tz = "UTC",
    id_col = ".row_id",
    coord_crs = "EPSG:4326",
    drop_all_na_cols = TRUE,
    parallel = FALSE,
    workers = max(1, parallel::detectCores() - 30),
    verbose = TRUE
) {
  require(sf)
  require(dplyr)

  stopifnot(inherits(m, "move2"))

  # ensure stable row id once
  if (!(id_col %in% names(sf::st_drop_geometry(m)))) {
    m[[id_col]] <- seq_len(nrow(m))
  }

  # helper to compute env df for one offset (id + env cols only)
  one_offset <- function(d) {
    if (verbose) message("Offset days: ", d)
    m_tmp <- add_env_to_move2_fast(
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

    sf::st_drop_geometry(m_tmp) |>
      dplyr::select(dplyr::all_of(c(id_col, env_cols)))
  }

  if (parallel && length(offsets_days) > 1) {
    require(parallel)
    require(parallelly)

    # PSOCK works on Windows; exports function + globals
    cl <- parallel::makeCluster(workers)
    on.exit(parallel::stopCluster(cl), add = TRUE)

    # export needed symbols
    parallel::clusterExport(
      cl,
      varlist = c("m", "raster_by_year", "var_names", "time_round", "tz", "id_col",
                  "coord_crs", "verbose", "add_env_to_move2_fast", "one_offset"),
      envir = environment()
    )
    parallel::clusterEvalQ(cl, { library(sf); library(dplyr); library(lubridate); library(terra); library(move2) })

    env_list <- parallel::parLapply(cl, offsets_days, one_offset)
  } else {
    env_list <- lapply(offsets_days, one_offset)
  }

  # merge wide by id only (prevents timestamp.x/ID.x duplication)
  env_df <- Reduce(function(x, y) dplyr::left_join(x, y, by = id_col), env_list)

  # clean row_id NAs + duplicates (defensive)
  env_df <- env_df |>
    dplyr::filter(!is.na(.data[[id_col]])) |>
    dplyr::group_by(.data[[id_col]]) |>
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x)), .groups = "drop")

  # optionally drop env columns that are all NA
  if (drop_all_na_cols) {
    keep <- vapply(env_df, function(col) !all(is.na(col)), logical(1))
    keep[[id_col]] <- TRUE
    env_df <- env_df[, keep, drop = FALSE]
  }

  # add to move2 once
  m_out <- m
  new_cols <- setdiff(names(env_df), id_col)
  for (nm in new_cols) m_out[[nm]] <- env_df[[nm]][match(m[[id_col]], env_df[[id_col]])]

  list(m = m_out, env_df = env_df)
}

library(tidyverse)
library(move2)
library(terra)
library(sf)
library(units)
load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")

raster_by_year <- list(
  "2022" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2022/ERA_2022.grib",
  "2023" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2023/ERA_2023.grib",
  "2024" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2024/ERA_2024.grib",
  "2025" = "../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib"
)
vars = c("u10","v10","t2m",
         "msl","sp","tp",
         "u100","v100",
         "i10fg","tcc")
days <- -2:2
bats_loc$individual_local_identifier %>% table()

start <- Sys.time()
res <- add_env_many_offsets_fast(
  m = bats_loc %>% dplyr::filter(individual_local_identifier == "Nnoc24_swiss_170_120E5FA"),
  offsets_days = days,
  raster_by_year = raster_by_year,
  var_names = vars,
  parallel = FALSE,   # set TRUE if you want (Windows-safe)
  verbose = TRUE
)
end <- Sys.time()

m2_env <- res$m
