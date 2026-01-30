add_env_to_move2 <- function(
    m,
    raster_by_year,
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
  require(terra)
  require(dplyr)
  require(lubridate)
  require(sf)

  stopifnot(inherits(m, "move2"))

  # ---- coords + attributes ----
  geom <- sf::st_geometry(m)
  coords <- sf::st_coordinates(geom)
  if (!all(c("X", "Y") %in% colnames(coords))) stop("Could not get X/Y coordinates from geometry.")

  df <- m |>
    sf::st_drop_geometry() |>
    dplyr::mutate(
      !!id_col := dplyr::row_number(),
      .lon = coords[, "X"],
      .lat = coords[, "Y"]
    )

  if (!("timestamp" %in% names(df))) stop("Expected a 'timestamp' column on the move2 object.")

  # ---- adjusted & rounded timestamps ----
  df <- df |>
    dplyr::mutate(
      .time_adj   = as.POSIXct(.data$timestamp, tz = tz) + lubridate::hours(shift_hours),
      .time_round = lubridate::round_date(.time_adj, unit = time_round),
      .year       = lubridate::year(.time_round)
    )

  # ---- validate raster_by_year ----
  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be a *named* list with names equal to years, e.g. list('2024'='path.grib').")
  }

  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }

  # ---- infer var names once if needed ----
  if (is.null(var_names)) {
    r0 <- open_raster(raster_by_year[[1]])
    stop("Please provide var_names for GRIB stacks (recommended).")
  }

  n_var <- length(var_names)
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

    # ---- IMPORTANT: GRIB is stored as blocks of variables per hour ----
    # ---- IMPORTANT: GRIB is stored as blocks of variables per hour ----
    n_var <- length(var_names)

    if (terra::nlyr(r) %% n_var != 0) {
      stop("nlyr(r) (", terra::nlyr(r), ") is not divisible by n_var (", n_var,
           "). var_names does not match GRIB layers-per-hour.")
    }

    rt <- as.POSIXct(terra::time(r), tz = tz)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no time vector (terra::time() is NULL).")

    # Block starts: 1, 1+n_var, 1+2*n_var, ...
    block_start <- seq.int(1L, terra::nlyr(r), by = n_var)

    # One timestamp per hour-block (all layers in the block share this time)
    rt_hour <- lubridate::round_date(rt[block_start], unit = time_round)

    # subset rows for this year
    idx_rows <- which(df$.year == yr)
    if (length(idx_rows) == 0) next

    t_req <- as.POSIXct(df$.time_round[idx_rows], tz = tz)
    t_req_num <- as.numeric(t_req)
    rt_num <- as.numeric(rt_hour)

    hour_idx <- match(t_req_num, rt_num)

    ok <- which(!is.na(hour_idx))
    if (length(ok) == 0) {
      if (verbose) message("No matching times in raster for year ", yr_chr)
      next
    }

    sub_rows <- idx_rows[ok]
    hour_idx <- hour_idx[ok]
    uniq_hours <- sort(unique(hour_idx))

    # points (already made above in your function) should correspond to sub_rows
    pts <- terra::vect(
      data.frame(x = df$.lon[sub_rows], y = df$.lat[sub_rows]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) {
      pts <- terra::project(pts, r_crs)
    }

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
      if (length(p_idx) == 0) next

      # ---- KEY: slice the FULL variable block for this hour ----
      start_layer <- block_start[h]
      layer_idx <- start_layer:(start_layer + n_var - 1L)

      rr <- r[[layer_idx]]
      names(rr) <- var_names

      vals <- terra::extract(rr, pts[p_idx], ID = FALSE)

      # Guard against silent recycling (THIS is what caused "all columns equal")
      if (ncol(vals) != n_var) {
        stop("Extraction returned ", ncol(vals), " columns, expected ", n_var,
             ". Likely wrong layer indexing or GRIB structure.")
      }

      # write back into df
      target_rows <- sub_rows[p_idx]
      df[target_rows, paste0(var_names, "_", shift_hours, "h")] <- as.matrix(vals)
    }
  }


  # ---- attach back to move2 (no st_drop_geometry<-) ----
  df_out <- df |>
    dplyr::select(-.lon, -.lat, -.time_adj, -.time_round, -.year)

  m_out <- m
  geom_col <- attr(m_out, "sf_column")

  for (nm in setdiff(names(df_out), geom_col)) {
    m_out[[nm]] <- df_out[[nm]]
  }

  m_out
}

# r <- terra::rast("../../../Dropbox/MPI/Noctule/Data/ECMWF/2025/ERA_2025.grib")
# n_var <- 10
# rt <- as.POSIXct(terra::time(r), tz="UTC")
#
# cbind(
#   layer = 1:(n_var*3),
#   time  = rt[1:(n_var*3)]
# )
