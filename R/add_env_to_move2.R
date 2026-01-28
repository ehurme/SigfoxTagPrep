
add_env_to_move2 <- function(
    m,
    raster_by_year,
    var_names = NULL,
    shift_hours = 0,
    time_round = "hour",          # "hour" is typical for ERA5 hourly
    tz = "UTC",
    id_col = ".row_id",
    coord_crs = "EPSG:4326",      # coords are almost always lon/lat
    verbose = TRUE
) {
  # ---- deps inside function ----
  require(move2)
  require(terra)
  require(dplyr)
  require(lubridate)
  require(sf)

  stopifnot(inherits(m, "move2"))

  # ---- pull coordinates + time from move2 ----
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

  if (!("timestamp" %in% names(df))) {
    stop("Expected a 'timestamp' column on the move2 object.")
  }

  # ---- adjusted & rounded timestamps ----
  df <- df |>
    dplyr::mutate(
      .time_adj = as.POSIXct(.data$timestamp, tz = tz) + lubridate::hours(shift_hours),
      .time_round = lubridate::round_date(.time_adj, unit = time_round),
      .year = lubridate::year(.time_round)
    )

  # ---- validate raster_by_year ----
  # raster_by_year can be:
  # 1) named list of SpatRaster
  # 2) named list of file paths to SpatRaster sources (recommended)
  if (is.null(names(raster_by_year))) {
    stop("raster_by_year must be a *named* list, with names equal to years, e.g. list('2024'='path.tif').")
  }

  # helper to open raster lazily (path -> rast; raster -> itself)
  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }

  # ---- preallocate output columns ----
  # We create NA columns now so we can fill per-year and bind safely
  if (is.null(var_names)) {
    # infer var names from the first year's raster
    r0 <- open_raster(raster_by_year[[1]])
    # terra names may include time-layer names; prefer sources varnames if present
    vn0 <- tryCatch(terra::varnames(r0), error = function(e) NULL)
    var_names <- if (!is.null(vn0) && length(vn0) == terra::nlyr(r0)) vn0 else names(r0)
  }

  out_names <- paste0(var_names, "_", shift_hours, "h")
  for (nm in out_names) df[[nm]] <- NA_real_

  # ---- process year-by-year ----
  years_needed <- sort(unique(df$.year))
  if (verbose) message("Years in track: ", paste(years_needed, collapse = ", "))

  for (yr in years_needed) {
    yr_chr <- as.character(yr)
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping year ", yr_chr, " (no raster provided).")
      next
    }

    r <- open_raster(raster_by_year[[yr_chr]])

    # ensure raster has time
    rt <- terra::time(r)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no time vector (terra::time() is NULL).")

    # subset rows for this year
    idx_rows <- which(df$.year == yr)
    if (length(idx_rows) == 0) next

    # match times to raster time vector
    t_num <- as.numeric(df$.time_round[idx_rows])
    rt_num <- as.numeric(rt)
    idx_match <- match(t_num, rt_num)

    ok <- which(!is.na(idx_match))
    if (length(ok) == 0) {
      if (verbose) message("No matching times in raster for year ", yr_chr)
      next
    }

    # build points vect once for this year subset (only rows that matched)
    sub_rows <- idx_rows[ok]
    pts <- terra::vect(
      data.frame(x = df$.lon[sub_rows], y = df$.lat[sub_rows]),
      geom = c("x", "y"),
      crs = coord_crs
    )

    # reproject points to raster CRS if needed
    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs) {
      pts <- terra::project(pts, r_crs)
    }

    # unique time indices to avoid repeated slicing
    time_idx <- idx_match[ok]
    uniq_time_idx <- sort(unique(time_idx))

    if (verbose) {
      message("Year ", yr_chr, ": matched rows = ", length(sub_rows),
              ", unique raster hours = ", length(uniq_time_idx))
      pb <- utils::txtProgressBar(min = 0, max = length(uniq_time_idx), style = 3)
      on.exit(close(pb), add = TRUE)
    }

    for (i in seq_along(uniq_time_idx)) {
      if (verbose) utils::setTxtProgressBar(pb, i)
      ti <- uniq_time_idx[i]

      # which points correspond to this time slice
      p_idx <- which(time_idx == ti)
      if (length(p_idx) == 0) next

      # ---- the memory-safe step ----
      # slice ONLY the needed time layer(s) from each variable (ti)
      # Avoid rast(map(...)) across all time slices
      rr <- r[[ti]]

      # set names consistently
      if (!is.null(var_names) && length(var_names) == terra::nlyr(rr)) {
        names(rr) <- var_names
      }

      # extract for just those points
      vals <- terra::extract(rr, pts[p_idx], ID = FALSE)

      # write into df at the correct original rows
      target_rows <- sub_rows[p_idx]
      df[target_rows, out_names] <- as.matrix(vals)
    }
  }

  # ---- return: move2 with new columns ----
  # keep original order; drop helper cols
  df_out <- df |>
    dplyr::select(-.lon, -.lat, -.time_adj, -.time_round, -.year)

  # # reattach geometry to preserve move2 class
  # m_out <- m
  # sf::st_drop_geometry(m_out) <- df_out
  # return(m_out)

  # ---- return: move2 with new columns (safe; no st_drop_geometry<-) ----
  m_out <- m
  geom_col <- attr(m_out, "sf_column")
  non_geom <- setdiff(names(df_out), geom_col)

  for (nm in non_geom) {
    m_out[[nm]] <- df_out[[nm]]
  }

  return(m_out)

}



