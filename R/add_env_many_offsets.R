#' Detect whether a raster_by_year/raster_by_month list is keyed by
#' year ("2024") or by year-month ("2024-01" / "2024_01").
detect_raster_key_mode <- function(raster_by_year) {
  nm <- names(raster_by_year)
  if (is.null(nm) || any(nm == "")) {
    stop("raster_by_year must be a *named* list, e.g. list('2024'='path.grib') ",
         "or list('2024-01'='path.grib').")
  }
  if (all(grepl("^[0-9]{4}$", nm))) return("year")
  if (all(grepl("^[0-9]{4}[-_][0-9]{2}$", nm))) return("month")
  stop("Names of raster_by_year must all be years ('2024') or all be ",
       "year-months ('2024-01'), not a mix.")
}

#' Build a year-month-keyed raster list from a directory of monthly GRIBs,
#' e.g. files named "era5_single_2022_01.grib" -> key "2022-01".
build_raster_by_month <- function(
    dir,
    pattern = "([0-9]{4})_([0-9]{2})\\.grib$"
) {
  files <- list.files(dir, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) stop("No files matching '", pattern, "' found in ", dir)

  m <- regmatches(files, regexec(pattern, files))
  keys <- vapply(m, function(x) paste0(x[2], "-", x[3]), character(1))

  out <- as.list(files)
  names(out) <- keys
  out[order(names(out))]
}

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

  # ---- validate raster_by_year / raster_by_month ----
  key_mode <- detect_raster_key_mode(raster_by_year)

  # ---- adjusted & rounded timestamps ----
  df <- df |>
    dplyr::mutate(
      .time_adj   = as.POSIXct(.data$timestamp, tz = tz) + lubridate::hours(shift_hours),
      .time_round = lubridate::round_date(.time_adj, unit = time_round),
      .key        = if (key_mode == "year") {
        as.character(lubridate::year(.time_round))
      } else {
        format(.time_round, "%Y-%m")
      }
    )

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

  keys_needed <- sort(unique(df$.key))
  if (verbose) message(if (key_mode == "year") "Years in track: " else "Year-months in track: ",
                       paste(keys_needed, collapse = ", "))

  for (yr_chr in keys_needed) {
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping ", yr_chr, " (no raster provided).")
      next
    }

    r <- open_raster(raster_by_year[[yr_chr]])
    n_var <- length(var_names)

    rt <- as.POSIXct(terra::time(r), tz = tz)
    if (is.null(rt)) stop("Raster for ", yr_chr, " has no time vector (terra::time() is NULL).")

    # ---- IMPORTANT: GRIB is stored as blocks of variables per hour ----
    # Derive the actual block size from the data itself (unique timestamps)
    # instead of just nlyr(r) %% n_var, which can pass by coincidence (e.g.
    # 720 hours x 11 real layers = 7920, and 7920 %% 10 == 0 too) even when
    # var_names doesn't match the file's real variable count/order.
    n_unique_times <- length(unique(lubridate::round_date(rt, unit = time_round)))
    actual_block <- terra::nlyr(r) / n_unique_times
    if (actual_block %% 1 != 0 || actual_block != n_var) {
      stop("Raster for ", yr_chr, ": nlyr(r) (", terra::nlyr(r), ") / unique timestamps (",
           n_unique_times, ") = ", actual_block, " layers/timestamp, but var_names has ",
           n_var, " entries. var_names does not match this GRIB's actual variables/order.")
    }

    # Block starts: 1, 1+n_var, 1+2*n_var, ...
    block_start <- seq.int(1L, terra::nlyr(r), by = n_var)

    # One timestamp per hour-block (all layers in the block share this time)
    rt_hour <- lubridate::round_date(rt[block_start], unit = time_round)

    # subset rows for this year / year-month
    idx_rows <- which(df$.key == yr_chr)
    if (length(idx_rows) == 0) next

    t_req <- as.POSIXct(df$.time_round[idx_rows], tz = tz)
    t_req_num <- as.numeric(t_req)
    rt_num <- as.numeric(rt_hour)

    hour_idx <- match(t_req_num, rt_num)

    ok <- which(!is.na(hour_idx))
    if (length(ok) == 0) {
      if (verbose) message("No matching times in raster for ", yr_chr)
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
      message(yr_chr, ": matched rows=", length(sub_rows),
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
    dplyr::select(-.lon, -.lat, -.time_adj, -.time_round, -.key)

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


add_env_many_offsets <- function(
    m,
    offsets_days,
    raster_by_year,
    var_names = c("u10","v10","t2m",
                  "msl","sp","tp",
                  "u100","v100",
                  "i10fg","tcc"),
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

