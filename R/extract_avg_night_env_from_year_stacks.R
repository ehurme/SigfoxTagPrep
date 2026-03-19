# =============================================================================
# extract_avg_night_env.R  — optimised version
#
# Key changes from original:
#
#  1. extract_nighttime_hours_24h()
#       - getSunlightTimes() is now called ONCE per unique (date, lat, lon)
#         combination instead of twice per fix in a for-loop.
#         For a typical deployment with many fixes sharing dates/locations
#         this can reduce suncalc calls by 80-90%.
#       - The per-fix hour-sequence building is done with vectorised
#         lapply() rather than a for-loop with repeated object allocation.
#
#  2. extract_avg_night_env_from_year_stacks()
#       - terra::options(threads = n_threads) activates terra's own C++
#         thread pool for extract(). This is the safe, Windows-compatible
#         way to get parallelism out of terra — no forking, no socket
#         serialization of SpatRaster objects.
#       - All unique-hour raster layers are extracted in ONE batched
#         terra::extract() call per year rather than one call per hour.
#         This avoids repeated SpatRaster subsetting overhead and
#         dramatically reduces the number of C/GDAL round-trips.
#       - Point reprojection is done once per year rather than inside
#         the per-hour loop.
#       - Long-table construction uses data.table::rbindlist() when
#         available (much faster than dplyr::bind_rows for wide tables),
#         with a silent fallback to bind_rows.
#
#  3. Windows parallelism note:
#       terra's thread pool (option 2 above) is the recommended approach.
#       R-level parallelism (parallel::makeCluster / future) fails because
#       SpatRaster objects can't be serialised across PSOCK workers without
#       re-opening the file on each worker — use n_threads instead.
# =============================================================================


# -----------------------------------------------------------------------------
# Helper: vectorised nighttime-hour computation
# -----------------------------------------------------------------------------
extract_nighttime_hours_24h <- function(timestamps, latitudes, longitudes,
                                        tz = "UTC") {
  requireNamespace("lubridate", quietly = TRUE)
  requireNamespace("suncalc",   quietly = TRUE)

  stopifnot(
    length(timestamps)  == length(latitudes),
    length(latitudes)   == length(longitudes)
  )

  n   <- length(timestamps)
  ts  <- as.POSIXct(timestamps, tz = tz)
  dt  <- as.Date(ts, tz = tz)
  dt1 <- dt - 1L   # yesterday

  # ------------------------------------------------------------------
  # Build one lookup table of unique (date, lat, lon) combos and call
  # getSunlightTimes() vectorised — one call covers today + yesterday
  # for all unique locations at once.
  # ------------------------------------------------------------------
  combos_today <- unique(data.frame(date = dt,  lat = latitudes, lon = longitudes))
  combos_yday  <- unique(data.frame(date = dt1, lat = latitudes, lon = longitudes))
  all_combos   <- unique(rbind(combos_today, combos_yday))

  sun_all <- suncalc::getSunlightTimes(
    data = all_combos,
    tz   = tz,
    keep = c("sunrise", "sunset")
  )
  # key for fast lookup
  sun_all$.key <- paste(sun_all$date, sun_all$lat, sun_all$lon, sep = "|")

  # helper to pull a field from the lookup by key
  .lookup <- function(d, lat, lon, field) {
    k   <- paste(d, lat, lon, sep = "|")
    idx <- match(k, sun_all$.key)
    sun_all[[field]][idx]
  }

  # ------------------------------------------------------------------
  # Per-fix: compute nighttime hour sequences using the cached sun times
  # ------------------------------------------------------------------
  lapply(seq_len(n), function(i) {
    t0    <- ts[i]
    start <- t0 - lubridate::hours(24)

    sr_today <- .lookup(dt[i],  latitudes[i], longitudes[i], "sunrise")
    ss_today <- .lookup(dt[i],  latitudes[i], longitudes[i], "sunset")
    ss_yday  <- .lookup(dt1[i], latitudes[i], longitudes[i], "sunset")

    night1_start <- max(ss_yday,  start, na.rm = FALSE)
    night1_end   <- min(sr_today, t0,    na.rm = FALSE)

    night2_start <- max(ss_today, start, na.rm = FALSE)
    night2_end   <- t0

    hours_seq <- seq(from = start, to = t0, by = "hour")
    keep <- rep(FALSE, length(hours_seq))

    if (!is.na(night1_start) && !is.na(night1_end) && night1_start < night1_end)
      keep <- keep | (hours_seq >= night1_start & hours_seq <= night1_end)

    if (!is.na(night2_start) && !is.na(night2_end) && night2_start < night2_end)
      keep <- keep | (hours_seq >= night2_start & hours_seq <= night2_end)

    lubridate::round_date(hours_seq[keep], unit = "hour")
  })
}


# -----------------------------------------------------------------------------
# Main extraction function
# -----------------------------------------------------------------------------
extract_avg_night_env_from_year_stacks <- function(
    timestamps,
    latitudes,
    longitudes,
    IDs        = NULL,
    shift_hours = 0,
    raster_by_year,
    var_names  = NULL,
    tz         = "UTC",
    coord_crs  = "EPSG:4326",
    n_threads  = parallel::detectCores(logical = FALSE),  # terra thread pool
    verbose    = TRUE
) {
  requireNamespace("terra",     quietly = TRUE)
  requireNamespace("lubridate", quietly = TRUE)
  requireNamespace("dplyr",     quietly = TRUE)

  # activate terra's own C++ thread pool — safe on Windows, no serialisation
  old_threads <- terra::terraOptions(print = FALSE)$threads
  terra::terraOptions(threads = max(1L, as.integer(n_threads)))
  on.exit(terra::terraOptions(threads = old_threads), add = TRUE)

  n <- length(timestamps)
  stopifnot(length(latitudes) == n, length(longitudes) == n)
  if (!is.null(IDs)) stopifnot(length(IDs) == n)

  # stable row id for joining
  df <- data.frame(
    .row_id    = seq_len(n),
    timestamp  = as.POSIXct(timestamps, tz = tz),
    latitude   = latitudes,
    longitude  = longitudes,
    ID         = if (is.null(IDs)) NA_character_ else as.character(IDs),
    stringsAsFactors = FALSE
  )

  df$.time_adj   <- df$timestamp + lubridate::hours(shift_hours)
  df$.time_round <- lubridate::round_date(df$.time_adj, "hour")
  df$.year       <- lubridate::year(df$.time_round)

  # lazy raster opener
  open_raster <- function(x) {
    if (inherits(x, "SpatRaster")) return(x)
    if (is.character(x) && length(x) == 1) return(terra::rast(x))
    stop("Each raster_by_year entry must be a SpatRaster or a single file path.")
  }
  if (is.null(names(raster_by_year)))
    stop("raster_by_year must be named with years, e.g. list('2024' = path_or_raster).")

  # infer var names if needed
  if (is.null(var_names)) {
    r0     <- open_raster(raster_by_year[[1]])
    vn0    <- tryCatch(terra::varnames(r0), error = function(e) NULL)
    var_names <- if (!is.null(vn0) && length(vn0) == terra::nlyr(r0)) vn0 else names(r0)
  }
  env_cols <- paste0(var_names, "_", shift_hours, "h")

  # ------------------------------------------------------------------
  # Step 1: compute nighttime hour sequences (vectorised suncalc calls)
  # ------------------------------------------------------------------
  if (verbose) message("Computing night-hour sequences per fix...")
  night_hours <- extract_nighttime_hours_24h(
    df$.time_adj, df$latitude, df$longitude, tz = tz
  )

  # ------------------------------------------------------------------
  # Step 2: expand to long table
  # Fast path uses data.table::rbindlist if available; fallback to
  # dplyr::bind_rows.
  # ------------------------------------------------------------------
  if (verbose) message("Expanding to fix-hour table...")

  rows <- lapply(seq_len(n), function(i) {
    nh <- night_hours[[i]]
    if (length(nh) == 0L) return(NULL)
    data.frame(
      .row_id    = df$.row_id[i],
      night_hour = nh,
      latitude   = df$latitude[i],
      longitude  = df$longitude[i],
      ID         = df$ID[i],
      year       = lubridate::year(nh[[1L]]),
      stringsAsFactors = FALSE
    )
  })

  if (requireNamespace("data.table", quietly = TRUE)) {
    long_tbl <- as.data.frame(data.table::rbindlist(rows))
  } else {
    long_tbl <- dplyr::bind_rows(rows)
  }
  rm(rows)

  if (nrow(long_tbl) == 0L) {
    if (verbose) message("No night hours found for any fix.")
    return(list(night_data = NULL, average_night_data = NULL))
  }

  # preallocate env columns
  for (nm in env_cols) long_tbl[[nm]] <- NA_real_

  # ------------------------------------------------------------------
  # Step 3: batched terra::extract() — ONE call per year, not per hour
  #
  # Strategy:
  #   a) For each year, build a *single* points SpatVector covering all
  #      (row, hour) combos for that year.
  #   b) Subset the raster to all required layers at once.
  #   c) Call terra::extract() once — terra internally parallelises
  #      across its thread pool.
  #   d) Index results back into long_tbl using the pre-computed
  #      (hour_idx → layer block) mapping.
  # ------------------------------------------------------------------
  years_needed <- sort(unique(long_tbl$year))
  if (verbose) message("Years in night-hour table: ", paste(years_needed, collapse = ", "))

  for (yr in years_needed) {
    yr_chr  <- as.character(yr)
    if (!yr_chr %in% names(raster_by_year)) {
      if (verbose) message("Skipping year ", yr_chr, " (no raster provided).")
      next
    }

    r       <- open_raster(raster_by_year[[yr_chr]])
    sub_idx <- which(long_tbl$year == yr)
    if (length(sub_idx) == 0L) next

    # GRIB layout: layers = (time × variable), time repeats per variable
    rt        <- as.POSIXct(terra::time(r), tz = tz)
    if (is.null(rt)) stop("Raster for year ", yr_chr, " has no time vector.")

    n_var     <- length(var_names)
    if (terra::nlyr(r) %% n_var != 0L)
      stop("nlyr(r) (", terra::nlyr(r), ") not divisible by n_var (", n_var, ").")

    rt_unique       <- unique(rt)
    rt_unique_round <- lubridate::round_date(rt_unique, "hour")

    t_round  <- lubridate::round_date(
      as.POSIXct(long_tbl$night_hour[sub_idx], tz = tz), "hour"
    )
    hour_idx <- match(as.numeric(t_round), as.numeric(rt_unique_round))

    ok       <- which(!is.na(hour_idx))
    if (length(ok) == 0L) {
      if (verbose) message("No matching raster hours for year ", yr_chr)
      next
    }

    sub_ok      <- sub_idx[ok]
    hour_idx_ok <- hour_idx[ok]
    uniq_hours  <- sort(unique(hour_idx_ok))

    # build all required layer indices up front
    layer_blocks <- lapply(uniq_hours, function(hidx)
      ((hidx - 1L) * n_var + 1L) : (hidx * n_var)
    )
    all_layers <- unlist(layer_blocks)   # e.g. c(1:4, 5:8, 9:12, ...)

    if (verbose) {
      message("Year ", yr_chr, ": rows=", length(sub_ok),
              ", unique hours=", length(uniq_hours),
              ", total layers=", length(all_layers),
              ", vars/hour=", n_var,
              ", terra threads=", terra::terraOptions(print = FALSE)$threads)
    }

    # --- project points once per year ---
    pts <- terra::vect(
      data.frame(x = long_tbl$longitude[sub_ok], y = long_tbl$latitude[sub_ok]),
      geom = c("x", "y"),
      crs  = coord_crs
    )
    r_crs <- terra::crs(r, proj = TRUE)
    if (!is.na(r_crs) && terra::crs(pts, proj = TRUE) != r_crs)
      pts <- terra::project(pts, r_crs)

    # --- subset raster to all needed layers and extract in one shot ---
    r_sub  <- r[[all_layers]]
    vals   <- terra::extract(r_sub, pts, ID = FALSE)   # nrow = length(sub_ok)

    # vals columns are ordered: [var1_h1, var2_h1, ..., varN_h1,
    #                             var1_h2, var2_h2, ..., varN_h2, ...]
    # Map each row back to its specific hour block
    hour_block_idx <- match(hour_idx_ok, uniq_hours)   # 1-based index into layer_blocks

    # Build a matrix of final values: one row per sub_ok point, one col per var
    out_mat <- matrix(NA_real_, nrow = length(sub_ok), ncol = n_var)
    for (v in seq_len(n_var)) {
      # column in vals for variable v at each point's hour block
      col_in_vals <- (hour_block_idx - 1L) * n_var + v
      out_mat[, v] <- vals[cbind(seq_along(sub_ok), col_in_vals)]
    }

    long_tbl[sub_ok, env_cols] <- out_mat
  }

  # ------------------------------------------------------------------
  # Step 4: aggregate back to one row per fix
  # ------------------------------------------------------------------
  if (verbose) message("Aggregating night-hour values to per-fix averages...")

  tp_col <- paste0("tp_", shift_hours, "h")

  avg_tbl <- long_tbl |>
    dplyr::group_by(.row_id, ID) |>
    dplyr::summarise(
      latitude  = dplyr::first(latitude),
      longitude = dplyr::first(longitude),
      night_n   = dplyr::n(),
      dplyr::across(dplyr::all_of(env_cols), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    )

  # override tp mean with sum (precipitation is additive)
  if (tp_col %in% names(avg_tbl)) {
    tp_sum <- long_tbl |>
      dplyr::group_by(.row_id) |>
      dplyr::summarise(tp_sum = sum(.data[[tp_col]], na.rm = TRUE), .groups = "drop")
    avg_tbl <- avg_tbl |>
      dplyr::left_join(tp_sum, by = ".row_id") |>
      dplyr::mutate(!!tp_col := .data$tp_sum) |>
      dplyr::select(-tp_sum)
  }

  avg_tbl <- avg_tbl |>
    dplyr::left_join(df[, c(".row_id", "timestamp")], by = ".row_id") |>
    dplyr::relocate(timestamp, .after = ID)

  list(
    night_data         = long_tbl,
    average_night_data = avg_tbl
  )
}


# -----------------------------------------------------------------------------
# Utility helpers (unchanged logic, kept for API compatibility)
# -----------------------------------------------------------------------------
consolidate_avg_night <- function(avg_night, row_id = ".row_id") {
  requireNamespace("dplyr", quietly = TRUE)
  stopifnot(row_id %in% names(avg_night))

  avg_night |>
    dplyr::filter(!is.na(.data[[row_id]]), .data[[row_id]] != "") |>
    dplyr::mutate(!!row_id := as.integer(.data[[row_id]])) |>
    dplyr::group_by(.data[[row_id]]) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::everything(),
        ~ { x <- .x[!is.na(.x)]; if (length(x) == 0L) NA else x[[1L]] }
      ),
      .groups = "drop"
    )
}

add_avg_night_to_move2 <- function(m, avg_night_data, row_id = ".row_id") {
  requireNamespace("dplyr", quietly = TRUE)
  requireNamespace("sf",    quietly = TRUE)
  stopifnot(inherits(m, "move2"))
  stopifnot(row_id %in% names(avg_night_data))

  attr_df <- sf::st_drop_geometry(m)
  if (!(row_id %in% names(attr_df))) attr_df[[row_id]] <- seq_len(nrow(attr_df))

  env_cols        <- setdiff(names(avg_night_data), row_id)
  env_cols        <- env_cols[!grepl("timestamp\\.", env_cols)]
  avg_night_data  <- avg_night_data[!is.na(avg_night_data$.row_id), ]

  avg_night_slim <- avg_night_data |>
    dplyr::select(dplyr::all_of(c(row_id, env_cols)))

  attr_df2  <- dplyr::left_join(attr_df, avg_night_slim, by = row_id)
  m_out     <- m
  geom_col  <- attr(m_out, "sf_column")
  for (nm in setdiff(names(attr_df2), geom_col)) m_out[[nm]] <- attr_df2[[nm]]

  m_out
}


# -----------------------------------------------------------------------------
# Usage example (commented out)
# -----------------------------------------------------------------------------
# terra::terraOptions(threads = 8)   # or let the function set it via n_threads
#
# for (day in days) {
#   res <- extract_avg_night_env_from_year_stacks(
#     timestamps     = b_daily$timestamp,
#     latitudes      = b_daily$lat,
#     longitudes     = b_daily$lon,
#     IDs            = b_daily$individual_local_identifier,
#     shift_hours    = day * 24,
#     raster_by_year = raster_by_year,
#     var_names      = vars,
#     n_threads      = 8        # uses terra's own thread pool — works on Windows
#   )
#   b_daily <- add_avg_night_to_move2(
#     m              = b_daily,
#     avg_night_data = res$average_night_data
#   )
# }
