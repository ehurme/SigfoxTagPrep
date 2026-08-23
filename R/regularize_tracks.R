#' Regularize irregular tracks onto a shared, grid-aligned time series
#'
#' Sigfox/ICARUS fixes arrive at irregular intervals, which makes it
#' impossible to compare two individuals' positions at "the same time"
#' without some form of interpolation. This function fits a continuous-time
#' correlated random walk (CTCRW, via \code{momentuHMM::crawlWrap}, i.e. the
#' same \code{crawl} engine already used in \code{R/pressure_wind_profile.R})
#' to each individual's track and predicts locations on a single global time
#' grid (aligned to a fixed epoch, so every individual's predicted timestamps
#' fall on exactly the same clock ticks). Predictions are only made between
#' each individual's own first and last fix (no extrapolation). A linear
#' interpolation fallback is used if the CTCRW fit fails for a track (too
#' few fixes, singular fit, etc.), and can also be requested directly via
#' \code{method = "linear"}.
#'
#' Distances are computed in a local azimuthal equidistant (AEQD) projection
#' centred on the data, rather than Web Mercator, because Mercator distances
#' become increasingly distorted away from the equator and this function's
#' entire purpose is downstream distance comparison.
#'
#' @param x A \code{move2}/\code{sf} object with POINT geometry (or a
#'   data.frame with longitude/latitude columns named \code{lon}/\code{lat}
#'   or \code{location_long}/\code{location_lat}).
#' @param id_col Name of the individual identifier column.
#' @param time_col Name of the POSIXct timestamp column.
#' @param dt_min Grid spacing in minutes.
#' @param method \code{"crawl"} (default) or \code{"linear"}.
#' @param crs Optional projected CRS (as accepted by \code{sf::st_crs}) to
#'   use for the x/y coordinates. Defaults to an auto-derived local
#'   azimuthal equidistant projection centred on the data.
#' @param min_fixes Tracks with fewer than this many fixes are dropped.
#' @param theta Starting values passed to \code{crawlWrap}'s CTCRW fit.
#' @param parallel If \code{TRUE} and the \code{future.apply} package is
#'   available, fit individuals in parallel (register a \code{future} plan
#'   beforehand, e.g. \code{future::plan("multisession")}).
#' @param verbose Print progress messages.
#' @return A \code{data.table} with one row per individual per grid
#'   timestamp: \code{id}, \code{timestamp}, \code{x}, \code{y} (projected
#'   metres), \code{lon}, \code{lat}, \code{se_x}, \code{se_y} (CTCRW
#'   prediction standard errors, \code{NA} for linear interpolation),
#'   \code{interpolated} (logical), \code{method} used per track, and
#'   \code{n_fix} (number of raw fixes the track was built from). The
#'   \code{crs} and \code{dt_min} are attached as attributes for use by
#'   downstream functions.
#' @importFrom sf st_geometry st_coordinates st_transform st_crs st_as_sf st_is_empty
#' @importFrom move2 mt_track_id
#' @importFrom data.table data.table setDT rbindlist setattr
regularize_tracks <- function(x,
                               id_col = "individual_local_identifier",
                               time_col = "timestamp",
                               dt_min = 10,
                               method = c("crawl", "linear"),
                               crs = NULL,
                               min_fixes = 3,
                               theta = c(6, -0.1),
                               parallel = FALSE,
                               verbose = TRUE) {
  method <- match.arg(method)
  requireNamespace("sf")
  requireNamespace("data.table")

  # ── 1. Pull id / time / coordinates into a flat table ─────────────────────
  if (inherits(x, "sf")) {
    geom <- sf::st_geometry(x)
    keep <- !sf::st_is_empty(geom)
    ll <- sf::st_coordinates(sf::st_transform(x[keep, ], 4326))
    id <- if (id_col %in% names(x)) x[[id_col]][keep] else move2::mt_track_id(x)[keep]
    ts <- x[[time_col]][keep]
    df <- data.table::data.table(id = as.character(id), timestamp = ts,
                                  lon = ll[, "X"], lat = ll[, "Y"])
  } else if (is.data.frame(x)) {
    lon_col <- intersect(c("lon", "location_long", ".lon"), names(x))[1]
    lat_col <- intersect(c("lat", "location_lat", ".lat"), names(x))[1]
    if (is.na(lon_col) || is.na(lat_col))
      stop("Could not find longitude/latitude columns in `x`.")
    df <- data.table::data.table(id = as.character(x[[id_col]]), timestamp = x[[time_col]],
                                  lon = x[[lon_col]], lat = x[[lat_col]])
  } else {
    stop("`x` must be an sf/move2 object or a data.frame.")
  }

  df <- df[!is.na(timestamp) & !is.na(lon) & !is.na(lat)]
  data.table::setorder(df, id, timestamp)
  df <- unique(df, by = c("id", "timestamp"))

  n_fix <- df[, .N, by = id]
  keep_ids <- n_fix[N >= min_fixes, id]
  dropped <- setdiff(unique(df$id), keep_ids)
  if (length(dropped) && verbose)
    message("Dropping ", length(dropped), " individual(s) with < ", min_fixes, " fixes: ",
            paste(dropped, collapse = ", "))
  df <- df[id %in% keep_ids]
  if (nrow(df) == 0) stop("No individuals with enough fixes to regularize.")

  # ── 2. Project ──────────────────────────────────────────────────────────
  if (is.null(crs)) {
    ctr <- c(mean(range(df$lon)), mean(range(df$lat)))
    crs <- sprintf("+proj=aeqd +lat_0=%f +lon_0=%f +datum=WGS84 +units=m +no_defs", ctr[2], ctr[1])
  }
  pts <- sf::st_as_sf(df, coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  xy <- sf::st_coordinates(sf::st_transform(pts, crs))
  df[, `:=`(x = xy[, "X"], y = xy[, "Y"])]

  # ── 3. Build a global, epoch-aligned prediction grid per track ────────────
  dt_sec <- dt_min * 60
  epoch <- as.numeric(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"))
  .grid_for <- function(t) {
    tn <- as.numeric(t)
    start <- epoch + ceiling((min(tn) - epoch) / dt_sec) * dt_sec
    end   <- epoch + floor((max(tn) - epoch) / dt_sec) * dt_sec
    if (end <= start) return(as.POSIXct(mean(tn), origin = "1970-01-01", tz = "UTC"))
    as.POSIXct(seq(start, end, by = dt_sec), origin = "1970-01-01", tz = "UTC")
  }

  ids <- unique(df$id)
  pred_times <- lapply(ids, function(i) .grid_for(df[id == i, timestamp]))
  names(pred_times) <- ids

  # ── 4. Fit + predict, per individual ───────────────────────────────────
  .fit_one <- function(i) {
    tr <- df[id == i]
    grid <- pred_times[[i]]

    .linear_fallback <- function() {
      xo <- stats::approx(as.numeric(tr$timestamp), tr$x, xout = as.numeric(grid))$y
      yo <- stats::approx(as.numeric(tr$timestamp), tr$y, xout = as.numeric(grid))$y
      data.table::data.table(id = i, timestamp = grid, x = xo, y = yo,
                              se_x = NA_real_, se_y = NA_real_,
                              interpolated = TRUE, method = "linear", n_fix = nrow(tr))
    }

    if (method == "linear") return(.linear_fallback())

    if (!requireNamespace("momentuHMM", quietly = TRUE)) {
      if (verbose) message("momentuHMM not available; using linear interpolation for '", i, "'.")
      return(.linear_fallback())
    }

    crawl_in <- data.frame(ID = i, Time = as.numeric(tr$timestamp), x = tr$x, y = tr$y)
    fit <- tryCatch(
      momentuHMM::crawlWrap(obsData = crawl_in, Time.name = "Time", ID.name = "ID",
                             coord = c("x", "y"), theta = theta, fixPar = c(NA, NA),
                             predTime = stats::setNames(list(as.numeric(grid)), i)),
      error = function(e) {
        if (verbose) message("CRAWL fit failed for '", i, "' (", conditionMessage(e),
                              "); falling back to linear interpolation.")
        NULL
      }
    )
    if (is.null(fit)) return(.linear_fallback())

    pred <- fit$crwPredict
    pred <- pred[pred$locType == "p", ]
    data.table::data.table(id = i, timestamp = grid, x = pred$mu.x, y = pred$mu.y,
                            se_x = pred$se.mu.x, se_y = pred$se.mu.y,
                            interpolated = TRUE, method = "crawl", n_fix = nrow(tr))
  }

  if (parallel && requireNamespace("future.apply", quietly = TRUE)) {
    out_list <- future.apply::future_lapply(ids, .fit_one, future.seed = TRUE)
  } else {
    out_list <- lapply(ids, function(i) {
      if (verbose) message("Regularizing '", i, "' (", method, ")...")
      .fit_one(i)
    })
  }
  out <- data.table::rbindlist(out_list, fill = TRUE)

  ll_out <- sf::st_coordinates(sf::st_transform(
    sf::st_as_sf(out, coords = c("x", "y"), crs = crs, remove = FALSE), 4326))
  out[, `:=`(lon = ll_out[, "X"], lat = ll_out[, "Y"])]

  data.table::setattr(out, "crs", crs)
  data.table::setattr(out, "dt_min", dt_min)
  out[]
}
