library(move2)

# internal utility: shift a numeric vector one step "back" within each track
.mt_prev_by_track <- function(x, ids) {
  n <- length(x)
  if (n == 0L) return(x)

  # basic lag: x[i] ← x[i-1]
  out <- c(NA, x[-n])

  # ensure ids are numeric for diff()
  if (is.character(ids)) {
    ids_num <- as.numeric(factor(ids))
  } else {
    ids_num <- as.numeric(ids)
  }

  # where track id changes between i and i+1, set first row of new track to NA
  breaks <- which(diff(ids_num) != 0L)
  if (length(breaks)) {
    out[breaks + 1L] <- NA
  }

  out
}

mt_distance_prev <- function(x, units = "km") {
  # x: move2 object with POINT geometry
  d <- mt_distance(x, units = units)         # standard "from-step" distance
  ids <- mt_track_id(x)                      # track ids
  .mt_prev_by_track(d, ids)                  # shift to "previous step"
}

mt_speed_prev <- function(x, units = "km/hr") {
  v <- mt_speed(x, units = units)           # standard "from-step" speed
  ids <- mt_track_id(x)
  .mt_prev_by_track(v, ids)
}

mt_azimuth_prev <- function(x) {
  a <- mt_azimuth(x)                        # standard "from-step" azimuth
  ids <- mt_track_id(x)
  .mt_prev_by_track(a, ids)
}

mt_time_lags_prev <- function(x, units = "secs") {
  tl <- mt_time_lags(x, units = units)       # time from i -> i+1
  ids <- mt_track_id(x)
  .mt_prev_by_track(tl, ids)                # attach to point i+1
}

mt_add_prev_metrics <- function(x,
                                dist_units  = "km",
                                speed_units = "km/hr",
                                time_units  = "secs") {
  stopifnot(inherits(x, "move2"))

  x$dist_prev    <- mt_distance_prev(x, units = dist_units)
  x$speed_prev   <- mt_speed_prev(x, units = speed_units)
  x$azimuth_prev <- mt_azimuth_prev(x)
  x$dt_prev      <- mt_time_lags_prev(x, units = time_units)

  x
}
