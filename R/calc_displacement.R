calc_displacement <- function(x, units = "km") {
  require(dplyr)
  require(sf)
  require(move2)

  if (!inherits(x, "move2")) {
    stop("x must be a 'move2' object.")
  }

  geom <- sf::st_geometry(x)
  ids  <- move2::mt_track_id(x)
  comments <- x$comments

  n <- length(geom)

  displacement_m <- rep(NA_real_, n)
  nsd            <- rep(NA_real_, n)
  dnsd           <- rep(NA_real_, n)

  track_levels <- unique(ids)

  for (id_val in track_levels) {
    idx <- which(ids == id_val)
    if (length(idx) == 0L) next

    ggeom     <- geom[idx]
    gcomments <- comments[idx]

    # 1) Prefer 'start' rows with valid geometry
    origin_local <- which(gcomments == "start" & !sf::st_is_empty(ggeom))

    # 2) Fall back to first non-empty geometry
    if (length(origin_local) == 0L) {
      origin_local <- which(!sf::st_is_empty(ggeom))
    }

    # 3) If still empty, skip track
    if (length(origin_local) == 0L) next

    origin_local <- origin_local[1]
    origin_geom  <- ggeom[origin_local]
    origin_rep   <- origin_geom[rep(1, length(idx))]

    # Compute displacement (meters)
    d <- sf::st_distance(ggeom, origin_rep, by_element = TRUE)
    d <- as.numeric(d)

    displacement_m[idx] <- d
    nsd[idx]            <- d^2

    # Change in NSD (ΔNSD) within the track
    # dnsd[i] = nsd[i] - nsd[i-1]  ; first point gets NA
    dnsd_track <- rep(NA_real_, length(idx))

    if (sum(!is.na(nsd[idx])) > 1) {
      dnsd_track[-1] <- diff(nsd[idx])
    }

    dnsd[idx] <- dnsd_track
  }

  # Unit conversion
  if (units == "km") {
    displacement <- displacement_m / 1000
  } else if (units == "m") {
    displacement <- displacement_m
  } else {
    stop("units must be 'km' or 'm'")
  }

  # Attach results
  x$displacement <- displacement
  x$nsd          <- nsd
  x$dnsd         <- dnsd

  return(x)
}
