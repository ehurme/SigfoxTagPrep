calc_displacement <- function(x, units = "km") {
  require(dplyr)
  require(sf)
  require(move2)

  if (!inherits(x, "move2")) stop("x must be a 'move2' object.")
  if (!units %in% c("km", "m")) stop("units must be 'km' or 'm'")

  geom     <- sf::st_geometry(x)
  ids      <- move2::mt_track_id(x)
  comments <- x$comments

  n <- length(geom)

  displacement_m <- rep(NA_real_, n)
  nsd_m2         <- rep(NA_real_, n)

  # NEW:
  ddisp_m        <- rep(NA_real_, n)  # delta displacement (meters)
  ddisp2_m2      <- rep(NA_real_, n)  # (delta displacement)^2 (m^2)

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

    # displacement from origin (meters)
    d <- sf::st_distance(ggeom, origin_rep, by_element = TRUE)
    d <- as.numeric(d)

    displacement_m[idx] <- d
    nsd_m2[idx]         <- d^2

    # NEW: delta displacement within track order (first is NA)
    ddisp_track <- rep(NA_real_, length(idx))
    if (length(idx) >= 2) {
      prev <- d[-length(d)]
      curr <- d[-1]
      ddisp_track[-1] <- ifelse(is.finite(prev) & is.finite(curr), curr - prev, NA_real_)
    }

    ddisp_m[idx]   <- ddisp_track
    ddisp2_m2[idx] <- ddisp_track^2
  }

  # Unit conversion + attach
  if (units == "km") {
    x$displacement        <- displacement_m / 1000
    x$nsd                 <- nsd_m2                 # keep NSD in m^2 as in your current function
    x$d_displacement      <- ddisp_m / 1000         # km
    x$d_displacement2     <- ddisp2_m2 / (1000^2)   # km^2
  } else { # "m"
    x$displacement        <- displacement_m
    x$nsd                 <- nsd_m2
    x$d_displacement      <- ddisp_m               # m
    x$d_displacement2     <- ddisp2_m2             # m^2
  }

  # Remove old dnsd if it exists (since we’re replacing it)
  if ("dnsd" %in% names(x)) x$dnsd <- NULL

  x
}
