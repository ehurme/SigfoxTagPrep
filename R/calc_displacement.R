#' Calculate displacement from origin for each fix in a move2 object
#'
#' For each track, computes cumulative displacement from the first fix (or a
#' fix flagged as \code{comments == "start"}), as well as net squared
#' displacement (NSD) and the change in displacement between consecutive fixes.
#'
#' @param x A \code{move2} object with \code{comments} and \code{geometry} columns.
#' @param units Character; output distance units. Either \code{"km"} (default)
#'   or \code{"m"}.
#' @return The input \code{move2} object with four new columns:
#' \describe{
#'   \item{\code{displacement}}{Distance from the deployment origin (km or m).}
#'   \item{\code{nsd}}{Net squared displacement in m\eqn{^2}.}
#'   \item{\code{d_displacement}}{Change in displacement from the previous fix
#'     (km or m). Positive = moving away, negative = returning.}
#'   \item{\code{d_displacement2}}{Squared change in displacement (km\eqn{^2}
#'     or m\eqn{^2}).}
#' }
#' @importFrom sf st_geometry st_is_empty st_distance
#' @importFrom move2 mt_track_id
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
