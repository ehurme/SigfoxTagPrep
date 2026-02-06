
# Add moonlight / twilight covariates to a move2 object using moonlit
add_moonlit_to_move2 <- function(x,
                                 time_col = "timestamp",
                                 e = NULL,
                                 e_from_altitude = FALSE,
                                 altitude_col = "altitude",
                                 altitude_fun = stats::median,  # used if e_from_altitude=TRUE and e is NULL
                                 tz = NULL,
                                 prefix = "moonlit_",
                                 quiet = FALSE) {
  # ---- deps inside function (as requested) ----
  require(move2)
  require(sf)
  require(dplyr)

  if (!requireNamespace("moonlit", quietly = TRUE)) {
    stop(
      "Package 'moonlit' is required. Install with:\n",
      "  install.packages('devtools')\n",
      "  devtools::install_github('msmielak/moonlit')\n",
      "Then: library(moonlit)\n"
    )
  }

  if (!inherits(x, "move2")) stop("x must be a 'move2' object.")
  if (!time_col %in% names(x)) stop("'", time_col, "' not found in x.")
  if (!is.null(tz) && !is.character(tz)) stop("tz must be NULL or a character time zone.")

  # ---- extract geometry + ensure lon/lat in EPSG:4326 ----
  g <- sf::st_geometry(x)
  if (is.na(sf::st_crs(g))) {
    stop("Geometry has no CRS. Please set CRS before calling (e.g., st_set_crs(x, 4326)).")
  }
  g_ll <- sf::st_transform(g, 4326)
  xy <- sf::st_coordinates(g_ll)
  lon <- xy[, 1]
  lat <- xy[, 2]

  # ---- timestamps ----
  tt <- x[[time_col]]
  if (!inherits(tt, "POSIXct")) {
    stop("'", time_col, "' must be POSIXct (local time zone recommended by moonlit).")
  }
  if (!is.null(tz)) {
    tt <- as.POSIXct(tt, tz = tz)
  }

  # ---- extinction coefficient e ----
  if (is.null(e)) {
    if (isTRUE(e_from_altitude)) {
      if (!altitude_col %in% names(x)) {
        stop("e_from_altitude=TRUE but '", altitude_col, "' not found in x.")
      }
      alt <- x[[altitude_col]]
      alt_val <- altitude_fun(alt[is.finite(alt)], na.rm = TRUE)

      if (!is.finite(alt_val)) {
        stop("Could not compute a finite altitude value from '", altitude_col, "'.")
      }
      # moonlit expects elevation (asl) in meters for elevExtCoeff()
      e <- moonlit::elevExtCoeff(alt_val)
      if (!quiet) message("Computed extinction coefficient e=", signif(e, 4),
                          " from ", altitude_col, " using ", deparse(substitute(altitude_fun)), ".")
    } else {
      # moonlit README provides typical values; we default to sea-level-ish if nothing else is known
      e <- 0.28
      if (!quiet) message("Using default extinction coefficient e=0.28 (sea level).")
    }
  } else {
    if (!is.numeric(e) || length(e) != 1L || !is.finite(e)) stop("e must be a single finite numeric.")
  }

  # ---- compute moonlight intensity ----
  # moonlit::calculateMoonlightIntensity(lat, lon, date, e) returns:
  # night (logical), sunAltDegrees, moonlightModel, twilightModel
  ok <- is.finite(lat) & is.finite(lon) & !is.na(tt)

  out <- data.frame(
    night = rep(NA, length(tt)),
    sunAltDegrees = rep(NA_real_, length(tt)),
    moonlightModel = rep(NA_real_, length(tt)),
    twilightModel = rep(NA_real_, length(tt))
  )

  if (any(ok)) {
    res <- moonlit::calculateMoonlightIntensity(
      lat = lat[ok],
      lon = lon[ok],
      date = tt[ok],
      e = e
    )
    # Defensive: ensure expected columns exist
    need <- c("night", "sunAltDegrees", "moonlightModel", "twilightModel")
    miss <- setdiff(need, names(res))
    if (length(miss) > 0) stop("moonlit output missing columns: ", paste(miss, collapse = ", "))

    out[ok, need] <- res[, need]
  }

  # ---- bind back to move2 object ----
  x[[paste0(prefix, "night")]]            <- out$night
  x[[paste0(prefix, "sun_alt_degrees")]]  <- out$sunAltDegrees
  x[[paste0(prefix, "moonlight_model")]]  <- out$moonlightModel
  x[[paste0(prefix, "twilight_model_lx")]]<- out$twilightModel
  x[[paste0(prefix, "ext_coeff_e")]]      <- e

  x
}

# b_daily_wind <- add_moonlit_to_move2(b_daily_wind)
b_daily_wind$moonlit_moonlight_model %>% plot()
