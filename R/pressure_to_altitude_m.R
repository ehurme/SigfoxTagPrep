#' Convert atmospheric pressure to altitude using the ISA hypsometric formula
#'
#' Vectorized conversion using the International Standard Atmosphere (ISA)
#' relationship: \eqn{z = 44330 \times (1 - (P / P_0)^{0.1903})} m.
#'
#' @param p_hpa Numeric vector of pressures in hPa (mbar).
#' @param p0_hpa Reference sea-level pressure in hPa. Default \code{1013.25}.
#' @return Numeric vector of altitudes in metres. Returns \code{NA} for missing
#'   or non-positive pressure values.
#' @examples
#' pressure_to_altitude_m(1013.25)  # ~ 0 m
#' pressure_to_altitude_m(800)      # ~ 1949 m
#' @export
pressure_to_altitude_m <- function(p_hpa, p0_hpa = 1013.25) {
  # ⛰️ Convert pressure (hPa / mbar) to altitude (m)
  # Vectorized; returns NA for non-positive or missing pressures
  p_hpa <- as.numeric(p_hpa)
  p0_hpa <- as.numeric(p0_hpa)

  out <- rep(NA_real_, length(p_hpa))
  ok <- which(is.finite(p_hpa) & p_hpa > 0 & is.finite(p0_hpa) & p0_hpa > 0)
  out[ok] <- 44330 * (1 - (p_hpa[ok] / p0_hpa)^(0.1903))
  out
}

#' Add altitude column derived from barometric pressure
#'
#' Converts barometric pressure to altitude using \code{\link{pressure_to_altitude_m}}
#' and optionally retrieves ground elevation from the AWS elevation service via
#' \pkg{elevatr}. Supports both TinyFox and NanoFox column schemas.
#'
#' @param df A data frame or move2 object.
#' @param tag_type_col Column name containing the tag type
#'   (\code{"nanofox"} or \code{"tinyfox"}). Default \code{"tag_type"}.
#' @param nano_pressure_col Column name for NanoFox minimum pressure (hPa).
#'   Default \code{"min_3h_pressure"}.
#' @param tiny_pressure_col Column name for TinyFox minimum pressure (hPa).
#'   Default \code{"tinyfox_pressure_min_last_24h"}.
#' @param p0_hpa Reference sea-level pressure in hPa. Default \code{1013.25}.
#' @param altitude_col Name of the output altitude column (m). Default
#'   \code{"altitude_m"}.
#' @param pressure_used_col Name of the output column recording which pressure
#'   value was used. Default \code{"pressure_hpa_used"}.
#' @return The input data frame with \code{altitude_m} and
#'   \code{pressure_hpa_used} columns added. Altitudes above 7000 m are set to
#'   \code{NA} as sensor errors.
#' @seealso \code{\link{pressure_to_altitude_m}}
add_altitude_from_pressure <- function(df,
                                       tag_type_col = "tag_type",
                                       nano_pressure_col = "min_3h_pressure",
                                       tiny_pressure_col = "tinyfox_pressure_min_last_24h",
                                       p0_hpa = 1013.25,
                                       altitude_col = "altitude_m",
                                       pressure_used_col = "pressure_hpa_used") {
  require(dplyr)
  require(elevatr)
  require(sf)

  df$elevation <- NA
  idx <- which(!sf::st_is_empty(df$geometry) & df$sensor_type == "location" & !is.na(df$lon))
  try({
    df$elevation[idx] <- get_elev_point(locations = with(df[idx,], data.frame(x = lon, y = lat)),
                                        src = "aws", prj = 4326)$elevation
  })

  if (!tag_type_col %in% names(df)) {
    stop("add_altitude_from_pressure(): tag_type_col not found: ", tag_type_col)
  }

  # Allow NULL to mean "not available"
  nano_ok <- !is.null(nano_pressure_col) && nano_pressure_col %in% names(df)
  tiny_ok <- !is.null(tiny_pressure_col) && tiny_pressure_col %in% names(df)

  # Pull columns safely (or NA vectors)
  nano_p <- if (nano_ok) as.numeric(df[[nano_pressure_col]]) else rep(NA_real_, nrow(df))
  tiny_p <- if (tiny_ok) as.numeric(df[[tiny_pressure_col]]) else rep(NA_real_, nrow(df))

  tag <- tolower(as.character(df[[tag_type_col]]))

  pressure_used <- rep(NA_real_, nrow(df))
  pressure_used[which(tag == "nanofox")] <- nano_p[which(tag == "nanofox")]
  pressure_used[which(tag == "tinyfox")] <- tiny_p[which(tag == "tinyfox")]

  df[[pressure_used_col]] <- pressure_used
  df[[altitude_col]] <- pressure_to_altitude_m(pressure_used, p0_hpa = p0_hpa)

  # remove altitude above 6000 m, which is an error
  df[[altitude_col]][which(df[[altitude_col]] > 7000)] <- NA

  df
}

