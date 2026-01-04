pressure_to_altitude_m <- function(p_hpa, p0_hpa = 1013.25) {
  # ⛰️ Convert pressure (hPa / mbar) to altitude (m)
  # Vectorized; returns NA for non-positive or missing pressures
  p_hpa <- as.numeric(p_hpa)
  p0_hpa <- as.numeric(p0_hpa)

  out <- rep(NA_real_, length(p_hpa))
  ok <- is.finite(p_hpa) & p_hpa > 0 & is.finite(p0_hpa) & p0_hpa > 0
  out[ok] <- 44330 * (1 - (p_hpa[ok] / p0_hpa)^(0.1903))
  out
}
