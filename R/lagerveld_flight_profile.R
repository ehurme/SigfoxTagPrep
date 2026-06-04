# lagerveld_flight_profile.R
# ─────────────────────────────────────────────────────────────────────────────
# Airspeed-altitude profile analysis following Lagerveld et al. (2024)
# Movement Ecology 12:81. doi:10.1186/s40462-024-00520-7
#
# Core idea: for a bat flying from A to B, at each altitude the ERA5 wind
# vector changes. The required airspeed to achieve the observed ground
# displacement is:
#
#   va(z) = |v_ground - v_wind(z)|          [vector magnitude]
#
# The altitude minimising va(z) is the predicted optimal (minimum-energy)
# flight level.  Minimum-power speed (Vmp) and maximum-range speed (Vmr)
# serve as biological benchmarks.  Altitudes where the crosswind exceeds the
# required airspeed are flagged as infeasible following Eq. 4 of the paper.
#
# Altitude-dependent adjustment (ISA):
#   Vmp(z) = Vmp_sl × √(ρ_sl / ρ(z))      [air density thins → bat flies faster]
#   Vmr(z) = Vmr_sl × √(ρ_sl / ρ(z))
#
# Functions
# ─────────────────────────────────────────────────────────────────────────────
#   bat_flight_params              data frame of species-specific Vmp/Vmr
#   isa_density_kgm3()             ISA air density at altitude
#   interpolate_wind_to_altgrid()  interpolate ERA5 pressure winds to alt grid
#   compute_airspeed_alt_profile() one-row profile (single interval/timestep)
#   compute_night_profiles()       applies the above over a full night data frame
#   plot_lagerveld_profile()       Fig. 3-style ggplot
# ─────────────────────────────────────────────────────────────────────────────


# ── Species reference table ───────────────────────────────────────────────────
#
#' Reference airspeeds for common migratory bat species
#'
#' Vmp = minimum power speed (minimises energy per unit time).
#' Vmr = maximum range speed (minimises energy per unit distance).
#' Theoretical ratio: Vmr / Vmp = 3^(1/4) ≈ 1.316 (Hedenström & Alerstam 1995).
#' Values at sea-level air density (ρ₀ = 1.225 kg/m³).
#'
#' @format A data frame with columns:
#' \describe{
#'   \item{species}{Binomial species name}
#'   \item{vmp_sl_ms}{Minimum power speed at sea level (m/s)}
#'   \item{vmp_sd_ms}{SD of Vmp (NA if theoretical)}
#'   \item{vmr_sl_ms}{Maximum range speed at sea level (m/s)}
#'   \item{vmr_sd_ms}{SD of Vmr (NA if theoretical)}
#'   \item{source}{Reference}
#' }
#' @export
bat_flight_params <- data.frame(
  species    = c("Pipistrellus nathusii",
                 "Nyctalus noctula",
                 "Nyctalus leisleri",
                 "Pipistrellus pipistrellus",
                 "Eptesicus serotinus",
                 "Myotis daubentonii"),
  vmp_sl_ms  = c(5.8,   8.0,   7.0,   5.5,   8.5,   6.5),
  vmp_sd_ms  = c(1.0,   NA,    NA,     NA,    NA,    NA),
  vmr_sl_ms  = c(7.5,   10.5,  9.2,   7.3,   11.2,  8.6),
  vmr_sd_ms  = c(1.1,   NA,    NA,     NA,    NA,    NA),
  source     = c(
    "Troxell et al. 2019 (wind tunnel)",
    "Pennycuick 2008 (theoretical, m=25g, b=35cm)",
    "Pennycuick 2008 (theoretical, m=15g, b=29cm)",
    "Pennycuick 2008 (theoretical, m=5g,  b=23cm)",
    "Pennycuick 2008 (theoretical, m=25g, b=38cm)",
    "Pennycuick 2008 (theoretical, m=9g,  b=27cm)"
  ),
  stringsAsFactors = FALSE
)


# ── ISA air density ───────────────────────────────────────────────────────────

#' Air density from ISA standard atmosphere
#'
#' Uses the International Standard Atmosphere tropospheric lapse rate
#' (-6.5 K/km) to compute air density at altitude.
#'
#' @param alt_m Numeric vector of altitudes (m). Clamped to [0, 11000] m
#'   (troposphere only).
#' @return Numeric vector of air densities (kg/m³).
#' @export
isa_density_kgm3 <- function(alt_m) {
  T0   <- 288.15      # K, sea-level temperature
  rho0 <- 1.225       # kg/m³, sea-level density
  L    <- -0.0065     # K/m, lapse rate
  g0   <- 9.80665     # m/s²
  R    <- 8.3144598   # J/(mol·K)
  M    <- 0.0289644   # kg/mol

  alt_m <- pmax(0, pmin(alt_m, 11000))
  T_z   <- T0 + L * alt_m
  # ρ/ρ₀ = (T_z/T0)^(g0*M/(R*(-L)) - 1)
  exponent <- g0 * M / (R * (-L)) - 1   # ≈ 4.2559
  rho0 * (T_z / T0)^exponent
}


# ── Vmp/Vmr altitude adjustment ───────────────────────────────────────────────

#' Adjust Vmp / Vmr for air density at altitude
#'
#' @param v_sl Sea-level airspeed (m/s), either Vmp_sl or Vmr_sl.
#' @param alt_m Altitude in metres.
#' @return Adjusted airspeed (m/s) at the given altitude.
#' @export
adjust_airspeed_for_altitude <- function(v_sl, alt_m) {
  rho0 <- 1.225
  rho_z <- isa_density_kgm3(alt_m)
  v_sl * sqrt(rho0 / rho_z)
}


# ── Wind interpolation from pressure levels to altitude grid ─────────────────

#' Interpolate ERA5 pressure-level winds to a fixed altitude grid
#'
#' Converts each ERA5 pressure level to an altitude using the ISA barometric
#' formula, then linearly interpolates u and v to the target altitude grid.
#' Extrapolation beyond the range of available levels returns NA.
#'
#' @param u_levels Named numeric vector; names = pressure levels (hPa),
#'   values = eastward wind component (m/s).
#' @param v_levels Named numeric vector; same for northward component.
#' @param p0_hpa   Sea-level pressure used in altitude conversion (hPa).
#'   Default 1013.25 (ISA).
#' @param alt_grid_m Target altitude grid (m). Default matches Lagerveld et al.
#' @return Data frame with columns: alt_m, u_ms, v_ms, wind_speed_ms,
#'   wind_heading_deg (meteorological: direction FROM).
#' @export
interpolate_wind_to_altgrid <- function(
    u_levels,
    v_levels,
    p0_hpa    = 1013.25,
    alt_grid_m = c(10, 200, 400, 600, 800, 1000, 1200, 1400,
                   1600, 1800, 2000, 2500, 3000)
) {
  if (!exists("pressure_to_altitude_m", mode = "function")) {
    stop("pressure_to_altitude_m() not found. Source pressure_to_altitude_m.R first.")
  }

  lvls <- as.numeric(names(u_levels))
  ok   <- is.finite(lvls) & is.finite(u_levels) & is.finite(v_levels)
  if (sum(ok) < 2) {
    return(data.frame(alt_m = alt_grid_m,
                      u_ms  = NA_real_, v_ms = NA_real_,
                      wind_speed_ms = NA_real_, wind_heading_deg = NA_real_))
  }

  # Convert pressure levels to altitude (descending pressure = ascending altitude)
  alt_levels <- pressure_to_altitude_m(lvls[ok], p0_hpa = p0_hpa)
  u_vals     <- as.numeric(u_levels)[ok]
  v_vals     <- as.numeric(v_levels)[ok]

  # Sort by altitude (ascending)
  ord      <- order(alt_levels)
  alt_src  <- alt_levels[ord]
  u_src    <- u_vals[ord]
  v_src    <- v_vals[ord]

  # Linear interpolation (approxfun, rule=2 → constant extrapolation at edges)
  u_interp <- approxfun(alt_src, u_src, rule = 2)
  v_interp <- approxfun(alt_src, v_src, rule = 2)

  # Mark altitudes outside the source range as NA
  in_range <- alt_grid_m >= min(alt_src) & alt_grid_m <= max(alt_src)

  u_out <- ifelse(in_range, u_interp(alt_grid_m), NA_real_)
  v_out <- ifelse(in_range, v_interp(alt_grid_m), NA_real_)

  # Wind speed and FROM-direction (meteorological convention)
  ws      <- sqrt(u_out^2 + v_out^2)
  wdir    <- (atan2(-u_out, -v_out) * 180 / pi + 360) %% 360

  data.frame(
    alt_m            = alt_grid_m,
    u_ms             = u_out,
    v_ms             = v_out,
    wind_speed_ms    = ws,
    wind_heading_deg = wdir
  )
}


# ── Core profile computation ──────────────────────────────────────────────────

#' Compute airspeed-altitude profile for a single fix interval
#'
#' For each altitude in \code{alt_grid_m}, computes the airspeed a bat flying
#' with the observed ground velocity would require if it were at that altitude.
#' Follows Lagerveld et al. (2024): the required airspeed is the magnitude of
#' the vector \emph{v_ground − v_wind(z)}.
#'
#' @section Feasibility filter (Lagerveld Eq. 4):
#' An altitude is "infeasible" when the crosswind component perpendicular to
#' the implied bat heading exceeds the required airspeed.  Equivalently: the
#' angle between the implied bat heading and the wind heading is greater than
#' \code{arcsin(va / vw)}.
#'
#' @param u_levels  Named numeric vector of ERA5 u-wind at pressure levels
#'   (hPa).  Names must be numeric pressure levels, e.g. \code{c("1000"=2.1)}.
#' @param v_levels  Same for v-wind.
#' @param ground_speed_ms   Observed ground speed (m/s) for the interval.
#' @param ground_heading_deg Observed ground heading (degrees from N, CW).
#' @param vmp_sl_ms Minimum power speed at sea level (m/s). Default 7.5.
#' @param vmr_sl_ms Max range speed at sea level (m/s).  If \code{NULL}
#'   computed as \code{vmp_sl_ms × 3^(1/4)}.
#' @param p0_hpa    Sea-level pressure for altitude conversion (hPa).
#' @param alt_grid_m Target altitude grid (m).
#'
#' @return A data frame (one row per altitude) with columns:
#' \describe{
#'   \item{alt_m}{Altitude (m)}
#'   \item{u_ms, v_ms}{Interpolated wind components (m/s)}
#'   \item{wind_speed_ms}{Wind speed (m/s)}
#'   \item{tailwind_ms}{Tailwind component along ground heading (m/s)}
#'   \item{crosswind_ms}{Crosswind component (m/s; left positive)}
#'   \item{va_required_ms}{Required airspeed (m/s)}
#'   \item{bat_heading_deg}{Implied bat heading (degrees from N)}
#'   \item{vmp_at_alt_ms}{Altitude-adjusted Vmp (m/s)}
#'   \item{vmr_at_alt_ms}{Altitude-adjusted Vmr (m/s)}
#'   \item{feasible}{Logical: is this altitude feasible (Eq. 4)?}
#'   \item{at_vmp}{va_required within ±1.5 m/s of Vmp}
#'   \item{at_vmr}{va_required within ±1.5 m/s of Vmr}
#' }
#'
#' @export
compute_airspeed_alt_profile <- function(
    u_levels,
    v_levels,
    ground_speed_ms,
    ground_heading_deg,
    vmp_sl_ms   = 7.5,
    vmr_sl_ms   = NULL,
    p0_hpa      = 1013.25,
    alt_grid_m  = c(10, 200, 400, 600, 800, 1000, 1200, 1400,
                    1600, 1800, 2000, 2500, 3000)
) {
  if (is.null(vmr_sl_ms)) vmr_sl_ms <- vmp_sl_ms * 3^(1/4)

  # Ground velocity in E-N components
  hdg_rad <- ground_heading_deg * pi / 180
  vg_E    <- ground_speed_ms * sin(hdg_rad)
  vg_N    <- ground_speed_ms * cos(hdg_rad)

  # Interpolate wind to altitude grid
  wind_grid <- interpolate_wind_to_altgrid(
    u_levels, v_levels, p0_hpa = p0_hpa, alt_grid_m = alt_grid_m
  )

  # Required airspeed vector at each altitude: v_air = v_ground - v_wind
  va_E <- vg_E - wind_grid$u_ms
  va_N <- vg_N - wind_grid$v_ms

  va_required  <- sqrt(va_E^2 + va_N^2)
  bat_hdg_deg  <- (atan2(va_E, va_N) * 180 / pi + 360) %% 360

  # Tailwind and crosswind relative to ground heading
  # (same convention as wind_support() in calculate_wind_features.R)
  tw <- wind_grid$u_ms * sin(hdg_rad) + wind_grid$v_ms * cos(hdg_rad)
  cw <- wind_grid$u_ms * cos(hdg_rad) - wind_grid$v_ms * sin(hdg_rad)

  # Altitude-adjusted reference speeds
  vmp_z <- adjust_airspeed_for_altitude(vmp_sl_ms, alt_grid_m)
  vmr_z <- adjust_airspeed_for_altitude(vmr_sl_ms, alt_grid_m)

  # Feasibility filter (Lagerveld Eq. 4)
  # Max allowable angle between bat heading and wind heading = arcsin(va/vw)
  # An altitude is infeasible when wind speed > required airspeed AND the
  # actual angle between wind and bat headings > arcmax.
  vw     <- wind_grid$wind_speed_ms
  angle_wind_bat <- abs(((wind_grid$wind_heading_deg - bat_hdg_deg) + 180) %% 360 - 180)
  alpha_max_deg  <- ifelse(vw > 0 & va_required < vw,
                           asin(pmin(1, va_required / vw)) * 180 / pi,
                           90)
  feasible <- is.na(va_required) | angle_wind_bat <= alpha_max_deg

  data.frame(
    alt_m            = alt_grid_m,
    u_ms             = wind_grid$u_ms,
    v_ms             = wind_grid$v_ms,
    wind_speed_ms    = vw,
    tailwind_ms      = tw,
    crosswind_ms     = cw,
    va_required_ms   = va_required,
    bat_heading_deg  = bat_hdg_deg,
    vmp_at_alt_ms    = vmp_z,
    vmr_at_alt_ms    = vmr_z,
    feasible         = feasible
  )
}


# ── Night-level wrapper ───────────────────────────────────────────────────────

#' Compute airspeed-altitude profiles for all fix intervals in a night
#'
#' For each pair of consecutive location fixes within a night window, calls
#' \code{\link{compute_airspeed_alt_profile}} and returns a long data frame
#' with one row per (fix interval × altitude) combination.
#'
#' @param night_df  Data frame / sf of location rows for the night, already
#'   annotated with ERA5 wind via \code{annotate_era5()}.  Must be ordered
#'   by timestamp.
#' @param pressure_levels Numeric vector of ERA5 pressure levels (hPa)
#'   matching the \code{era5_u<level>} columns in \code{night_df}.
#' @param vmp_sl_ms Sea-level Vmp (m/s). Default 7.5. Override per species.
#' @param vmr_sl_ms Sea-level Vmr (m/s). Default NULL → 1.316 × Vmp.
#' @param alt_grid_m Altitude evaluation grid (m).
#' @param min_speed_ms Minimum ground speed (m/s) for a fix interval to be
#'   included. Filters out near-stationary rows. Default 1.
#'
#' @return A data frame with all profile columns plus:
#' \describe{
#'   \item{interval_id}{Integer counter for each fix interval}
#'   \item{t_mid}{POSIXct midpoint of the interval}
#'   \item{ground_speed_ms}{Ground speed of the interval (m/s)}
#'   \item{ground_heading_deg}{Ground heading of the interval (°)}
#' }
#' @export
compute_night_profiles <- function(
    night_df,
    pressure_levels = c(500, 600, 700, 800, 850, 900, 925, 950, 1000),
    vmp_sl_ms    = 7.5,
    vmr_sl_ms    = NULL,
    alt_grid_m   = c(10, 200, 400, 600, 800, 1000, 1200, 1400,
                     1600, 1800, 2000, 2500, 3000),
    min_speed_ms = 1
) {
  suppressPackageStartupMessages(library(sf))
  if (!exists("pressure_to_altitude_m", mode = "function"))
    stop("Source pressure_to_altitude_m.R before calling this function.")

  # Drop geometry if sf
  if (inherits(night_df, "sf")) {
    coords   <- sf::st_coordinates(night_df[!sf::st_is_empty(night_df), ])
    night_df <- sf::st_drop_geometry(night_df)
    if (!"lon" %in% names(night_df)) night_df$lon <- coords[, "X"]
    if (!"lat" %in% names(night_df)) night_df$lat <- coords[, "Y"]
  }

  night_df <- night_df[order(night_df$timestamp), ]
  n        <- nrow(night_df)

  u_col_prefix <- "era5_u"
  v_col_prefix <- "era5_v"

  # Check wind columns are available
  u_cols <- paste0(u_col_prefix, pressure_levels)
  v_cols <- paste0(v_col_prefix, pressure_levels)
  avail  <- u_cols[u_cols %in% names(night_df)]
  if (length(avail) == 0)
    stop("No era5_u* columns found. Run annotate_era5() first.")
  avail_lvls <- as.numeric(gsub(u_col_prefix, "", avail))

  results <- vector("list", n - 1L)

  for (i in seq_len(n - 1L)) {
    r1 <- night_df[i, ]
    r2 <- night_df[i + 1L, ]

    dt_s <- as.numeric(difftime(r2$timestamp, r1$timestamp, units = "secs"))
    if (is.na(dt_s) || dt_s <= 0) next

    # Ground displacement
    dep <- c(r1$lon, r1$lat)
    arr <- c(r2$lon, r2$lat)
    if (any(is.na(dep)) || any(is.na(arr))) next

    dist_m <- tryCatch(
      as.numeric(sf::st_distance(
        sf::st_sfc(sf::st_point(dep), crs = 4326),
        sf::st_sfc(sf::st_point(arr), crs = 4326)
      )),
      error = function(e) NA_real_
    )
    if (is.na(dist_m) || dist_m / dt_s < min_speed_ms) next

    gs_ms  <- dist_m / dt_s
    hdg_deg <- (atan2(arr[1] - dep[1], arr[2] - dep[2]) * 180 / pi + 360) %% 360

    # Average wind at each level across the two fixes (mid-interval estimate)
    u_vec <- setNames(
      vapply(avail_lvls, function(lv) {
        col <- paste0(u_col_prefix, lv)
        mean(c(as.numeric(r1[[col]]), as.numeric(r2[[col]])), na.rm = TRUE)
      }, numeric(1)),
      as.character(avail_lvls)
    )
    v_vec <- setNames(
      vapply(avail_lvls, function(lv) {
        col <- paste0(v_col_prefix, lv)
        mean(c(as.numeric(r1[[col]]), as.numeric(r2[[col]])), na.rm = TRUE)
      }, numeric(1)),
      as.character(avail_lvls)
    )

    # Use mean barometric pressure for altitude reference
    p0_est <- 1013.25
    if ("era5_msl" %in% names(r1)) {
      p0_est <- mean(c(as.numeric(r1$era5_msl),
                       as.numeric(r2$era5_msl)), na.rm = TRUE) / 100
    }

    prof <- tryCatch(
      compute_airspeed_alt_profile(
        u_levels = u_vec, v_levels = v_vec,
        ground_speed_ms = gs_ms, ground_heading_deg = hdg_deg,
        vmp_sl_ms = vmp_sl_ms, vmr_sl_ms = vmr_sl_ms,
        p0_hpa = p0_est, alt_grid_m = alt_grid_m
      ),
      error = function(e) NULL
    )
    if (is.null(prof)) next

    prof$interval_id       <- i
    prof$t_mid             <- r1$timestamp + dt_s / 2
    prof$ground_speed_ms   <- gs_ms
    prof$ground_heading_deg <- hdg_deg

    results[[i]] <- prof
  }

  out <- do.call(rbind, results[!sapply(results, is.null)])
  if (is.null(out) || nrow(out) == 0) return(data.frame())
  out$t_mid <- as.POSIXct(out$t_mid, tz = "UTC", origin = "1970-01-01")
  rownames(out) <- NULL
  out
}


# ── Plot: Fig. 3-style airspeed-altitude profile ──────────────────────────────

#' Lagerveld-style airspeed vs altitude profile plot
#'
#' Reproduces Figure 3 of Lagerveld et al. (2024) for any night of data:
#' required airspeed (and ground speed) on y-axis vs altitude on x-axis.
#' Individual fix intervals are shown as faint lines, with mean ± ribbon.
#' Vmp and Vmr reference bands are drawn as horizontal shaded regions.
#' Infeasible altitude ranges are shaded grey.
#'
#' @param profiles  Output of \code{\link{compute_night_profiles}}.
#' @param vmp_sl_ms Sea-level Vmp (m/s).
#' @param vmr_sl_ms Sea-level Vmr (m/s). Default NULL → 3^0.25 × Vmp.
#' @param vmp_sd_ms SD of Vmp reference band.  Default 1.0.
#' @param vmr_sd_ms SD of Vmr reference band.  Default 1.1.
#' @param observed_alt_m  Optional scalar or vector: observed bat altitude(s)
#'   in metres.  Drawn as a vertical dashed line (median if multiple).
#' @param species_label   Character label for the subtitle.
#' @param theme_dark Logical. Default \code{FALSE}.
#'
#' @return A ggplot object.
#' @export
plot_lagerveld_profile <- function(
    profiles,
    vmp_sl_ms       = 7.5,
    vmr_sl_ms       = NULL,
    vmp_sd_ms       = 1.0,
    vmr_sd_ms       = 1.1,
    observed_alt_m  = NULL,
    species_label   = NULL,
    theme_dark      = FALSE
) {
  suppressPackageStartupMessages(library(ggplot2))
  suppressPackageStartupMessages(library(dplyr))

  if (is.null(vmr_sl_ms)) vmr_sl_ms <- vmp_sl_ms * 3^(1/4)

  bg <- if (theme_dark) "black" else "white"
  fg <- if (theme_dark) "white" else "black"

  .pt <- function() {
    theme_minimal(base_size = 9) +
      theme(panel.background = element_rect(fill = bg, color = NA),
            plot.background  = element_rect(fill = bg, color = NA),
            text             = element_text(color = fg),
            axis.text        = element_text(color = fg),
            axis.title       = element_text(color = fg),
            panel.grid       = element_line(color = if (theme_dark) "grey25" else "grey90"),
            legend.position  = "right")
  }

  if (nrow(profiles) == 0) {
    return(ggplot() + .pt() +
             labs(title = "No profiles to plot"))
  }

  # Summary: mean ± SD across intervals at each altitude
  summary_df <- profiles %>%
    filter(!is.na(va_required_ms)) %>%
    group_by(alt_m) %>%
    summarise(
      va_mean  = mean(va_required_ms,  na.rm = TRUE),
      va_sd    = sd(va_required_ms,    na.rm = TRUE),
      gs_mean  = mean(ground_speed_ms, na.rm = TRUE),
      gs_sd    = sd(ground_speed_ms,   na.rm = TRUE),
      n_obs    = n(),
      .groups  = "drop"
    )

  # Altitude-adjusted reference speeds
  alt_seq   <- seq(0, max(profiles$alt_m, na.rm = TRUE) + 100, by = 50)
  vmp_curve <- data.frame(alt_m = alt_seq,
                          vmp = adjust_airspeed_for_altitude(vmp_sl_ms, alt_seq),
                          vmr = adjust_airspeed_for_altitude(vmr_sl_ms, alt_seq))

  # Grey infeasible band: merge consecutive infeasible altitudes per interval
  infeasible_df <- profiles %>%
    filter(!feasible & !is.na(va_required_ms)) %>%
    group_by(alt_m) %>%
    summarise(n_infeasible = n(), .groups = "drop")

  # Infeasible shading: altitudes where > 50% of intervals are infeasible
  n_intervals <- length(unique(profiles$interval_id))
  infeasible_shade <- infeasible_df %>%
    filter(n_infeasible / n_intervals > 0.5)

  p <- ggplot() +
    # Infeasible shading
    { if (nrow(infeasible_shade) > 0)
        geom_vline(data = infeasible_shade,
                   aes(xintercept = alt_m),
                   col = "grey70", linewidth = 3, alpha = 0.4) } +
    # Individual interval traces (faint)
    geom_path(data    = profiles %>% filter(!is.na(va_required_ms)),
              aes(alt_m, va_required_ms, group = interval_id,
                  col = as.numeric(t_mid)),
              linewidth = 0.35, alpha = 0.4) +
    scale_color_viridis_c(option = "plasma", guide = "none") +
    # Ground speed ribbon (green)
    geom_ribbon(data = summary_df,
                aes(alt_m,
                    ymin = pmax(0, gs_mean - gs_sd),
                    ymax = gs_mean + gs_sd),
                fill = "#1B7837", alpha = 0.2) +
    geom_path(data = summary_df, aes(alt_m, gs_mean),
              col = "#1B7837", linewidth = 1.2, linetype = "solid") +
    # Airspeed mean ± ribbon (red)
    geom_ribbon(data = summary_df %>% filter(!is.na(va_sd)),
                aes(alt_m,
                    ymin = pmax(0, va_mean - va_sd),
                    ymax = va_mean + va_sd),
                fill = "#D73027", alpha = 0.2) +
    geom_path(data = summary_df, aes(alt_m, va_mean),
              col = "#D73027", linewidth = 1.4) +
    # Vmp band (purple)
    geom_ribbon(data = vmp_curve,
                aes(alt_m,
                    ymin = pmax(0, vmp - vmp_sd_ms),
                    ymax = vmp + vmp_sd_ms),
                fill = "#762A83", alpha = 0.25) +
    geom_path(data = vmp_curve, aes(alt_m, vmp),
              col = "#762A83", linewidth = 1, linetype = "solid") +
    # Vmr band (cyan)
    geom_ribbon(data = vmp_curve,
                aes(alt_m,
                    ymin = pmax(0, vmr - vmr_sd_ms),
                    ymax = vmr + vmr_sd_ms),
                fill = "#35978F", alpha = 0.25) +
    geom_path(data = vmp_curve, aes(alt_m, vmr),
              col = "#35978F", linewidth = 1, linetype = "dashed") +
    # Observed bat altitude
    { if (!is.null(observed_alt_m)) {
        med_alt <- median(observed_alt_m, na.rm = TRUE)
        list(
          geom_vline(xintercept = med_alt, col = "white", linewidth = 1.5,
                     linetype = "dashed"),
          annotate("text", x = med_alt, y = Inf, label = "Observed\naltitude",
                   col = "white", vjust = 1.5, hjust = -0.1, size = 2.5)
        )
      } } +
    # Legend annotation
    annotate("text", x = -Inf, y = Inf, hjust = -0.1, vjust = 1.5,
             label = paste0("Airspeed (red)  |  Ground speed (green)\n",
                            "Vmp (purple)  |  Vmr (teal)  |  Infeasible (grey)"),
             col = fg, size = 2.2) +
    scale_x_continuous(
      name   = "Altitude (m)",
      breaks = c(0, 500, 1000, 1500, 2000, 2500, 3000),
      limits = c(0, NA)
    ) +
    scale_y_continuous(
      name   = "Speed (m/s)",
      limits = c(0, NA)
    ) +
    labs(
      title    = "Airspeed & ground speed vs altitude",
      subtitle = if (!is.null(species_label))
        paste0(species_label,
               "  |  Vmp = ", round(vmp_sl_ms, 1),
               " m/s  |  Vmr = ", round(vmr_sl_ms, 1), " m/s (sea level)")
      else
        paste0("Vmp = ", round(vmp_sl_ms, 1),
               " m/s  |  Vmr = ", round(vmr_sl_ms, 1), " m/s (sea level)")
    ) +
    .pt()

  p
}


# ── Predicted optimal altitudes summary ──────────────────────────────────────

#' Extract predicted optimal altitudes from night profiles
#'
#' For each fix interval in the profile output, identifies:
#' \itemize{
#'   \item The altitude of minimum required airspeed (optimal altitude).
#'   \item Whether the minimum airspeed is within the Vmp or Vmr bands.
#'   \item The feasible altitude range.
#' }
#'
#' @param profiles  Output of \code{\link{compute_night_profiles}}.
#' @param vmp_sl_ms Sea-level Vmp (m/s).
#' @param vmr_sl_ms Sea-level Vmr (m/s). NULL → 3^0.25 × Vmp.
#' @param tolerance_ms  How close required airspeed must be to Vmp or Vmr
#'   to count as "at" that speed (m/s). Default 1.5.
#'
#' @return Data frame with one row per interval:
#' \describe{
#'   \item{interval_id}{Fix interval index}
#'   \item{t_mid}{Interval midpoint (POSIXct)}
#'   \item{optimal_alt_m}{Altitude minimising required airspeed (m)}
#'   \item{min_va_ms}{Required airspeed at the optimal altitude (m/s)}
#'   \item{vmp_alt_range}{Altitude range where va ≈ Vmp (formatted string)}
#'   \item{vmr_alt_range}{Altitude range where va ≈ Vmr (formatted string)}
#'   \item{feasible_alt_min_m, feasible_alt_max_m}{Feasible range (m)}
#'   \item{at_vmp_optimal}{Logical: optimal altitude within Vmp band?}
#'   \item{at_vmr_optimal}{Logical: optimal altitude within Vmr band?}
#' }
#' @export
summarise_optimal_altitudes <- function(
    profiles,
    vmp_sl_ms    = 7.5,
    vmr_sl_ms    = NULL,
    tolerance_ms = 1.5
) {
  suppressPackageStartupMessages(library(dplyr))
  if (is.null(vmr_sl_ms)) vmr_sl_ms <- vmp_sl_ms * 3^(1/4)

  profiles %>%
    filter(!is.na(va_required_ms)) %>%
    group_by(interval_id, t_mid) %>%
    summarise(
      optimal_alt_m      = alt_m[which.min(va_required_ms)],
      min_va_ms          = min(va_required_ms, na.rm = TRUE),
      vmp_at_opt         = adjust_airspeed_for_altitude(vmp_sl_ms, optimal_alt_m),
      vmr_at_opt         = adjust_airspeed_for_altitude(vmr_sl_ms, optimal_alt_m),
      at_vmp_optimal     = abs(min_va_ms - vmp_at_opt) <= tolerance_ms,
      at_vmr_optimal     = abs(min_va_ms - vmr_at_opt) <= tolerance_ms,
      feasible_alt_min_m = if (any(feasible, na.rm = TRUE))
                             min(alt_m[feasible & !is.na(feasible)], na.rm = TRUE)
                           else NA_real_,
      feasible_alt_max_m = if (any(feasible, na.rm = TRUE))
                             max(alt_m[feasible & !is.na(feasible)], na.rm = TRUE)
                           else NA_real_,
      ground_speed_ms    = first(ground_speed_ms),
      ground_heading_deg = first(ground_heading_deg),
      .groups            = "drop"
    )
}
