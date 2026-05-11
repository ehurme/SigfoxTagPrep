library(terra)
library(tidyterra)
library(dplyr)
library(sf)
library(ggplot2)
library(rnaturalearth)
library(elevatr)
library(move2)
library(ggpubr)

# Source extract_elevation_segments from whichever path resolves
if (!exists("extract_elevation_segments", mode = "function")) {
  for (.p in c("R/extract_elevation_segments.R",
               "../SigfoxTagPrep/R/extract_elevation_segments.R")) {
    if (file.exists(.p)) { source(.p); break }
  }
  rm(.p)
}

# Convert pressure (mbar) to altitude (m) via standard barometric formula
.pressure_to_alt_m <- function(p_mbar, p0_mbar = 1013.25) {
  44330 * (1 - (p_mbar / p0_mbar)^0.1903)
}

# ── scan_tracks() ─────────────────────────────────────────────────────────────
# Generate scan-track diagnostic plots for one or more bat individuals.
#
# Sensor panels are built dynamically per tag type:
#   All tags  : step distance, temperature, pressure, altitude profile
#   TinyFox   : total VeDBA (cumulative), activity %, min+max temp, pressure
#   NanoFox   : vedba / vedba_burst_sum, external_temperature, barometric_pressure
#   30Days*   : temperature_min + temperature_max, barometric_pressure
#
# Arguments:
#   bats_full  move2 object with all sensor rows
#   bats_loc   move2 object with location rows only
#   out_dir    directory to write PNG files
#   tags       character vector of individual_local_identifier; NULL = all
#   overwrite  FALSE (default) skips existing files; TRUE regenerates
#   elev_z     zoom level for elevatr::get_elev_raster (default 5)
# ─────────────────────────────────────────────────────────────────────────────
scan_tracks <- function(
    bats_full,
    bats_loc,
    out_dir   = "../../../Dropbox/MPI/Noctule/Plots/ScanTrack/",
    tags      = NULL,
    overwrite = FALSE,
    elev_z    = 5
) {

  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  if (is.null(tags))
    tags <- unique(bats_full$individual_local_identifier)

  # Shared minimal panel theme
  .pt <- function() {
    theme_minimal() +
      theme(legend.position  = "none",
            panel.background = element_rect(fill = "white", color = "white"),
            plot.background  = element_rect(fill = "white", color = "white"))
  }

  # Helper: TRUE if column exists and has at least one non-NA value
  .has_data <- function(df, col) {
    col %in% names(df) && any(!is.na(df[[col]]))
  }

  for (tag in tags) {

    b_full <- bats_full %>% filter(individual_local_identifier == tag)
    b_loc  <- bats_loc  %>% filter(individual_local_identifier == tag)

    if (nrow(b_loc) < 2) {
      message("Skipping ", tag, ": fewer than 2 locations")
      next
    }

    sp  <- b_full$species[1]
    yr  <- year(b_full$timestamp[1])
    sex <- b_full$sex[1]

    out_file <- file.path(out_dir, paste0(sp, "_", yr, "_", tag, "_", sex, ".png"))

    if (!overwrite && file.exists(out_file)) {
      message("Exists, skipping: ", basename(out_file))
      next
    }

    try({
      # Drop empty geometries
      b_loc_valid <- b_loc[!st_is_empty(b_loc), ]
      if (nrow(b_loc_valid) < 2) {
        message("Skipping ", tag, ": fewer than 2 valid geometries")
        next
      }

      # ── Tag type detection ─────────────────────────────────────────────────
      b_full_df <- b_full %>% st_drop_geometry()

      is_tinyfox <- any(grepl("tinyfox", b_full_df$model, ignore.case = TRUE),
                        na.rm = TRUE) ||
                    any(b_full_df$tag_firmware %in% c("V13", "V13P", "V14P"),
                        na.rm = TRUE)

      # ── Elevation raster ───────────────────────────────────────────────────
      e    <- terra::rast(elevatr::get_elev_raster(b_loc_valid, z = elev_z, expand = 1))
      elev <- extract_elevation_segments(raster = e, tag = b_loc_valid)

      # ── Coordinates & step distances ───────────────────────────────────────
      coords  <- st_coordinates(st_transform(b_loc_valid, 3035))
      step_m  <- c(NA_real_, sqrt(diff(coords[, "X"])^2 + diff(coords[, "Y"])^2))
      lon_lat <- st_coordinates(b_loc_valid)

      b_df <- b_loc_valid %>%
        st_drop_geometry() %>%
        mutate(
          lon         = lon_lat[, "X"],
          lat         = lon_lat[, "Y"],
          step_m      = step_m,
          cum_dist_km = cumsum(replace(step_m, is.na(step_m), 0)) / 1000,
          seq_plot    = if ("sequence_number" %in% names(.)) sequence_number else row_number()
        )

      # ── Tag metadata & track summary ───────────────────────────────────────
      mdl      <- if (!is.na(b_full_df$model[1]))        b_full_df$model[1]        else "unknown"
      firmware <- if (!is.na(b_full_df$tag_firmware[1])) b_full_df$tag_firmware[1] else "unknown"

      n_fixes    <- nrow(b_df)
      date_start <- format(min(b_df$timestamp, na.rm = TRUE), "%Y-%m-%d")
      date_end   <- format(max(b_df$timestamp, na.rm = TRUE), "%Y-%m-%d")
      duration_d <- round(as.numeric(difftime(max(b_df$timestamp, na.rm = TRUE),
                                              min(b_df$timestamp, na.rm = TRUE),
                                              units = "days")), 1)
      max_disp   <- if (.has_data(b_df, "displacement"))
                      round(max(b_df$displacement, na.rm = TRUE), 0) else NA
      total_dist <- round(sum(b_df$step_m, na.rm = TRUE) / 1000, 0)

      stats_sub <- paste0(
        "Model: ", mdl, "  |  Firmware: ", firmware, "\n",
        date_start, " \u2013 ", date_end, "  |  ", duration_d, " days  |  ",
        n_fixes, " fixes  |  ",
        if (!is.na(max_disp)) paste0("max disp: ", max_disp, " km  |  ") else "",
        "total dist: ", total_dist, " km"
      )

      xlims <- c(min(b_df$lon) - 2, max(b_df$lon) + 1)
      ylims <- c(min(b_df$lat) - 1, max(b_df$lat) + 1)

      # Add seq_plot to b_loc_valid so buffered uncertainty circles inherit it
      b_loc_valid$seq_plot <- if ("sequence_number" %in% names(b_loc_valid))
        b_loc_valid$sequence_number else seq_len(nrow(b_loc_valid))

      r_idx <- !is.na(b_loc_valid$sigfox_computed_location_radius)
      r <- st_buffer(b_loc_valid[r_idx, ],
                     dist = b_loc_valid$sigfox_computed_location_radius[r_idx])

      # ── Panel: Map ─────────────────────────────────────────────────────────
      map <- ggplot() +
        geom_spatraster(data = e) +
        scale_fill_hypso_c(name = "Elevation (m)", palette = "arctic_bathy",
                           limits = c(0, 4000)) +
        geom_sf(data = ne_countries(scale = 10), fill = NA, col = "white") +
        geom_sf(data = r, alpha = 0.1, aes(col = seq_plot)) +
        geom_path(data  = b_df, aes(lon, lat, col = seq_plot), linewidth = 1) +
        geom_point(data = b_df, aes(lon, lat, col = seq_plot)) +
        scale_color_viridis_c(name = "Sequence", option = "A") +
        coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
        labs(title    = paste("ID:", tag, "| Species:", sp, "| Sex:", sex, "| Year:", yr),
             subtitle = stats_sub,
             x = "Longitude", y = "Latitude") +
        theme(legend.position  = "none",
              panel.background = element_rect(fill = "white", color = "white"),
              plot.background  = element_rect(fill = "white", color = "white"),
              plot.title       = element_text(size = 8, face = "bold"),
              plot.subtitle    = element_text(size = 6.5, colour = "grey30"))

      # ── Sensor panels (built dynamically) ──────────────────────────────────
      sensor_panels <- list()

      # Panel: Step distance
      sensor_panels[["distance"]] <- ggplot(b_df, aes(timestamp, step_m / 1000)) +
        geom_point(aes(size = sigfox_computed_location_radius, col = seq_plot)) +
        geom_path(aes(col = seq_plot)) +
        scale_color_viridis_c(option = "A") +
        .pt() + ylab("Step distance (km)") + xlab("Date")

      # Panel: VeDBA
      # TinyFox: tinyfox_total_vedba (cumulative sum stored on tag) vs time
      # Others:  tinyfox_vedba_burst_sum or vedba
      p_vedba <- ggplot() + .pt() + ylab("VeDBA") + xlab("Date")

      if (is_tinyfox && .has_data(b_full_df, "tinyfox_total_vedba")) {
        p_vedba <- p_vedba +
          geom_path(data = b_full_df,
                    aes(timestamp, tinyfox_total_vedba), col = "blue") +
          geom_point(data = b_full_df,
                     aes(timestamp, tinyfox_total_vedba), col = "blue", size = 1.5) +
          ylab("Total VeDBA (cumulative)")
      } else {
        vedba_col <- dplyr::case_when(
          .has_data(b_full_df, "tinyfox_vedba_burst_sum") ~ "tinyfox_vedba_burst_sum",
          .has_data(b_full_df, "vedba")                   ~ "vedba",
          TRUE                                             ~ NA_character_
        )
        if (!is.na(vedba_col))
          p_vedba <- p_vedba +
            geom_path(data  = b_full_df, aes(timestamp, .data[[vedba_col]]), col = "blue") +
            geom_point(data = b_full_df, aes(timestamp, .data[[vedba_col]]),
                       col = "blue", size = 1.5)
      }
      sensor_panels[["vedba"]] <- p_vedba

      # Panel: Activity % — TinyFox only
      if (is_tinyfox && .has_data(b_full_df, "tinyfox_activity_percent_last_24")) {
        sensor_panels[["activity"]] <- ggplot(b_full_df) +
          geom_path(aes(timestamp, tinyfox_activity_percent_last_24), col = "forestgreen") +
          geom_point(aes(timestamp, tinyfox_activity_percent_last_24),
                     col = "forestgreen", size = 1.5) +
          ylim(c(0, 100)) +
          .pt() + ylab("Activity (% above threshold)") + xlab("Date")
      }

      # Panel: Temperature
      # TinyFox          : tinyfox_temperature_min_last_24h (darkblue) +
      #                    tinyfox_temperature_max_last_24h (tomato)
      # 30Days*          : temperature_min (steelblue) + temperature_max (tomato)
      # NanoFox / others : external_temperature (darkred)
      p_temperature <- ggplot() +
        ylim(c(-15, 45)) +
        .pt() + ylab("Temperature (\u00b0C)") + xlab("Date")

      if (is_tinyfox) {
        if (.has_data(b_full_df, "tinyfox_temperature_min_last_24h"))
          p_temperature <- p_temperature +
            geom_path(data = b_full_df,
                      aes(timestamp, tinyfox_temperature_min_last_24h), col = "darkblue")
        if (.has_data(b_full_df, "tinyfox_temperature_max_last_24h"))
          p_temperature <- p_temperature +
            geom_path(data = b_full_df,
                      aes(timestamp, tinyfox_temperature_max_last_24h), col = "tomato")
      } else {
        # 30Days / 30DaysFineScalePressure min + max
        if (.has_data(b_full_df, "temperature_min"))
          p_temperature <- p_temperature +
            geom_path(data = b_full_df,
                      aes(timestamp, temperature_min), col = "steelblue")
        if (.has_data(b_full_df, "temperature_max"))
          p_temperature <- p_temperature +
            geom_path(data = b_full_df,
                      aes(timestamp, temperature_max), col = "tomato")
        # NanoFox average temperature
        if (.has_data(b_full_df, "external_temperature"))
          p_temperature <- p_temperature +
            geom_path(data = b_full_df,
                      aes(timestamp, external_temperature), col = "darkred")
      }
      sensor_panels[["temperature"]] <- p_temperature

      # Panel: Pressure
      p_pressure <- ggplot() + .pt() + ylab("Pressure (mbar)") + xlab("Date")

      if (.has_data(b_full_df, "barometric_pressure"))
        p_pressure <- p_pressure +
          geom_path(data = b_full_df,
                    aes(timestamp, barometric_pressure / 100), col = "purple")

      if (.has_data(b_full_df, "tinyfox_pressure_min_last_24h"))
        p_pressure <- p_pressure +
          geom_path(data = b_full_df,
                    aes(timestamp, tinyfox_pressure_min_last_24h), col = "darkorchid")

      sensor_panels[["pressure"]] <- p_pressure

      # ── Altitude profile ───────────────────────────────────────────────────
      # Layers (bottom to top):
      #   gray20  — terrain elevation from DEM
      #   orange  — GPS/GNSS altitude_m from location fixes
      #   purple dashed — pressure-derived altitude (where pressure available)
      #
      # Pressure-derived altitude uses location-row pressure columns (b_df).
      # tinyfox_pressure_min_last_24h is already in mbar.
      # barometric_pressure (NanoFox) is in Pa → divide by 100.
      b_df$alt_pressure_m <- NA_real_
      if (.has_data(b_df, "tinyfox_pressure_min_last_24h")) {
        b_df$alt_pressure_m <- .pressure_to_alt_m(b_df$tinyfox_pressure_min_last_24h)
      } else if (.has_data(b_df, "barometric_pressure")) {
        b_df$alt_pressure_m <- .pressure_to_alt_m(b_df$barometric_pressure / 100)
      }

      p_altitude <- ggplot() +
        geom_path(data = elev, aes(distance_from_origin, elevation), col = "gray20") +
        geom_path(data = b_df,  aes(cum_dist_km, altitude_m),        col = "orange") +
        { if (any(!is.na(b_df$alt_pressure_m)))
            geom_path(data = b_df, aes(cum_dist_km, alt_pressure_m),
                      col = "purple", linetype = "dashed")
        } +
        .pt() +
        ylab("Elevation / Altitude (m)") + xlab("Cumulative distance (km)")

      # ── Assemble & save ────────────────────────────────────────────────────
      n_panels  <- length(sensor_panels)
      ncol_grid <- if (n_panels <= 4) 2L else 3L
      nrow_grid <- ceiling(n_panels / ncol_grid)

      p_sensors <- ggarrange(
        plotlist = unname(sensor_panels),
        ncol = ncol_grid, nrow = nrow_grid
      )

      p <- ggarrange(
        map, p_sensors, p_altitude,
        ncol = 1,
        heights = c(2.5, nrow_grid * 1.0, 0.6)
      )

      fig_height <- 3.5 + nrow_grid * 2.2
      ggsave(p, filename = out_file, width = if (ncol_grid == 3) 9 else 6,
             height = fig_height)
      message("Saved: ", basename(out_file))
    })
  }

  invisible(NULL)
}
