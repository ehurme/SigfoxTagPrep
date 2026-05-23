# scan_migration_nights.R
# One diagnostic PNG per migration night: departure → arrival map, terrain
# elevation profile along the flight path, and all available sensor panels
# for the night window from bats_full.
#
# Arguments:
#   bats_full   sf/move2  All sensor rows (e.g. leisler / bats_full)
#   bats_loc    sf/move2  Location-only rows (e.g. lloc / bats_loc)
#   mig_nights  data.frame  One row per migration night; must contain:
#                 individual_local_identifier  – animal ID
#                 timestamp                    – daytime fix AFTER the flight
#               Optionally: season, d_displacement
#   out_dir     Output directory (created if absent)
#   overwrite   FALSE = skip existing files
#   elev_z      Zoom level for elevatr::get_elev_raster (default 6)
#   buffer_deg  Degrees of padding around the map extent
#   map_extent  "flight" (default) – extent around departure/arrival only;
#               "track"  – extent covers the individual's full trajectory
#   theme_dark  If TRUE use a dark plot background

scan_migration_nights <- function(
    bats_full,
    bats_loc,
    mig_nights,
    out_dir    = "Plots/ScanTrack/Migration/",
    overwrite  = FALSE,
    elev_z     = 6,
    buffer_deg = 1.5,
    map_extent = c("flight", "track"),
    theme_dark = FALSE
) {
  map_extent <- match.arg(map_extent)
  # bats_full is used for sensor data only — convert to plain data frame to avoid
  # move2's filter/rbind dispatcher rejecting duplicate track IDs.
  bats_full <- as.data.frame(bats_full)
  suppressPackageStartupMessages({
    library(dplyr)
    library(sf)
    library(ggplot2)
    library(ggpubr)
    library(terra)
    library(tidyterra)
    library(rnaturalearth)
    library(elevatr)
  })

  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  # Source extract_elevation_segments if not already loaded
  if (!exists("extract_elevation_segments", mode = "function")) {
    for (.p in c("R/extract_elevation_segments.R",
                 "../SigfoxTagPrep/R/extract_elevation_segments.R")) {
      if (file.exists(.p)) { source(.p); break }
    }
  }

  # Shared panel theme (mirrors scan_track_plot)
  .pt <- function() {
    if (!isTRUE(theme_dark)) {
      theme_minimal(base_size = 10) +
        theme(panel.background = element_rect(fill = "white", color = NA),
              plot.background  = element_rect(fill = "white", color = NA),
              legend.position  = "none")
    } else {
      theme_minimal(base_size = 10) +
        theme(panel.background = element_rect(fill = "black", color = NA),
              plot.background  = element_rect(fill = "black", color = NA),
              panel.grid       = element_line(color = "gray35"),
              text             = element_text(color = "white"),
              axis.text        = element_text(color = "white"),
              axis.title       = element_text(color = "white"),
              legend.position  = "none")
    }
  }

  # TRUE only when column exists and has at least one non-NA value
  .has_col <- function(df, nm) {
    !is.null(nm) && nm %in% names(df) && any(!is.na(df[[nm]]))
  }

  # Pre-extract location coordinates once for fast lookup
  loc_coords <- st_coordinates(bats_loc)
  loc_df <- bats_loc %>%
    st_drop_geometry() %>%
    mutate(lon = loc_coords[, "X"], lat = loc_coords[, "Y"]) %>%
    arrange(individual_local_identifier, timestamp)

  for (i in seq_len(nrow(mig_nights))) {
    night    <- mig_nights[i, ]
    indiv_id <- as.character(night$individual_local_identifier)
    t_arr    <- night$timestamp

    # Departure = last daytime fix strictly before the arrival fix
    prev_row <- loc_df %>%
      filter(individual_local_identifier == indiv_id, timestamp < t_arr) %>%
      slice_max(timestamp, n = 1)

    if (nrow(prev_row) == 0) {
      message("Skipping ", indiv_id, " ", as.Date(t_arr), ": no prior location")
      next
    }
    t_dep <- prev_row$timestamp

    # Arrival = first fix at or after t_arr for this individual
    arr_row <- loc_df %>%
      filter(individual_local_identifier == indiv_id, timestamp >= t_arr) %>%
      slice_min(timestamp, n = 1)

    if (nrow(arr_row) == 0) {
      message("Skipping ", indiv_id, " ", as.Date(t_arr), ": no arrival location")
      next
    }

    # Flag suspiciously short step distances: reported dist_prev says migration
    # but the two location fixes are geographically very close (< 10% of reported).
    if ("dist_prev" %in% names(night) && !is.na(night$dist_prev)) {
      actual_dist_km <- tryCatch(
        as.numeric(sf::st_distance(
          sf::st_sfc(sf::st_point(c(prev_row$lon, prev_row$lat)), crs = 4326),
          sf::st_sfc(sf::st_point(c(arr_row$lon,  arr_row$lat)),  crs = 4326)
        )) / 1000,
        error = function(e) NA_real_
      )
      if (!is.na(actual_dist_km) && actual_dist_km < 0.1 * night$dist_prev)
        message("WARNING: ", indiv_id, " ", as.Date(t_arr),
                " — reported dist=", round(night$dist_prev, 1),
                " km but loc-to-loc=", round(actual_dist_km, 1),
                " km (possible location error)")
    }

    date_str   <- format(as.Date(t_arr), "%Y-%m-%d")
    season_str <- if ("season" %in% names(night) && !is.na(night$season))
                    as.character(night$season) else ""
    out_file <- file.path(out_dir, paste0(
      indiv_id, "_", date_str,
      if (nzchar(season_str)) paste0("_", season_str) else "",
      "_migration.png"
    ))

    if (!overwrite && file.exists(out_file)) {
      message("Exists, skipping: ", basename(out_file))
      next
    }

    try({
      message("Processing: ", basename(out_file))

      # Two-point sf: departure (green circle) and arrival (orange triangle)
      two_pts <- st_as_sf(
        data.frame(
          sequence_number             = 1:2,
          individual_local_identifier = indiv_id,
          role      = c("Departure", "Arrival"),
          timestamp = c(t_dep, t_arr),
          lon = c(prev_row$lon, arr_row$lon),
          lat = c(prev_row$lat, arr_row$lat)
        ),
        coords = c("lon", "lat"), crs = 4326
      )

      # Sensor window: 1.5 days before and after migration departure
      t_win_start <- t_dep - 1.5 * 24 * 3600
      t_win_end   <- t_dep + 1.5 * 24 * 3600

      # Noon markers kept for reference (used in window day generation below)
      t_noon_dep <- as.POSIXct(
        paste0(format(as.Date(t_dep), "%Y-%m-%d"), " 12:00:00"), tz = "UTC"
      )
      t_noon_arr <- as.POSIXct(
        paste0(format(as.Date(t_arr), "%Y-%m-%d"), " 12:00:00"), tz = "UTC"
      )

      # All sensor rows for this individual within the expanded window
      bf_night <- bats_full %>%
        filter(
          individual_local_identifier == indiv_id,
          timestamp >= t_win_start,
          timestamp <= t_win_end
        )

      # Tag type detection
      is_tinyfox <- .has_col(bf_night, "model") &&
        any(grepl("tinyfox", bf_night$model, ignore.case = TRUE), na.rm = TRUE)
      is_nanofox <- .has_col(bf_night, "model") &&
        any(grepl("nanofox", bf_night$model, ignore.case = TRUE), na.rm = TRUE)
      # Fine-scale NanoFox: pressure is instantaneous, not a 3h aggregate
      is_finescale_nanofox <- is_nanofox &&
        .has_col(bf_night, "tag_firmware") &&
        any(grepl("finescale", bf_night$tag_firmware, ignore.case = TRUE), na.rm = TRUE)

      # NanoFox reports burst start in start_timestamp; use that for sensor plots.
      # For TinyFox/others fall back to timestamp.
      bf_night$.plot_time <- if (is_nanofox && .has_col(bf_night, "start_timestamp"))
        bf_night$start_timestamp else bf_night$timestamp

      bf_night <- bf_night %>% arrange(.plot_time)

      if (nrow(bf_night) == 0) {
        message("WARNING: ", indiv_id, " ", as.Date(t_arr),
                " — no sensor rows found in expanded window")
      }

      # TinyFox intra-batch delta detection:
      # ~4 near-redundant messages ~30 min apart cover the same 22h window.
      # Differences between consecutive batch messages (within 2h) reveal
      # finer-scale altitude/activity changes during that ~30 min interval.
      tf_batch_pairs <- NULL
      if (is_tinyfox && nrow(bf_night) > 1) {
        tf_sorted  <- bf_night %>% arrange(timestamp)
        tdiff_secs <- c(Inf, diff(as.numeric(tf_sorted$timestamp)))
        pair_idx   <- which(tdiff_secs <= 2 * 3600)
        if (length(pair_idx) > 0) {
          curr <- tf_sorted[pair_idx,     , drop = FALSE]
          prev <- tf_sorted[pair_idx - 1, , drop = FALSE]
          curr$.press_delta <- if (.has_col(tf_sorted, "tinyfox_pressure_min_last_24h"))
            curr$tinyfox_pressure_min_last_24h - prev$tinyfox_pressure_min_last_24h
          else NA_real_
          tf_batch_pairs <- curr
        }
      }

      # Full trajectory for this individual (needed for map layer and elevation context)
      full_track <- loc_df %>%
        filter(individual_local_identifier == indiv_id) %>%
        arrange(timestamp)

      # Context fixes: fix just before departure and just after arrival
      pre_row  <- loc_df %>%
        filter(individual_local_identifier == indiv_id, timestamp < t_dep) %>%
        slice_max(timestamp, n = 1)
      post_row <- loc_df %>%
        filter(individual_local_identifier == indiv_id, timestamp > t_arr) %>%
        slice_min(timestamp, n = 1)

      # Raster extent covers flight endpoints + pre/post context for extended profile
      extent_pts_df <- data.frame(lon = c(prev_row$lon, arr_row$lon),
                                  lat = c(prev_row$lat, arr_row$lat))
      if (nrow(pre_row)  > 0) extent_pts_df <- rbind(
        data.frame(lon = pre_row$lon,  lat = pre_row$lat),  extent_pts_df)
      if (nrow(post_row) > 0) extent_pts_df <- rbind(
        extent_pts_df, data.frame(lon = post_row$lon, lat = post_row$lat))
      extent_pts <- st_as_sf(extent_pts_df, coords = c("lon", "lat"), crs = 4326)

      # Elevation raster covering flight path + pre/post context
      elev_rast <- try(
        terra::rast(elevatr::get_elev_raster(extent_pts, z = elev_z, expand = 1)),
        silent = TRUE
      )

      elev_profile    <- NULL
      flight_start_km <- 0
      flight_end_km   <- 0

      if (inherits(elev_rast, "try-error")) {
        elev_rast <- NULL
        message("  Elevation fetch failed, continuing without terrain profile")
      } else {
        terra::values(elev_rast)[terra::values(elev_rast) < 0] <- 0
        seg_list <- list()
        cum_dist <- 0

        # Pre-flight context segment (fix before departure → departure)
        if (nrow(pre_row) > 0) {
          pre_pts <- tryCatch(
            st_as_sf(data.frame(lon = c(pre_row$lon, prev_row$lon),
                                lat = c(pre_row$lat, prev_row$lat)),
                     coords = c("lon", "lat"), crs = 4326),
            error = function(e) NULL
          )
          if (!is.null(pre_pts)) {
            pre_elev <- tryCatch(
              extract_elevation_segments(raster = elev_rast, tag = pre_pts),
              error = function(e) NULL
            )
            if (!is.null(pre_elev) && nrow(pre_elev) > 0) {
              pre_elev$segment <- "pre"
              seg_list$pre     <- pre_elev
              cum_dist         <- max(pre_elev$distance_from_origin, na.rm = TRUE)
            }
          }
        }
        flight_start_km <- cum_dist

        # Flight segment (departure → arrival)
        flight_elev <- tryCatch(
          extract_elevation_segments(raster = elev_rast, tag = two_pts),
          error = function(e) NULL
        )
        if (!is.null(flight_elev) && nrow(flight_elev) > 0) {
          flight_elev$distance_from_origin <- flight_elev$distance_from_origin + cum_dist
          flight_elev$segment <- "flight"
          seg_list$flight     <- flight_elev
          cum_dist            <- max(flight_elev$distance_from_origin, na.rm = TRUE)
        }
        flight_end_km <- cum_dist

        # Post-flight context segment (arrival → fix after arrival)
        if (nrow(post_row) > 0) {
          post_pts <- tryCatch(
            st_as_sf(data.frame(lon = c(arr_row$lon, post_row$lon),
                                lat = c(arr_row$lat, post_row$lat)),
                     coords = c("lon", "lat"), crs = 4326),
            error = function(e) NULL
          )
          if (!is.null(post_pts)) {
            post_elev <- tryCatch(
              extract_elevation_segments(raster = elev_rast, tag = post_pts),
              error = function(e) NULL
            )
            if (!is.null(post_elev) && nrow(post_elev) > 0) {
              post_elev$distance_from_origin <- post_elev$distance_from_origin + cum_dist
              post_elev$segment <- "post"
              seg_list$post     <- post_elev
            }
          }
        }

        if (length(seg_list) > 0) {
          elev_profile <- do.call(rbind, seg_list)
          rownames(elev_profile) <- NULL
        }
      }

      # Map extent
      if (map_extent == "track") {
        xlims <- range(full_track$lon, na.rm = TRUE) + c(-1, 1) * buffer_deg
        ylims <- range(full_track$lat, na.rm = TRUE) + c(-1, 1) * buffer_deg
      } else {
        xlims <- c(min(prev_row$lon, arr_row$lon) - buffer_deg,
                   max(prev_row$lon, arr_row$lon) + buffer_deg)
        ylims <- c(min(prev_row$lat, arr_row$lat) - buffer_deg,
                   max(prev_row$lat, arr_row$lat) + buffer_deg)
      }

      # Compute distance from the same prev_row/arr_row used for the elevation profile
      # and map, so the title matches the plot geometry exactly.
      dist_km <- tryCatch(
        round(as.numeric(sf::st_distance(
          sf::st_sfc(sf::st_point(c(prev_row$lon, prev_row$lat)), crs = 4326),
          sf::st_sfc(sf::st_point(c(arr_row$lon,  arr_row$lat)),  crs = 4326)
        )) / 1000, 0),
        error = function(e) NA_real_
      )

      title_str <- paste0(
        indiv_id, "  |  ",
        format(t_dep, "%Y-%m-%d %H:%M"), " \u2013 ", format(t_arr, "%Y-%m-%d %H:%M"),
        if (!is.na(dist_km)) paste0("  |  ", dist_km, " km") else "",
        if (nzchar(season_str)) paste0("  |  ", season_str) else ""
      )

      # ---- Map panel ----
      p_map <- ggplot() +
        { if (!is.null(elev_rast)) geom_spatraster(data = elev_rast) } +
        { if (!is.null(elev_rast)) scale_fill_hypso_c(
            name = "Elevation (m)", palette = "arctic_bathy", limits = c(0, 4000)) } +
        geom_sf(data = ne_countries(scale = 10), fill = NA,
                color = if (theme_dark) "gray70" else "gray80") +
        # full track — thin grey line + faint points + sequence labels
        geom_path(
          data = full_track,
          aes(lon, lat),
          col = "grey60", linewidth = 0.35, alpha = 0.6
        ) +
        geom_point(
          data = full_track,
          aes(lon, lat),
          col = "grey60", size = 0.8, alpha = 0.5
        ) +
        { if ("sequence_number" %in% names(full_track))
            geom_text(
              data = full_track %>% filter(!is.na(sequence_number)),
              aes(lon, lat, label = sequence_number),
              col = "grey80", size = 2, vjust = -0.9, hjust = 0.5,
              check_overlap = TRUE
            )
        } +
        # migration segment — dashed white line
        geom_segment(
          aes(x = prev_row$lon, y = prev_row$lat,
              xend = arr_row$lon, yend = arr_row$lat),
          col = "white", linewidth = 1.1, linetype = "dashed"
        ) +
        # departure (green circle) and arrival (orange triangle) + sequence labels
        geom_point(
          data = data.frame(lon = prev_row$lon, lat = prev_row$lat),
          aes(lon, lat), col = "#4DAF4A", size = 4, shape = 16
        ) +
        geom_point(
          data = data.frame(lon = arr_row$lon, lat = arr_row$lat),
          aes(lon, lat), col = "#FF7F00", size = 4, shape = 17
        ) +
        { if ("sequence_number" %in% names(prev_row) && !is.na(prev_row$sequence_number))
            geom_text(
              data = data.frame(lon = prev_row$lon, lat = prev_row$lat,
                                label = as.character(prev_row$sequence_number)),
              aes(lon, lat, label = label),
              col = "#4DAF4A", size = 2.8, vjust = -1.3, hjust = 0.5, fontface = "bold"
            )
        } +
        { if ("sequence_number" %in% names(arr_row) && !is.na(arr_row$sequence_number))
            geom_text(
              data = data.frame(lon = arr_row$lon, lat = arr_row$lat,
                                label = as.character(arr_row$sequence_number)),
              aes(lon, lat, label = label),
              col = "#FF7F00", size = 2.8, vjust = -1.3, hjust = 0.5, fontface = "bold"
            )
        } +
        coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
        labs(title = title_str, x = "Longitude", y = "Latitude") +
        .pt() +
        theme(plot.title = element_text(size = 8, face = "bold"))

      # ---- Sensor panels ----
      panels <- list()

      # Shared x-axis: 1.5 days either side of departure.
      # Noon vlines (grey solid) for each day in window; midnight (dotted) marks nights.
      # Departure (green) and arrival (orange) dashed vlines mark actual fix times.
      .win_days <- seq.Date(as.Date(t_win_start), as.Date(t_win_end), by = "day")
      .t_noons  <- as.POSIXct(paste0(.win_days, " 12:00:00"), tz = "UTC")
      .t_mids   <- as.POSIXct(paste0(.win_days, " 00:00:00"), tz = "UTC")
      .t_noons  <- .t_noons[.t_noons >= t_win_start & .t_noons <= t_win_end]
      .t_mids   <- .t_mids[.t_mids   >  t_win_start & .t_mids  <  t_win_end]

      # Fix closest to noon on each day in window (hollow diamonds on sensor panels)
      .noon_list <- lapply(.win_days, function(d) {
        noon_t <- as.POSIXct(paste0(d, " 12:00:00"), tz = "UTC")
        bf_night %>%
          filter(as.Date(.plot_time) == d) %>%
          mutate(.noon_diff = abs(as.numeric(difftime(.plot_time, noon_t, units = "secs")))) %>%
          slice_min(.noon_diff, n = 1, with_ties = FALSE)
      })
      .noon_list <- .noon_list[sapply(.noon_list, function(x) !is.null(x) && nrow(x) > 0)]
      noon_pts   <- if (length(.noon_list) > 0) do.call(rbind, .noon_list) else data.frame()

      .tx <- scale_x_datetime(
        limits = c(t_win_start, t_win_end),
        expand = expansion(mult = 0)
      )
      .vlines <- c(
        lapply(.t_noons, function(t)
          geom_vline(xintercept = as.numeric(t), linetype = "solid",
                     col = "grey40", linewidth = 0.5)),
        lapply(.t_mids, function(t)
          geom_vline(xintercept = as.numeric(t), linetype = "dotted",
                     col = "grey60", linewidth = 0.4)),
        list(
          geom_vline(xintercept = as.numeric(t_dep), linetype = "dashed",
                     col = "#4DAF4A", linewidth = 0.7),
          geom_vline(xintercept = as.numeric(t_arr), linetype = "dashed",
                     col = "#FF7F00", linewidth = 0.7)
        )
      )

      # Helper: add noon-closest points (hollow diamonds) for a given y-column
      .add_noon_pts <- function(p, col) {
        if (!is.null(noon_pts) && nrow(noon_pts) > 0 && col %in% names(noon_pts)) {
          valid_pts <- noon_pts[!is.na(noon_pts[[col]]), , drop = FALSE]
          if (nrow(valid_pts) > 0)
            p <- p + geom_point(data = valid_pts,
                                aes(.plot_time, .data[[col]]),
                                shape = 23, size = 2.5, fill = "white",
                                col = "grey30", stroke = 0.8, inherit.aes = FALSE)
        }
        p
      }

      # Normalise tag_fell_off
      tag_fell <- if (.has_col(bf_night, "tag_fell_off"))
        as.logical(bf_night$tag_fell_off) else rep(FALSE, nrow(bf_night))
      tag_fell[is.na(tag_fell)] <- FALSE

      # VeDBA
      vedba_col <- dplyr::case_when(
        is_tinyfox && .has_col(bf_night, "tinyfox_total_vedba") ~ "tinyfox_total_vedba",
        .has_col(bf_night, "tinyfox_vedba_burst_sum")           ~ "tinyfox_vedba_burst_sum",
        .has_col(bf_night, "vpm")                               ~ "vpm",
        .has_col(bf_night, "vedba")                             ~ "vedba",
        TRUE                                                     ~ NA_character_
      )
      if (!is.na(vedba_col)) {
        bf_night$tag_fell_chr <- as.character(tag_fell)
        vedba_ylim <- if (is_nanofox) c(0, 10000) else c(0, NA)
        p_vedba <- ggplot(bf_night, aes(.plot_time, .data[[vedba_col]])) +
          .vlines +
          geom_path(col = "steelblue") +
          geom_point(aes(col = tag_fell_chr), size = 1.5) +
          scale_color_manual(values = c("FALSE" = "steelblue", "TRUE" = "red"),
                             guide  = "none") +
          scale_y_continuous(limits = vedba_ylim) +
          .tx + .pt() +
          ylab(if (vedba_col == "tinyfox_total_vedba") "Total VeDBA\n(cumulative)" else "VeDBA") +
          xlab("Time")
        p_vedba <- .add_noon_pts(p_vedba, vedba_col)
        panels$vedba <- p_vedba
      }

      # Activity % (TinyFox only)
      if (is_tinyfox && .has_col(bf_night, "tinyfox_activity_percent_last_24")) {
        p_act <- ggplot(bf_night,
            aes(.plot_time, tinyfox_activity_percent_last_24)) +
          .vlines +
          geom_path(col = "forestgreen") +
          geom_point(col = "forestgreen", size = 1.5) +
          scale_y_continuous(limits = c(0, 100)) + .tx +
          .pt() + ylab("Activity\n(% above threshold)") + xlab("Time")
        p_act <- .add_noon_pts(p_act, "tinyfox_activity_percent_last_24")
        panels$activity <- p_act
      }

      # Temperature
      temp_cols <- if (is_tinyfox) {
        c("tinyfox_temperature_min_last_24h", "tinyfox_temperature_max_last_24h")
      } else {
        c("temperature_min", "temperature_max", "external_temperature", "avg_temp")
      }
      temp_cols   <- temp_cols[sapply(temp_cols, function(c) .has_col(bf_night, c))]
      temp_colors <- c("darkblue", "tomato", "darkred", "orange")

      if (length(temp_cols) > 0) {
        p_temp <- ggplot() + .vlines +
          scale_y_continuous(limits = c(-14, 45)) + .tx + .pt() +
          ylab("Temperature (\u00b0C)") + xlab("Time")
        for (j in seq_along(temp_cols)) {
          p_temp <- p_temp +
            geom_path(data  = bf_night, aes(.plot_time, .data[[temp_cols[j]]]),
                      col = temp_colors[j]) +
            geom_point(data = bf_night, aes(.plot_time, .data[[temp_cols[j]]]),
                       col = temp_colors[j], size = 1.5)
          p_temp <- .add_noon_pts(p_temp, temp_cols[j])
        }
        panels$temperature <- p_temp
      }

      # Pressure — rendering geometry differs by tag type:
      #   TinyFox        : tinyfox_pressure_min_last_24h
      #                    horizontal segment over 22h window (start_timestamp → timestamp)
      #   NanoFox FS     : barometric_pressure, instantaneous → dots + path
      #   NanoFox (3h)   : barometric_pressure, min over 3h window → horizontal segment
      #                    (start_timestamp → timestamp)
      press_col <- dplyr::case_when(
        is_tinyfox && .has_col(bf_night, "tinyfox_pressure_min_last_24h")
                                             ~ "tinyfox_pressure_min_last_24h",
        .has_col(bf_night, "barometric_pressure") ~ "barometric_pressure",
        TRUE                                 ~ NA_character_
      )

      if (!is.na(press_col)) {
        bf_press <- bf_night %>% filter(!is.na(.data[[press_col]]))

        # Determine segment start: prefer stored start_timestamp, fall back to computed.
        # Must be explicitly POSIXct for geom_segment to map onto scale_x_datetime.
        has_start_ts <- "start_timestamp" %in% names(bf_press) &&
                        any(!is.na(bf_press$start_timestamp))
        if (has_start_ts) {
          bf_press$t_start <- as.POSIXct(bf_press$start_timestamp, tz = "UTC")
        } else if (is_tinyfox) {
          bf_press$t_start <- as.POSIXct(bf_press$timestamp - 22 * 3600, tz = "UTC")
        } else {
          bf_press$t_start <- as.POSIXct(bf_press$timestamp - 3 * 3600, tz = "UTC")
        }

        if (is_finescale_nanofox) {
          # Instantaneous pressure — dots connected by path
          p_press <- ggplot(bf_press, aes(.plot_time, .data[[press_col]])) +
            .vlines +
            geom_path(col = "purple") +
            geom_point(col = "purple", size = 1.5) +
            scale_y_continuous(limits = c(600, 1050)) +
            .tx + .pt() + ylab("Pressure (mbar)") + xlab("Time")
          p_press <- .add_noon_pts(p_press, press_col)
          panels$pressure <- p_press
        } else {
          # Aggregate (3h NanoFox or 22h TinyFox) — horizontal segment per measurement
          p_press <- ggplot(bf_press) +
            .vlines +
            geom_segment(aes(x = t_start, xend = timestamp,
                             y = .data[[press_col]], yend = .data[[press_col]]),
                         col = "purple", linewidth = 1.5) +
            geom_point(aes(timestamp, .data[[press_col]]),
                       col = "purple", size = 2, shape = 16) +
            scale_y_continuous(limits = c(600, 1050)) +
            .tx + .pt() + ylab("Pressure (mbar)") + xlab("Time")
          p_press <- .add_noon_pts(p_press, press_col)
          # TinyFox intra-batch: highlight pressure changes between consecutive batch
          # messages (asterisk + delta label; ↓ means bat flew higher in that ~30min)
          if (is_tinyfox && !is.null(tf_batch_pairs) && nrow(tf_batch_pairs) > 0 &&
              press_col %in% names(tf_batch_pairs) &&
              ".press_delta" %in% names(tf_batch_pairs)) {
            hl_press <- tf_batch_pairs %>%
              filter(!is.na(.data[[press_col]]), !is.na(.press_delta), .press_delta != 0) %>%
              mutate(.delta_lbl = ifelse(.press_delta < 0,
                paste0("\u2193", round(abs(.press_delta), 1)),
                paste0("\u2191", round(.press_delta, 1))))
            if (nrow(hl_press) > 0) {
              p_press <- p_press +
                geom_point(data = hl_press, aes(timestamp, .data[[press_col]]),
                           shape = 8, size = 3, col = "orange", inherit.aes = FALSE) +
                geom_text(data = hl_press,
                          aes(timestamp, .data[[press_col]], label = .delta_lbl),
                          vjust = -1.2, size = 2.5, col = "orange", inherit.aes = FALSE)
            }
          }
          panels$pressure <- p_press
        }
      }

      # ---- Elevation profile + pressure-derived altitude ----
      if (!is.null(elev_profile)) {
        p_elev <- ggplot(elev_profile, aes(distance_from_origin, elevation)) +
          geom_ribbon(aes(ymax = elevation, ymin = 0), fill = "gray70", col = NA) +
          geom_path(col = "gray30") +
          .pt() +
          ylab("Elevation (m)") + xlab("Distance (km)")

        # Segment boundary vlines: green = departure fix, orange = arrival fix
        if (flight_start_km > 0)
          p_elev <- p_elev +
            geom_vline(xintercept = flight_start_km, linetype = "dashed",
                       col = "#4DAF4A", linewidth = 0.7)
        if (flight_end_km > flight_start_km)
          p_elev <- p_elev +
            geom_vline(xintercept = flight_end_km, linetype = "dashed",
                       col = "#FF7F00", linewidth = 0.7)

        # Pressure-derived altitude, mapped onto the flight segment only
        if (!is.na(press_col) && nrow(bf_night) > 0) {
          total_dur <- as.numeric(difftime(t_arr, t_dep, units = "secs"))
          flight_km <- flight_end_km - flight_start_km

          if (total_dur > 0 && flight_km > 0) {
            bf_alt <- bf_night %>%
              filter(!is.na(.data[[press_col]])) %>%
              mutate(
                frac    = pmax(0, pmin(1,
                            as.numeric(difftime(.plot_time, t_dep, units = "secs")) / total_dur)),
                dist_km = flight_start_km + frac * flight_km,
                alt_m   = 44330 * (1 - (.data[[press_col]] / 1013.25)^0.1903)
              )
            if (nrow(bf_alt) > 0)
              p_elev <- p_elev +
                geom_path(data  = bf_alt, aes(dist_km, alt_m),
                          col = "purple", linetype = "dashed", inherit.aes = FALSE) +
                geom_point(data = bf_alt, aes(dist_km, alt_m),
                           col = "purple", size = 2, inherit.aes = FALSE)
          }
        }

        panels$elevation <- p_elev
      }

      # ---- Assemble ----
      if (length(panels) == 0) {
        p_all <- p_map
      } else {
        ncols <- if (length(panels) <= 2) 1L else 2L
        nrows <- ceiling(length(panels) / ncols)
        grid  <- ggarrange(plotlist = unname(panels), ncol = ncols, nrow = nrows)
        p_all <- ggarrange(p_map, grid, ncol = 1, heights = c(2.5, nrows * 1.0))
      }

      fig_h <- 4.5 + ceiling(length(panels) / 2) * 2.0
      ggsave(p_all, filename = out_file, width = 9, height = fig_h, bg = "white")
      message("Saved: ", basename(out_file))
    })
  }

  invisible(NULL)
}
