library(terra)
library(tidyterra)
library(dplyr)
library(sf)
library(ggplot2)
library(rnaturalearth)
library(elevatr)
library(move2)
library(ggpubr)
library(furrr)
library(future)
library(purrr)
library(lubridate)

# Source extract_elevation_segments from whichever path resolves
if (!exists("extract_elevation_segments", mode = "function")) {
  for (.p in c(
    "R/extract_elevation_segments.R",
    "../SigfoxTagPrep/R/extract_elevation_segments.R"
  )) {
    if (file.exists(.p)) {
      source(.p)
      break
    }
  } 
  rm(.p)
}

# Convert pressure (mbar) to altitude (m) via standard barometric formula
.pressure_to_alt_m <- function(p_mbar, p0_mbar = 1013.25) {
  44330 * (1 - (p_mbar / p0_mbar)^0.1903)
}

.has_data <- function(df, col) {
  col %in% names(df) && any(!is.na(df[[col]]))
}

.first_or <- function(x, default = NA_character_) {
  if (length(x) == 0L || is.na(x[1])) return(default)
  as.character(x[1])
}

.empty_like <- function(x) {
  x[0, , drop = FALSE]
}

.clean_filename_part <- function(x) {
  x <- ifelse(is.na(x) | !nzchar(x), "unknown", x)
  gsub("[^A-Za-z0-9_.-]+", "_", x)
}

.split_by_deployment <- function(x) {
  if (!"deployment_id" %in% names(x)) {
    stop("Input object must contain a deployment_id column.", call. = FALSE)
  }

  ids <- as.character(x$deployment_id)
  split(x, ids, drop = TRUE)
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

.pick_first_col <- function(df, candidates, require_data = TRUE) {
  for (cc in candidates) {
    if (cc %in% names(df)) {
      if (!require_data || any(!is.na(df[[cc]]))) {
        return(cc)
      }
    }
  }
  NA_character_
}

# Snap a vector of daily timestamps to the nearest timestamp in raw_ts.
# Returns daily_ts unchanged when raw_ts is empty.
.snap_to_nearest_ts <- function(daily_ts, raw_ts) {
  t_r <- sort(as.numeric(raw_ts[!is.na(raw_ts)]))
  if (!length(t_r)) return(daily_ts)
  t_d <- as.numeric(daily_ts)
  idx_l <- findInterval(t_d, t_r, all.inside = TRUE)
  idx_r <- pmin(idx_l + 1L, length(t_r))
  use_r <- abs(t_d - t_r[idx_r]) < abs(t_d - t_r[idx_l])
  as.POSIXct(t_r[ifelse(use_r, idx_r, idx_l)], origin = "1970-01-01",
             tz = attr(daily_ts[1L], "tzone") %||% "UTC")
}

# Build a logical mask: TRUE on rows of df where any of cols is non-NA.
.mk_ts_mask <- function(df, cols) {
  m <- rep(FALSE, nrow(df))
  for (cc in cols) if (cc %in% names(df)) m <- m | !is.na(df[[cc]])
  m
}

.prepare_daily_df <- function(b_daily) {
  if (is.null(b_daily) || nrow(b_daily) == 0) {
    return(data.frame())
  }

  has_geom <- inherits(b_daily, "sf")

  d <- if (has_geom) {
    sf::st_drop_geometry(b_daily)
  } else {
    as.data.frame(b_daily)
  }

  # Robust daily date/timestamp handling
  date_col <- .pick_first_col(
    d,
    c("bat_day", "daily_date", "date", "day"),
    require_data = FALSE
  )

  time_col <- .pick_first_col(
    d,
    c("daily_timestamp", "timestamp", "time"),
    require_data = FALSE
  )

  if (!is.na(date_col)) {
    d$daily_date <- as.Date(d[[date_col]])
  } else if (!is.na(time_col)) {
    d$daily_date <- as.Date(d[[time_col]])
  } else {
    d$daily_date <- as.Date(NA)
  }

  if (!is.na(time_col)) {
    d$daily_timestamp <- d[[time_col]]
    if (!inherits(d$daily_timestamp, "POSIXct")) {
      d$daily_timestamp <- as.POSIXct(d$daily_timestamp, tz = "UTC")
    }
  } else {
    d$daily_timestamp <- as.POSIXct(d$daily_date, tz = "UTC") + lubridate::hours(12)
  }

  # Pull lon/lat from geometry where possible, for daily locations on the map
  if (has_geom) {
    xy <- try(
      sf::st_coordinates(sf::st_transform(b_daily, 4326)),
      silent = TRUE
    )

    if (!inherits(xy, "try-error") && nrow(xy) == nrow(d)) {
      d$lon <- xy[, "X"]
      d$lat <- xy[, "Y"]
    }
  }

  # Standard plotting columns
  d$daily_seq <- seq_len(nrow(d))

  if ("daily_speed_kmh" %in% names(d)) {
    d$daily_speed_kmh_plot <- as.numeric(d$daily_speed_kmh)
  } else if ("speed_kmh" %in% names(d)) {
    d$daily_speed_kmh_plot <- as.numeric(d$speed_kmh)
  } else if ("daily_speed_mps" %in% names(d)) {
    d$daily_speed_kmh_plot <- as.numeric(d$daily_speed_mps) * 3.6
  } else {
    d$daily_speed_kmh_plot <- NA_real_
  }

  if ("daily_step_km" %in% names(d)) {
    d$daily_step_km_plot <- as.numeric(d$daily_step_km)
  } else {
    step_m_col <- .pick_first_col(
      d,
      c("daily_step_m", "step_m", "daily_distance_m", "distance_m", "displacement_m")
    )

    d$daily_step_km_plot <- if (!is.na(step_m_col)) {
      as.numeric(d[[step_m_col]]) / 1000
    } else {
      NA_real_
    }
  }

  pressure_col <- .pick_first_col(
    d,
    c(
      "daily_p_min",
      "daily_pmin_24h",
      "daily_pressure_min",
      "daily_pressure_mbar",
      "tinyfox_pressure_min_last_24h",
      "barometric_pressure"
    )
  )

  d$daily_pressure_alt_m <- if (!is.na(pressure_col)) {
    .pressure_to_alt_m(as.numeric(d[[pressure_col]]))
  } else {
    NA_real_
  }

  d
}

.scan_one_track <- function(
  tag,
  b_full,
  b_loc,
  b_daily = NULL,
  out_dir,
  overwrite,
  elev_z,
  countries,
  verbose = TRUE
) {
  tag <- as.character(tag)

  status_row <- function(status, file = NA_character_, message = NA_character_) {
    data.frame(
      deployment_id = tag,
      status = status,
      file = file,
      message = message,
      stringsAsFactors = FALSE
    )
  }

  tryCatch({
    if (nrow(b_loc) < 2) {
      msg <- "fewer than 2 locations"
      if (verbose) message("Skipping ", tag, ": ", msg)
      return(status_row("skipped", message = msg))
    }

    if (nrow(b_full) == 0) {
      msg <- "no full/sensor rows"
      if (verbose) message("Skipping ", tag, ": ", msg)
      return(status_row("skipped", message = msg))
    }

    b_full_df <- sf::st_drop_geometry(b_full)

    sp  <- .first_or(b_full_df$species, "unknown_species")
    sex <- .first_or(b_full_df$sex, "unknown_sex")

    yr <- if ("timestamp" %in% names(b_full_df) && length(b_full_df$timestamp) > 0) {
      lubridate::year(b_full_df$timestamp[1])
    } else {
      NA_integer_
    }

    tag_id <- if (
      "tag_local_identifier" %in% names(b_full_df) &&
        !is.na(b_full_df$tag_local_identifier[1])
    ) {
      as.character(b_full_df$tag_local_identifier[1])
    } else {
      NA_character_
    }

    indiv_id <- if (
      "individual_local_identifier" %in% names(b_full_df) &&
        !is.na(b_full_df$individual_local_identifier[1])
    ) {
      as.character(b_full_df$individual_local_identifier[1])
    } else {
      NA_character_
    }

    out_file <- file.path(
      out_dir,
      paste0(
        .clean_filename_part(sp), "_",
        .clean_filename_part(as.character(yr)), "_",
        .clean_filename_part(tag_id), "_",
        .clean_filename_part(indiv_id), "_",
        .clean_filename_part(sex), ".png"
      )
    )

    if (!overwrite && file.exists(out_file)) {
      msg <- paste0("exists, skipping: ", basename(out_file))
      if (verbose) message(msg)
      return(status_row("exists", file = out_file, message = msg))
    }

    # Drop empty geometries
    b_loc_valid <- b_loc[!sf::st_is_empty(b_loc), ]

    if (nrow(b_loc_valid) < 2) {
      msg <- "fewer than 2 valid geometries"
      if (verbose) message("Skipping ", tag, ": ", msg)
      return(status_row("skipped", file = out_file, message = msg))
    }
    
    b_daily_df <- .prepare_daily_df(b_daily)
    has_daily <- nrow(b_daily_df) > 0

    # ── Daily column name candidates (resolved once, used in overlays below) ──
    daily_vedba_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_vedba_sum", "tinyfox_total_vedba",
      "daily_total_vedba_24h",
      "daily_vedba_24h", "daily_total_vedba",
      "tinyfox_vedba_24h", "daily_tinyfox_vedba_24h"
    )) else NA_character_

    daily_temp_min_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_temp_min", "daily_tmin_24h",
      "daily_temperature_min", "tinyfox_temperature_min_last_24h"
    )) else NA_character_

    daily_temp_max_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_temp_max", "daily_tmax_24h",
      "daily_temperature_max", "tinyfox_temperature_max_last_24h"
    )) else NA_character_

    daily_temp_mean_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_temp_mean", "daily_tmean_24h",
      "daily_temperature_mean", "temperature_mean"
    )) else NA_character_

    daily_pressure_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_p_min", "daily_pmin_24h",
      "daily_pressure_min", "daily_pressure_mbar",
      "tinyfox_pressure_min_last_24h", "barometric_pressure"
    )) else NA_character_

    daily_activity_col <- if (has_daily) .pick_first_col(b_daily_df, c(
      "daily_activity_percent_24h", "tinyfox_activity_percent_last_24h"
    )) else NA_character_

    # ── Tag type detection ───────────────────────────────────────────────
    is_tinyfox <-
      any(grepl("tinyfox|BBX5|BBV6|TV1", b_full_df$model, ignore.case = TRUE), na.rm = TRUE) ||
      any(b_full_df$tag_firmware %in% c("V13", "V13P", "V14P", "BBX5", "BBV6", "TV1"), na.rm = TRUE)

    # ── Elevation raster ─────────────────────────────────────────────────
    e <- terra::rast(elevatr::get_elev_raster(b_loc_valid, z = elev_z, expand = 1))
    elev <- extract_elevation_segments(raster = e, tag = b_loc_valid)

    # ── Coordinates & step distances ─────────────────────────────────────
    coords <- sf::st_coordinates(sf::st_transform(b_loc_valid, 3035))
    step_m <- c(
      NA_real_,
      sqrt(diff(coords[, "X"])^2 + diff(coords[, "Y"])^2)
    )

    lon_lat <- sf::st_coordinates(b_loc_valid)

    b_df <- b_loc_valid %>%
      sf::st_drop_geometry() %>%
      mutate(
        lon = lon_lat[, "X"],
        lat = lon_lat[, "Y"],
        step_m = step_m,
        dt_h = c(NA_real_, as.numeric(diff(timestamp), units = "hours")),
        speed_kmh = step_m / 1000 / dt_h,
        cum_dist_km = cumsum(replace(step_m, is.na(step_m), 0)) / 1000,
        seq_plot = if ("sequence_number" %in% names(.)) sequence_number else row_number(),
        radius_plot = if ("sigfox_computed_location_radius" %in% names(.)) {
          sigfox_computed_location_radius
        } else {
          NA_real_
        }
      )

    # ── Tag metadata & track summary ─────────────────────────────────────
    mdl <- .first_or(b_full_df$model, "unknown")
    firmware <- .first_or(b_full_df$tag_firmware, "unknown")

    n_fixes <- nrow(b_df)
    date_start <- format(min(b_df$timestamp, na.rm = TRUE), "%Y-%m-%d")
    date_end <- format(max(b_df$timestamp, na.rm = TRUE), "%Y-%m-%d")

    duration_d <- round(
      as.numeric(
        difftime(
          max(b_df$timestamp, na.rm = TRUE),
          min(b_df$timestamp, na.rm = TRUE),
          units = "days"
        )
      ),
      1
    )

    max_disp <- if (.has_data(b_df, "displacement")) {
      round(max(b_df$displacement, na.rm = TRUE), 0)
    } else {
      NA_real_
    }

    total_dist <- round(sum(b_df$step_m, na.rm = TRUE) / 1000, 0)

    stats_sub <- paste0(
      "Model: ", mdl, " | Firmware: ", firmware, "\n",
      date_start, " – ", date_end,
      " | ", duration_d, " days",
      " | ", n_fixes, " fixes | ",
      if (!is.na(max_disp)) paste0("max disp: ", max_disp, " km | ") else "",
      "total dist: ", total_dist, " km"
    )

    xlims <- c(min(b_df$lon, na.rm = TRUE) - 2, max(b_df$lon, na.rm = TRUE) + 1)
    ylims <- c(min(b_df$lat, na.rm = TRUE) - 1, max(b_df$lat, na.rm = TRUE) + 1)

    b_loc_valid$seq_plot <- if ("sequence_number" %in% names(b_loc_valid)) {
      b_loc_valid$sequence_number
    } else {
      seq_len(nrow(b_loc_valid))
    }

    if ("sigfox_computed_location_radius" %in% names(b_loc_valid)) {
      r_idx <- !is.na(b_loc_valid$sigfox_computed_location_radius)
      r <- sf::st_buffer(
        b_loc_valid[r_idx, ],
        dist = b_loc_valid$sigfox_computed_location_radius[r_idx]
      )
    } else {
      r <- b_loc_valid[0, ]
    }

    # ── Shared minimal panel theme ───────────────────────────────────────
    .pt <- function() {
      theme_minimal() +
        theme(
          legend.position = "none",
          panel.background = element_rect(fill = "white", color = "white"),
          plot.background = element_rect(fill = "white", color = "white")
        )
    }

    # ── Panel: Map ───────────────────────────────────────────────────────
    map <- ggplot() +
      tidyterra::geom_spatraster(data = e) +
      tidyterra::scale_fill_hypso_c(
        name = "Elevation (m)",
        palette = "arctic_bathy",
        limits = c(0, 4000)
      ) +
      geom_sf(data = countries, fill = NA, col = "white") +
      geom_sf(data = r, alpha = 0.1, aes(col = seq_plot)) +
      geom_path(data = b_df, aes(lon, lat, col = seq_plot), linewidth = 1) +
      geom_point(data = b_df, aes(lon, lat, col = seq_plot)) +
      scale_color_viridis_c(name = "Sequence", option = "A") +
      coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
      labs(
        title = paste0(
          if (!is.na(indiv_id)) paste("Individual:", indiv_id) else "",
          if (!is.na(tag_id)) paste0(" | Tag: ", tag_id) else "",
          " | Species: ", sp,
          " | Sex: ", sex,
          " | Year: ", yr
        ),
        subtitle = stats_sub,
        x = "Longitude",
        y = "Latitude"
      ) +
      theme(
        legend.position = "none",
        panel.background = element_rect(fill = "white", color = "white"),
        plot.background = element_rect(fill = "white", color = "white"),
        plot.title = element_text(size = 8, face = "bold"),
        plot.subtitle = element_text(size = 6.5, colour = "grey30")
      )
    
    if (
      has_daily &&
        all(c("lon", "lat") %in% names(b_daily_df)) &&
        any(!is.na(b_daily_df$lon)) &&
        any(!is.na(b_daily_df$lat))
      ) {
      map <- map +
        geom_path(
          data = b_daily_df,
          aes(lon, lat),
          inherit.aes = FALSE,
          col = "black",
          linewidth = 0.6,
          linetype = "dashed"
        ) +
        geom_point(
          data = b_daily_df,
          aes(lon, lat),
          inherit.aes = FALSE,
          shape = 21,
          fill = "white",
          col = "black",
          size = 2.3,
          stroke = 0.8
        )
    }
    # ── Snap daily timestamps to nearest raw sensor row (one per sensor type) ──
    # For NanoFox, sensor rows sit at their own timestamps (not location times),
    # so solar-noon daily_timestamp doesn't fall on a raw measurement. Snapping
    # makes daily overlay circles land exactly on an existing raw data point.
    if (has_daily && nrow(b_daily_df) > 0 && "daily_timestamp" %in% names(b_daily_df)) {
      .vs_col <- .pick_first_col(b_full_df,
        c("tinyfox_total_vedba", "tinyfox_vedba_burst_sum", "vedba"))
      b_daily_df$snap_ts_v <- if (!is.na(.vs_col))
        .snap_to_nearest_ts(b_daily_df$daily_timestamp, b_full_df$timestamp[!is.na(b_full_df[[.vs_col]])])
      else b_daily_df$daily_timestamp

      .t_mask <- .mk_ts_mask(b_full_df, c(
        "temperature_min", "temperature_max", "external_temperature",
        "tinyfox_temperature_min_last_24h", "tinyfox_temperature_max_last_24h"))
      b_daily_df$snap_ts_t <- .snap_to_nearest_ts(
        b_daily_df$daily_timestamp, b_full_df$timestamp[.t_mask])

      .p_mask <- .mk_ts_mask(b_full_df,
        c("barometric_pressure", "tinyfox_pressure_min_last_24h"))
      b_daily_df$snap_ts_p <- .snap_to_nearest_ts(
        b_daily_df$daily_timestamp, b_full_df$timestamp[.p_mask])

      .a_mask <- .mk_ts_mask(b_full_df, c("tinyfox_activity_percent_last_24h"))
      b_daily_df$snap_ts_a <- .snap_to_nearest_ts(
        b_daily_df$daily_timestamp, b_full_df$timestamp[.a_mask])
    } else if (has_daily && nrow(b_daily_df) > 0) {
      b_daily_df$snap_ts_v <- b_daily_df$daily_timestamp
      b_daily_df$snap_ts_t <- b_daily_df$daily_timestamp
      b_daily_df$snap_ts_p <- b_daily_df$daily_timestamp
      b_daily_df$snap_ts_a <- b_daily_df$daily_timestamp
    }

    # ── Sensor panels ────────────────────────────────────────────────────
    sensor_panels <- list()

    sensor_panels[["distance"]] <- ggplot(b_df, aes(timestamp, speed_kmh)) +
      geom_point(aes(size = radius_plot, col = seq_plot)) +
      geom_path(aes(col = seq_plot)) +
      scale_color_viridis_c(option = "A") +
      .pt() +
      ylab("Speed (km/h)") +
      xlab("Date")

    if (has_daily && .has_data(b_daily_df, "daily_speed_kmh_plot")) {
    sensor_panels[["distance"]] <- sensor_panels[["distance"]] +
      geom_line(
        data = b_daily_df,
        aes(daily_timestamp, daily_speed_kmh_plot),
        inherit.aes = FALSE,
        col = "black",
        linewidth = 0.6,
        linetype = "dashed"
      ) +
      geom_point(
        data = b_daily_df,
        aes(daily_timestamp, daily_speed_kmh_plot),
        inherit.aes = FALSE,
        shape = 21,
        fill = "white",
        col = "black",
        size = 2.2,
        stroke = 0.7
      )
    }
    if ("tag_fell_off" %in% names(b_full_df)) {
      b_full_df$tag_fell_off <- as.logical(b_full_df$tag_fell_off)
    } else {
      b_full_df$tag_fell_off <- FALSE
    }

    b_full_df$tag_fell_off[is.na(b_full_df$tag_fell_off)] <- FALSE

    .vedba_fell_scale <- scale_color_manual(
      values = c("FALSE" = "blue", "TRUE" = "red"),
      labels = c("FALSE" = "On bat", "TRUE" = "Fell off"),
      name = NULL
    )

    p_vedba <- ggplot() +
      .pt() +
      ylab("VeDBA") +
      xlab("Date") +
      theme(legend.position = "right")

    if (is_tinyfox && .has_data(b_full_df, "tinyfox_total_vedba")) {
      p_vedba <- p_vedba +
        geom_path(data = b_full_df, aes(timestamp, tinyfox_total_vedba), col = "blue") +
        geom_point(
          data = b_full_df,
          aes(timestamp, tinyfox_total_vedba, col = as.character(tag_fell_off)),
          size = 1.5
        ) +
        .vedba_fell_scale +
        ylab("Total VeDBA\n(cumulative)")
    } else {
      vedba_col <- dplyr::case_when(
        .has_data(b_full_df, "tinyfox_vedba_burst_sum") ~ "tinyfox_vedba_burst_sum",
        .has_data(b_full_df, "vedba") ~ "vedba",
        TRUE ~ NA_character_
      )

      if (!is.na(vedba_col)) {
        .vdf <- b_full_df[!is.na(b_full_df[[vedba_col]]), ]
        p_vedba <- p_vedba +
          geom_path(data = .vdf, aes(timestamp, .data[[vedba_col]]), col = "blue") +
          geom_point(
            data = .vdf,
            aes(timestamp, .data[[vedba_col]], col = as.character(tag_fell_off)),
            size = 1.5
          ) +
          .vedba_fell_scale
      }
    }

    if (has_daily && !is.na(daily_vedba_col) && .has_data(b_daily_df, daily_vedba_col)) {
      p_vedba <- p_vedba +
        geom_line(
          data = b_daily_df,
          aes(snap_ts_v, .data[[daily_vedba_col]]),
          inherit.aes = FALSE, col = "black", linewidth = 0.6, linetype = "dashed"
        ) +
        geom_point(
          data = b_daily_df,
          aes(snap_ts_v, .data[[daily_vedba_col]]),
          inherit.aes = FALSE, shape = 21, fill = "white",
          col = "black", size = 2.8, stroke = 1.2
        )
    }

    sensor_panels[["vedba"]] <- p_vedba

    if (is_tinyfox && .has_data(b_full_df, "tinyfox_activity_percent_last_24h")) {
      p_activity <- ggplot() +
        geom_path(data = b_full_df, aes(timestamp, tinyfox_activity_percent_last_24h), col = "forestgreen") +
        geom_point(data = b_full_df, aes(timestamp, tinyfox_activity_percent_last_24h), col = "forestgreen", size = 1.5) +
        ylim(c(0, 100)) +
        .pt() +
        ylab("Activity\n(% above threshold)") +
        xlab("Date")

      if (has_daily && !is.na(daily_activity_col) && .has_data(b_daily_df, daily_activity_col)) {
        p_activity <- p_activity +
          geom_line(
            data = b_daily_df,
            aes(snap_ts_a, .data[[daily_activity_col]]),
            inherit.aes = FALSE, col = "forestgreen", linewidth = 0.6, linetype = "dashed"
          ) +
          geom_point(
            data = b_daily_df,
            aes(snap_ts_a, .data[[daily_activity_col]]),
            inherit.aes = FALSE, shape = 21, fill = "white",
            col = "forestgreen", size = 2.8, stroke = 1.2
          )
      }

      sensor_panels[["activity"]] <- p_activity
    }

    # Dynamic ylim: range of valid raw temp values (exclude exact zeros = sensor artifact)
    .raw_temp_vals <- suppressWarnings(as.numeric(c(
      b_full_df$tinyfox_temperature_min_last_24h,
      b_full_df$tinyfox_temperature_max_last_24h,
      b_full_df$temperature_min,
      b_full_df$temperature_max,
      b_full_df$external_temperature
    )))
    .raw_temp_vals <- .raw_temp_vals[!is.na(.raw_temp_vals) & is.finite(.raw_temp_vals) &
                                       .raw_temp_vals != 0 & .raw_temp_vals > -30 & .raw_temp_vals < 55]
    .temp_ylim <- if (length(.raw_temp_vals) >= 5) {
      c(max(-25, min(.raw_temp_vals) - 3), min(55, max(.raw_temp_vals) + 3))
    } else {
      c(-15, 45)
    }

    p_temperature <- ggplot() +
      coord_cartesian(ylim = .temp_ylim) +
      .pt() +
      ylab("Temperature (°C)") +
      xlab("Date")

    if (is_tinyfox) {
      if (.has_data(b_full_df, "tinyfox_temperature_min_last_24h")) {
        .tdf <- b_full_df[!is.na(b_full_df$tinyfox_temperature_min_last_24h), ]
        p_temperature <- p_temperature +
          geom_path(data = .tdf, aes(timestamp, tinyfox_temperature_min_last_24h), col = "darkblue") +
          geom_point(data = .tdf, aes(timestamp, tinyfox_temperature_min_last_24h), col = "darkblue", size = 1.5)
      }

      if (.has_data(b_full_df, "tinyfox_temperature_max_last_24h")) {
        .tdf <- b_full_df[!is.na(b_full_df$tinyfox_temperature_max_last_24h), ]
        p_temperature <- p_temperature +
          geom_path(data = .tdf, aes(timestamp, tinyfox_temperature_max_last_24h), col = "tomato") +
          geom_point(data = .tdf, aes(timestamp, tinyfox_temperature_max_last_24h), col = "tomato", size = 1.5)
      }
    } else {
      if (.has_data(b_full_df, "temperature_min")) {
        .tdf <- b_full_df[!is.na(b_full_df$temperature_min), ]
        p_temperature <- p_temperature +
          geom_path(data = .tdf, aes(timestamp, temperature_min), col = "steelblue") +
          geom_point(data = .tdf, aes(timestamp, temperature_min), col = "steelblue", size = 1.5)
      }

      if (.has_data(b_full_df, "temperature_max")) {
        .tdf <- b_full_df[!is.na(b_full_df$temperature_max), ]
        p_temperature <- p_temperature +
          geom_path(data = .tdf, aes(timestamp, temperature_max), col = "tomato") +
          geom_point(data = .tdf, aes(timestamp, temperature_max), col = "tomato", size = 1.5)
      }

      if (.has_data(b_full_df, "external_temperature")) {
        .tdf <- b_full_df[!is.na(b_full_df$external_temperature), ]
        p_temperature <- p_temperature +
          geom_path(data = .tdf, aes(timestamp, external_temperature), col = "darkred") +
          geom_point(data = .tdf, aes(timestamp, external_temperature), col = "darkred", size = 1.5)
      }
    }

    if (has_daily) {
      if (!is.na(daily_temp_min_col) && .has_data(b_daily_df, daily_temp_min_col)) {
        p_temperature <- p_temperature +
          geom_line(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_min_col]]),
            inherit.aes = FALSE, col = "steelblue", linewidth = 0.6, linetype = "dashed"
          ) +
          geom_point(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_min_col]]),
            inherit.aes = FALSE, shape = 21, fill = "white",
            col = "steelblue", size = 2.8, stroke = 1.2
          )
      }
      if (!is.na(daily_temp_max_col) && .has_data(b_daily_df, daily_temp_max_col)) {
        p_temperature <- p_temperature +
          geom_line(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_max_col]]),
            inherit.aes = FALSE, col = "tomato", linewidth = 0.6, linetype = "dashed"
          ) +
          geom_point(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_max_col]]),
            inherit.aes = FALSE, shape = 21, fill = "white",
            col = "tomato", size = 2.8, stroke = 1.2
          )
      }
      if (!is.na(daily_temp_mean_col) && .has_data(b_daily_df, daily_temp_mean_col)) {
        p_temperature <- p_temperature +
          geom_line(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_mean_col]]),
            inherit.aes = FALSE, col = "black", linewidth = 0.7, linetype = "dashed"
          ) +
          geom_point(
            data = b_daily_df,
            aes(snap_ts_t, .data[[daily_temp_mean_col]]),
            inherit.aes = FALSE, shape = 21, fill = "white",
            col = "black", size = 2.8, stroke = 1.2
          )
      }
    }

    if (.has_data(b_full_df, "min_temp_c")) {
      .mt_rows <- b_full_df[!is.na(b_full_df$min_temp_c), ]
      if (nrow(.mt_rows) > 0) {
        .mt_rows$ymin <- dplyr::case_when(
          .mt_rows$min_temp_c == ">0 / <=5"  ~ 0,
          .mt_rows$min_temp_c == ">5 / <=10" ~ 5,
          TRUE ~ NA_real_
        )
        .mt_rows$ymax <- dplyr::case_when(
          .mt_rows$min_temp_c == ">0 / <=5"  ~ 5,
          .mt_rows$min_temp_c == ">5 / <=10" ~ 10,
          TRUE ~ NA_real_
        )
        .mt_rows$yline <- dplyr::case_when(
          .mt_rows$min_temp_c == "<=0" ~ 0,
          .mt_rows$min_temp_c == ">10" ~ 10,
          TRUE ~ NA_real_
        )
        .rb <- .mt_rows[!is.na(.mt_rows$ymin), ]
        if (nrow(.rb) > 0) {
          p_temperature <- p_temperature +
            geom_ribbon(
              data = .rb,
              aes(x = timestamp, ymin = ymin, ymax = ymax),
              inherit.aes = FALSE, fill = "darkgreen", alpha = 0.2
            )
        }
        .ln <- .mt_rows[!is.na(.mt_rows$yline), ]
        if (nrow(.ln) > 0) {
          p_temperature <- p_temperature +
            geom_line(
              data = .ln, aes(x = timestamp, y = yline),
              inherit.aes = FALSE, colour = "darkgreen", linewidth = 0.7
            ) +
            geom_point(
              data = .ln, aes(x = timestamp, y = yline),
              inherit.aes = FALSE, colour = "darkgreen", size = 1.5
            )
        }
      }
    }

    sensor_panels[["temperature"]] <- p_temperature

    p_pressure <- ggplot() +
      .pt() +
      ylab("Pressure (mbar)") +
      xlab("Date")

    if (.has_data(b_full_df, "barometric_pressure")) {
      .bpdf <- b_full_df[!is.na(b_full_df$barometric_pressure), ]
      p_pressure <- p_pressure +
        geom_path(data = .bpdf, aes(timestamp, barometric_pressure), col = "purple") +
        geom_point(data = .bpdf, aes(timestamp, barometric_pressure), col = "purple", size = 1.5)
    }

    if (.has_data(b_full_df, "tinyfox_pressure_min_last_24h")) {
      .bpdf <- b_full_df[!is.na(b_full_df$tinyfox_pressure_min_last_24h), ]
      p_pressure <- p_pressure +
        geom_path(data = .bpdf, aes(timestamp, tinyfox_pressure_min_last_24h), col = "darkorchid") +
        geom_point(data = .bpdf, aes(timestamp, tinyfox_pressure_min_last_24h), col = "darkorchid", size = 1.5)
    }

    if (has_daily && !is.na(daily_pressure_col) && .has_data(b_daily_df, daily_pressure_col)) {
      p_pressure <- p_pressure +
        geom_line(
          data = b_daily_df,
          aes(snap_ts_p, .data[[daily_pressure_col]]),
          inherit.aes = FALSE, col = "purple", linewidth = 0.6, linetype = "dashed"
        ) +
        geom_point(
          data = b_daily_df,
          aes(snap_ts_p, .data[[daily_pressure_col]]),
          inherit.aes = FALSE, shape = 21, fill = "white",
          col = "purple", size = 2.8, stroke = 1.2
        )
    }

    sensor_panels[["pressure"]] <- p_pressure

    # ── Altitude profile ─────────────────────────────────────────────────
    b_full_df$alt_pressure_m <- NA_real_

    if (.has_data(b_full_df, "tinyfox_pressure_min_last_24h")) {
      b_full_df$alt_pressure_m <- .pressure_to_alt_m(b_full_df$tinyfox_pressure_min_last_24h)
    } else if (.has_data(b_full_df, "barometric_pressure")) {
      b_full_df$alt_pressure_m <- .pressure_to_alt_m(b_full_df$barometric_pressure)
    }

    b_df$alt_pressure_m <- NA_real_

    if (.has_data(b_df, "tinyfox_pressure_min_last_24h")) {
      b_df$alt_pressure_m <- .pressure_to_alt_m(b_df$tinyfox_pressure_min_last_24h)
    } else if (.has_data(b_df, "barometric_pressure")) {
      b_df$alt_pressure_m <- .pressure_to_alt_m(b_df$barometric_pressure)
    }

    # Assign cum_dist_km to all sensor rows via nearest location timestamp
    if (nrow(b_full_df) > 0 && nrow(b_df) > 0) {
      t_full <- as.numeric(b_full_df$timestamp)
      t_loc <- as.numeric(b_df$timestamp)

      idx_l <- findInterval(t_full, t_loc, all.inside = TRUE)
      idx_r <- pmin(idx_l + 1L, nrow(b_df))

      use_r <- abs(t_full - t_loc[idx_r]) < abs(t_full - t_loc[idx_l])
      nn <- ifelse(use_r, idx_r, idx_l)

      b_full_df$cum_dist_km <- b_df$cum_dist_km[nn]
    } else {
      b_full_df$cum_dist_km <- NA_real_
    }

    p_altitude <- ggplot() +
      geom_path(data = elev, aes(distance_from_origin, elevation), col = "gray20") +
      .pt() +
      ylab("Elevation /\nAltitude (m)") +
      xlab("Cumulative\ndistance (km)")

    if (.has_data(b_df, "altitude_m")) {
      p_altitude <- p_altitude +
        geom_path(data = b_df, aes(cum_dist_km, altitude_m), col = "orange")
    }

    if (.has_data(b_full_df, "altitude_m")) {
      p_altitude <- p_altitude +
        geom_point(
          data = b_full_df[!is.na(b_full_df$altitude_m), ],
          aes(cum_dist_km, altitude_m),
          col = "orange",
          size = 1.5
        )
    }

    if (any(!is.na(b_df$alt_pressure_m))) {
      p_altitude <- p_altitude +
        geom_path(data = b_df, aes(cum_dist_km, alt_pressure_m), col = "purple", linetype = "dashed")
    }

    if (any(!is.na(b_full_df$alt_pressure_m))) {
      p_altitude <- p_altitude +
        geom_point(
          data = b_full_df[!is.na(b_full_df$alt_pressure_m), ],
          aes(cum_dist_km, alt_pressure_m),
          col = "purple",
          size = 1.5
        )
    }

    if (has_daily && .has_data(b_daily_df, "daily_pressure_alt_m")) {
      # Snap daily cum_dist_km to nearest raw pressure data row (not nearest location)
      # so the circle lands on top of an existing raw pressure altitude point.
      .pres_rows <- b_full_df[!is.na(b_full_df$alt_pressure_m) & !is.na(b_full_df$cum_dist_km), ]
      if (nrow(.pres_rows) > 0) {
        t_d <- as.numeric(b_daily_df$daily_timestamp)
        t_p <- as.numeric(.pres_rows$timestamp)
        idx_l <- findInterval(t_d, t_p, all.inside = TRUE)
        idx_r <- pmin(idx_l + 1L, nrow(.pres_rows))
        use_r <- abs(t_d - t_p[idx_r]) < abs(t_d - t_p[idx_l])
        b_daily_df$cum_dist_km <- .pres_rows$cum_dist_km[ifelse(use_r, idx_r, idx_l)]
      } else {
        # Fallback: snap to nearest location row
        t_d <- as.numeric(b_daily_df$daily_timestamp)
        t_l <- as.numeric(b_df$timestamp)
        idx_l <- findInterval(t_d, t_l, all.inside = TRUE)
        idx_r <- pmin(idx_l + 1L, nrow(b_df))
        use_r <- abs(t_d - t_l[idx_r]) < abs(t_d - t_l[idx_l])
        b_daily_df$cum_dist_km <- b_df$cum_dist_km[ifelse(use_r, idx_r, idx_l)]
      }

      p_altitude <- p_altitude +
        geom_point(
          data = b_daily_df[!is.na(b_daily_df$daily_pressure_alt_m), ],
          aes(cum_dist_km, daily_pressure_alt_m),
          inherit.aes = FALSE,
          shape = 21,
          fill = "white",
          col = "black",
          size = 2.2,
          stroke = 0.7
        )
    }
    # ── Daily standalone panels ──────────────────────────────────────────────
    # VeDBA, temperature, and pressure are overlaid directly on the raw sensor
    # panels above. Only step distance (different metric from speed) and message
    # count (no raw equivalent) remain as separate panels here.
    if (has_daily) {
      daily_panels <- list()

      if (.has_data(b_daily_df, "daily_step_km_plot")) {
        daily_panels[["daily_movement"]] <- ggplot(b_daily_df) +
          geom_col(aes(daily_date, daily_step_km_plot), fill = "grey65") +
          geom_point(aes(daily_date, daily_step_km_plot), col = "black", size = 1.6) +
          .pt() +
          theme(legend.position = "none") +
          ylab("Daily step\n(km)") +
          xlab("Daily date")
      }

      daily_n_col <- .pick_first_col(
        b_daily_df,
        c("daily_n_msg", "daily_n_messages", "n_msg", "n_messages")
      )

      if (!is.na(daily_n_col)) {
        daily_panels[["daily_messages"]] <- ggplot(b_daily_df) +
          geom_col(aes(daily_date, .data[[daily_n_col]]), fill = "grey65") +
          geom_point(aes(daily_date, .data[[daily_n_col]]), col = "black", size = 1.6) +
          .pt() +
          ylab("Messages\nper day") +
          xlab("Daily date")
      }

      if (length(daily_panels) > 0) {
        sensor_panels <- c(sensor_panels, daily_panels)
      }
    }
    # ── Assemble & save ──────────────────────────────────────────────────
    n_panels <- length(sensor_panels)
    ncol_grid <- if (n_panels <= 4) 2L else 3L
    nrow_grid <- ceiling(n_panels / ncol_grid)

    p_sensors <- ggpubr::ggarrange(
      plotlist = unname(sensor_panels),
      ncol = ncol_grid,
      nrow = nrow_grid
    )

    p <- ggpubr::ggarrange(
      map,
      p_sensors,
      p_altitude,
      ncol = 1,
      heights = c(2.5, nrow_grid * 1.0, 0.6)
    )
    p <- ggpubr::annotate_figure(
      p,
      bottom = ggpubr::text_grob(
        "○ open circle + dashed line = daily summary value     • filled circle + solid line = raw sensor data",
        size = 7, color = "grey30"
      )
    )

    fig_height <- 3.5 + nrow_grid * 2.2 + 0.2

    ggplot2::ggsave(
      plot = p,
      filename = out_file,
      width = 9,
      height = fig_height,
      bg = "white"
    )

    msg <- paste0("saved: ", basename(out_file))
    if (verbose) message("Saved: ", basename(out_file))

    status_row("saved", file = out_file, message = msg)
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (verbose) message("Error in ", tag, ": ", msg)
    status_row("error", message = msg)
  })
}

# ── scan_tracks() ─────────────────────────────────────────────────────────────
# Generate scan-track diagnostic plots for one or more bat individuals.
#
# Arguments:
# bats_full   move2/sf object with all sensor rows
# bats_loc    move2/sf object with location rows only
# out_dir     directory to write PNG files
# tags        character vector of deployment_id; NULL = all deployments in bats_full
# overwrite   FALSE skips existing PNGs; TRUE regenerates
# elev_z      zoom level for elevatr::get_elev_raster
# parallel    TRUE uses furrr; FALSE uses purrr in the current R session
# workers     number of multisession workers; use NULL to rely on the existing future plan
# scheduling  furrr chunk scheduling; 1 is a good default, Inf gives one future per tag
# seed        future RNG seed setting
# verbose     print saved/skipped/error messages
# ─────────────────────────────────────────────────────────────────────────────
scan_tracks <- function(
  bats_full,
  bats_loc,
  bats_daily = NULL,
  out_dir = "../../../Dropbox/MPI/Noctule/Plots/ScanTrack/",
  tags = NULL,
  overwrite = FALSE,
  elev_z = 5,
  parallel = TRUE,
  workers = max(1L, future::availableCores() - 1L),
  scheduling = 1,
  seed = TRUE,
  verbose = TRUE
) {
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  if (!"deployment_id" %in% names(bats_full)) {
    stop("bats_full must contain deployment_id.", call. = FALSE)
  }

  if (!"deployment_id" %in% names(bats_loc)) {
    stop("bats_loc must contain deployment_id.", call. = FALSE)
  }

  if (!is.null(bats_daily) && !"deployment_id" %in% names(bats_daily)) {
    stop("bats_daily must contain deployment_id.", call. = FALSE)
  }

  if (is.null(tags)) {
    tags <- unique(as.character(bats_full$deployment_id))
  } else {
    tags <- as.character(tags)
  }

  tags <- unique(tags[!is.na(tags)])

  if (length(tags) == 0L) {
    return(invisible(data.frame(
      deployment_id = character(),
      status = character(),
      file = character(),
      message = character()
    )))
  }

  full_split <- .split_by_deployment(bats_full)
  loc_split  <- .split_by_deployment(bats_loc)

  full_empty <- .empty_like(bats_full)
  loc_empty  <- .empty_like(bats_loc)

  full_by_tag <- purrr::map(tags, ~ full_split[[.x]] %||% full_empty)
  loc_by_tag  <- purrr::map(tags, ~ loc_split[[.x]] %||% loc_empty)

  if (is.null(bats_daily)) {
    daily_by_tag <- purrr::map(tags, ~ NULL)
  } else {
    daily_split <- .split_by_deployment(bats_daily)
    daily_empty <- .empty_like(bats_daily)
    daily_by_tag <- purrr::map(tags, ~ daily_split[[.x]] %||% daily_empty)
  }

  names(full_by_tag) <- tags
  names(loc_by_tag) <- tags
  names(daily_by_tag) <- tags

  countries <- rnaturalearth::ne_countries(scale = 10, returnclass = "sf")

  if (isTRUE(parallel)) {
    old_plan <- future::plan()

    if (!is.null(workers)) {
      future::plan(future::multisession, workers = workers)
      on.exit(future::plan(old_plan), add = TRUE)
    }

    if (verbose) {
      message(
        "Running scan_tracks() with furrr using ",
        ifelse(is.null(workers), "the current future plan", paste(workers, "workers")),
        "."
      )
    }
    res <- furrr::future_pmap(
      .l = list(
        tag = tags,
        b_full = full_by_tag,
        b_loc = loc_by_tag,
        b_daily = daily_by_tag
      ),
      .f = function(tag, b_full, b_loc, b_daily) {
        .scan_one_track(
          tag = tag,
          b_full = b_full,
          b_loc = b_loc,
          b_daily = b_daily,
          out_dir = out_dir,
          overwrite = overwrite,
          elev_z = elev_z,
          countries = countries,
          verbose = verbose
        )
      },
      .options = furrr::furrr_options(
        seed = seed,
        scheduling = scheduling,
        packages = c(
          "terra", "tidyterra", "dplyr", "sf", "ggplot2",
          "rnaturalearth", "elevatr", "move2", "ggpubr",
          "lubridate", "purrr"
        )
      )
    )  
    } else {
    if (verbose) {
      message("Running scan_tracks() serially.")
    }

    res <- purrr::pmap(
      .l = list(
        tag = tags,
        b_full = full_by_tag,
        b_loc = loc_by_tag,
        b_daily = daily_by_tag
      ),
      .f = function(tag, b_full, b_loc, b_daily) {
        .scan_one_track(
          tag = tag,
          b_full = b_full,
          b_loc = b_loc,
          b_daily = b_daily,
          out_dir = out_dir,
          overwrite = overwrite,
          elev_z = elev_z,
          countries = countries,
          verbose = verbose
        )
      }
    )
  }   # <- this was missing

  res <- dplyr::bind_rows(res)

  if (verbose) {
    print(table(res$status, useNA = "ifany"))
  }

  invisible(res)
}
 