scan_track_plot <- function(
    df_full,
    df_loc,
    df_thin = NULL,
    individual_id,

    # columns (UPDATED DEFAULTS)
    lon   = "lon",
    lat   = "lat",
    time  = "timestamp",
    idcol = "individual_local_identifier",   # <-- filter by this
    seq   = "seq_number",
    radius = "sigfox_computed_location_radius",

    # optional sensor columns (set to NULL to skip a panel)
    vedba = c("vedba_sum", "vpm"),
    temp  = c("avg_temp", "temperature", "tinyfox_temperature_min_last_24h", "tinyfox_temperature_max_last_24h"),
    press = c("min_3h_pressure", "barometric_pressure", "tinyfox_pressure_min_last_24h"),
    altitude = "altitude_m",
    step_dist = "displacement",

    vedba_col = NULL,
    temp_col  = NULL,
    press_col = NULL,

    # outputs
    out_path = NULL,

    # map/elevation options
    add_elevation = TRUE,
    elev_z = 5,
    elev_expand = 1,
    elev_limits = c(0, 4000),
    buffer_deg = 1,
    country_scale = 10,

    # aesthetics/layout
    point_size = 1.8,
    path_size = 0.8,
    alpha_full = 0.55,
    alpha_thin = 1,
    show_buffers = TRUE,
    buffer_alpha = 0.08,
    theme_dark = FALSE,

    verbose = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(sf)
    library(ggplot2)
    library(ggpubr)
    library(terra)
    library(tidyterra)
    library(rnaturalearth)
    library(elevatr)
    library(units)
  })

  # -------------------- helpers --------------------
  .has_col <- function(df, nm) !is.null(nm) && nm %in% names(df)
  .pick_first_existing <- function(df, nms) {
    nms <- nms[!is.na(nms) & nzchar(nms)]
    hit <- nms[nms %in% names(df)]
    if (length(hit) == 0) NULL else hit[1]
  }
  .fmt_num <- function(x, digits = 2) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) return("NA")
    format(round(x, digits), nsmall = digits, trim = TRUE)
  }
  .fmt_int <- function(x) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) return("NA")
    format(as.integer(round(x)), big.mark = ",", trim = TRUE)
  }
  .panel_theme <- function() {
    if (!isTRUE(theme_dark)) {
      theme_minimal(base_size = 10) +
        theme(
          panel.background = element_rect(fill = "white", color = NA),
          plot.background  = element_rect(fill = "white", color = NA),
          legend.position  = "none"
        )
    } else {
      theme_minimal(base_size = 10) +
        theme(
          panel.background = element_rect(fill = "black", color = NA),
          plot.background  = element_rect(fill = "black", color = NA),
          panel.grid       = element_line(color = "gray35"),
          text             = element_text(color = "white"),
          axis.text        = element_text(color = "white"),
          axis.title       = element_text(color = "white"),
          plot.title       = element_text(color = "white"),
          legend.position  = "none"
        )
    }
  }

  # -------------------- VALIDATE + FILTER (FIX) --------------------
  if (!idcol %in% names(df_full)) {
    stop("idcol='", idcol, "' not found in df_full. Available: ", paste(names(df_full), collapse = ", "))
  }
  if (!("geometry" %in% names(df_full))) {
    stop("df_full must be an sf object with a 'geometry' column.")
  }

  # Avoid factor/number mismatch: compare as character
  target <- as.character(individual_id)

  # df_full <- df_full %>%
  #   mutate(.scan_id = as.character(.data[[idcol]])) %>%
  #   filter(.scan_id == target) %>%
  #   select(-.scan_id)
  df_f <- df_full[which(df_full$individual_local_identifier == target),]
  if (is.null(df_loc)) {
    if (verbose) message("df_loc is NULL; using df_full as 'location' overlay (no locations provided).")
    df_loc <- df_f
  } else {
    if (!idcol %in% names(df_loc)) {
      stop("idcol='", idcol, "' not found in df_loc. Available: ", paste(names(df_loc), collapse = ", "))
    }
    # df_thin <- df_thin %>%
    #   mutate(.scan_id = as.character(.data[[idcol]])) %>%
    #   filter(.scan_id == target) %>%
    #   select(-.scan_id)
    df_l <- df_loc[which(df_loc$individual_local_identifier == target),]
  }

  if (is.null(df_thin)) {
    if (verbose) message("df_thin is NULL; using df_full as 'thin' overlay (no thinning provided).")
    df_t <- df_f
  } else {
    if (!idcol %in% names(df_thin)) {
      stop("idcol='", idcol, "' not found in df_thin. Available: ", paste(names(df_thin), collapse = ", "))
    }
    # df_thin <- df_thin %>%
    #   mutate(.scan_id = as.character(.data[[idcol]])) %>%
    #   filter(.scan_id == target) %>%
    #   select(-.scan_id)
    df_t <- df_thin[which(df_thin$individual_local_identifier == target),]
  }

  if (nrow(df_f) == 0) {
    stop("Filtering returned 0 rows. Check individual_id='", target, "' and idcol='", idcol, "'.")
  }
  if (nrow(df_f) < 2) {
    stop("Not enough rows after filtering (n < 2) for individual_id='", target, "'.")
  }

  # -------------------- from here down: SAME LOGIC as before --------------------
  df_f <- df_f %>% arrange(.data[[time]])
  df_l <- df_l %>% arrange(.data[[time]])
  df_t <- df_t %>% arrange(.data[[time]])

  df_f_g <- df_f %>% filter(!st_is_empty(geometry))
  df_l_g <- df_l %>% filter(!st_is_empty(geometry))
  df_t_g <- df_t %>% filter(!st_is_empty(geometry))

  if (nrow(df_f_g) < 2) stop("After dropping empty geometries, not enough points to plot.")

  # choose first available sensor cols
  if(any(df_f_g$tag_type == "nanofox", na.rm = TRUE)){
    vedba_col <- vedba[1]
    temp_col  <- temp[1]
    press_col <- press[1]
  }
  if(any(df_f_g$tag_type == "tinyfox", na.rm = TRUE)){
    vedba_col <- vedba[2]
    temp_col  <- temp[3:4]
    press_col <- press[3]
  }

  # map extents
  xlims <- range(df_f_g[[lon]], na.rm = TRUE) + c(-2, 1) * buffer_deg
  ylims <- range(df_f_g[[lat]], na.rm = TRUE) + c(-1, 1) * buffer_deg

  # buffers
  buf_full <- NULL
  buf_thin <- NULL
  if (isTRUE(show_buffers) && .has_col(df_f_g, radius)) {
    idx <- which(!is.na(df_f_g[[radius]]) & as.numeric(df_f_g[[radius]]) > 0)
    if (length(idx) > 0) buf_full <- st_buffer(df_f_g[idx, ], dist = as.numeric(df_f_g[[radius]][idx]))
  }
  if (isTRUE(show_buffers) && .has_col(df_t_g, radius)) {
    idx <- which(!is.na(df_t_g[[radius]]) & as.numeric(df_t_g[[radius]]) > 0)
    if (length(idx) > 0) buf_thin <- st_buffer(df_t_g[idx, ], dist = as.numeric(df_t_g[[radius]][idx]))
  }

  # elevation raster (optional)
  elev_rast <- NULL
  if (isTRUE(add_elevation)) {
    if (verbose) message("Fetching elevation raster (z=", elev_z, ") ...")
    elev_rast <- try({
      terra::rast(elevatr::get_elev_raster(df_f_g, z = elev_z, expand = elev_expand))
    }, silent = TRUE)
    if (inherits(elev_rast, "try-error")) {
      elev_rast <- NULL
      if (verbose) message("Elevation raster fetch failed; continuing without it.")
    } else {
      terra::values(elev_rast)[terra::values(elev_rast) < 0] <- 0
    }
  }

  source("./R/extract_elevation_segments.R")
  elev <- NULL
  elev <- extract_elevation_segments(raster = elev_rast, tag = df_l_g)

  base_countries <- rnaturalearth::ne_countries(scale = country_scale)

  p_map <- ggplot() +
    { if (!is.null(elev_rast)) geom_spatraster(data = elev_rast) } +
    { if (!is.null(elev_rast)) scale_fill_hypso_c(name = "Elevation (m)", palette = "arctic_bathy", limits = elev_limits) } +
    geom_sf(data = base_countries, fill = NA, color = ifelse(theme_dark, "gray70", "gray80")) +
    geom_path(data = df_f_g, aes(.data[[lon]], .data[[lat]]), linewidth = path_size, alpha = alpha_full) +
    geom_path(data = df_t_g, aes(.data[[lon]], .data[[lat]]),
              linewidth = path_size + 0.25, alpha = alpha_thin, col = "gray") +
    { if (!is.null(buf_full)) geom_sf(data = buf_full, aes(col = .data[[time]]),
                                      alpha = buffer_alpha, inherit.aes = FALSE) } +
    { if (!is.null(buf_thin)) geom_sf(data = buf_thin, aes(col = .data[[time]]),
                                      alpha = buffer_alpha + 0.04, inherit.aes = FALSE) } +
    # geom_point(data = df_f_g, aes(.data[[lon]], .data[[lat]], col = .data[[time]],
    #                               size = .data[[radius]] %>% as.numeric()), alpha = alpha_full) +
    # geom_point(data = df_t_g, aes(.data[[lon]], .data[[lat]], col = .data[[time]],
    #                               size = .data[[radius]] %>% as.numeric()), alpha = alpha_thin) +
    scale_color_viridis_c(option = "inferno")+
    coord_sf(xlim = xlims, ylim = ylims, expand = FALSE) +
    labs(title = paste0("ScanTrack | ID: ", target, "| sex: ", df_f_g$sex[1]),
         x = "Longitude", y = "Latitude") +
    .panel_theme()

  # sensor panels (minimal set)
  panels <- list()

  if (!is.null(step_dist) && .has_col(df_l_g, step_dist)) {
    panels$distance <- ggplot() +
      geom_path(data = df_l_g, aes(.data[[time]], distance), alpha = alpha_full) +
      geom_point(data = df_l_g, aes(.data[[time]], distance, col = .data[[time]]),
                 alpha = alpha_full) +
      geom_path(data = df_t_g, aes(.data[[time]], distance), linewidth = 0.9, alpha = alpha_thin) +
      geom_point(data = df_t_g, aes(.data[[time]], distance, col = .data[[time]]),
                 alpha = alpha_thin) +
      scale_color_viridis_c(option = "inferno")+
      labs(x = "Time", y = "distance (km)") + #, title = "Displacement (full vs thin)") +
      .panel_theme()

    panels$displacement <- ggplot() +
      geom_path(data = df_l_g, aes(.data[[time]], displacement), alpha = alpha_full) +
      geom_point(data = df_l_g, aes(.data[[time]], displacement, col = .data[[time]]),
                 alpha = alpha_full) +
      geom_path(data = df_t_g, aes(.data[[time]], displacement), linewidth = 0.9, alpha = alpha_thin) +
      geom_point(data = df_t_g, aes(.data[[time]], displacement, col = .data[[time]]),
                 alpha = alpha_thin) +
      scale_color_viridis_c(option = "inferno")+
      labs(x = "Time", y = "Displacement (km)") + #, title = "Displacement (full vs thin)") +
      .panel_theme()
  }

  if (!is.null(vedba_col)) {
    panels$vedba <- ggplot() +
      geom_path(data = df_f_g, aes(.data[[time]], .data[[vedba_col]]), alpha = alpha_full) +
      geom_point(data = df_f_g, aes(.data[[time]], .data[[vedba_col]]), alpha = alpha_full) +
      scale_size(range = c(0.5, 3)) +
      scale_color_viridis_c(option = "inferno")+
      labs(x = "Time", y = vedba_col) + #, title = "Activity (full vs thin)") +
      .panel_theme()

    if(any(df_f_g$tag_type == "nanofox", na.rm = TRUE)){
      panels$vedba <- panels$vedba +
        geom_point(data = df_t_g, aes(.data[[time]], daily_vedba_sum,
                                      size = daily_vedba_sum_n,
                                      col = .data[[time]]), alpha = alpha_thin)
    }
  }

  if (!is.null(temp_col)) {
    if(length(temp_col) == 2){
      panels$temp <- ggplot() +
        geom_path(data = df_f_g, aes(.data[[time]], .data[[temp_col[1]]], col = .data[[time]]), alpha = alpha_full) +
        geom_path(data = df_f_g, aes(.data[[time]], .data[[temp_col[2]]], col = .data[[time]]), alpha = alpha_full) +
        scale_color_viridis_c(option = "inferno")+
        labs(x = "Time", y = temp_col) + # title = "Temperature (full vs thin)") +
        .panel_theme()
    }
    if(length(temp_col) == 1){
      panels$temp <- ggplot() +
        geom_path(data = df_f_g, aes(.data[[time]], .data[[temp_col]], col = .data[[time]]), alpha = alpha_full) +
        scale_color_viridis_c(option = "inferno")+
        labs(x = "Time", y = temp_col) + # title = "Temperature (full vs thin)") +
        .panel_theme()
    }

    if(any(df_t_g$tag_type == "nanofox")){
      panels$temp <- panels$temp +
        #geom_point(data = df_t_g, aes(.data[[time]], daily_temp_min), size = 1.6, alpha = alpha_thin, col = "cyan") +
        #geom_point(data = df_t_g, aes(.data[[time]], daily_temp_max), size = 1.6, alpha = alpha_thin, col = "pink") +
        geom_point(data = df_t_g, aes(.data[[time]], daily_temp_mean), size = 1.6, alpha = alpha_thin)
    }
  }

  if (!is.null(press_col)) {
    panels$altitude <- ggplot() +
      geom_ribbon(data = elev, aes(x = distance_from_origin, ymax = elevation, ymin = 0), fill = "gray")+
      geom_path(data = df_l_g, aes(cumsum(as.numeric(distance)), .data[[altitude]]), alpha = alpha_full) +
      geom_point(data = df_t_g, aes(cumsum(as.numeric(distance)), .data[[altitude]],
                                    col = .data[[time]]), size = 1.6, alpha = alpha_thin) +
      geom_point(data = df_t_g, aes(cumsum(as.numeric(distance)), daily_alt_day_mean_m,
                                    size = daily_alt_day_mean_n), alpha = alpha_thin, col = "orange") +
      geom_point(data = df_t_g, aes(cumsum(as.numeric(distance)), daily_alt_night_max_m,
                                    size = daily_alt_night_max_n), alpha = alpha_thin, col = "darkblue") +
      scale_color_viridis_c(option = "inferno")+
      labs(x = "Cumulative Distance (km)", y = "Altitude (m)")+ #, title = "Pressure (full vs thin)") +
      .panel_theme()

    panels$altitude_time <- ggplot() +
      # geom_ribbon(data = df_l_g, aes(x = .data[[time]], ymax = elevation, ymin = 0), fill = "gray")+
      geom_path(data = df_l_g, aes(.data[[time]], .data[[altitude]]), alpha = alpha_full) +
      geom_point(data = df_t_g, aes(.data[[time]], .data[[altitude]], col = .data[[time]]), size = 1.6, alpha = alpha_thin) +
      geom_point(data = df_t_g, aes(.data[[time]], daily_alt_day_mean_m,
                                    size = daily_alt_day_mean_n), alpha = alpha_thin, col = "orange") +
      geom_point(data = df_t_g, aes(.data[[time]], daily_alt_night_max_m,
                                    size = daily_alt_night_max_n), alpha = alpha_thin, col = "darkblue") +
      scale_color_viridis_c(option = "inferno")+
      labs(x = "Time", y = "Altitude (m)")+ #, title = "Pressure (full vs thin)") +
      .panel_theme()
  }

  panel_list <- unname(panels)
  if (length(panel_list) == 0) {
    p_all <- p_map
  } else {
    grid <- ggarrange(plotlist = panel_list, ncol = 2, nrow = ceiling(length(panel_list) / 2))
    p_all <- ggarrange(p_map, grid, ncol = 1, heights = c(2, 1.6))
  }

  if (!is.null(out_path)) {
    ggsave(filename = paste0(out_path, "scantrack_", individual_id, ".png"), plot = p_all, width = 7, height = 9, dpi = 300)
  }

  invisible(list(plot = p_all, df_full = df_full, df_thin = df_thin))
}

# # full data: s[[4]] ; thinned data: s[[6]]
# # Nlei25_freinat_Tier135_3639D2C
# # Nlei25_swiss_296_9EB3C6
# tags <- df_thin$individual_local_identifier[which(df_thin$displacement > 100)] %>% unique()
# for(tag in tags){
#   try({
#     res <- scan_track_plot(
#       df_full = y$full,
#       df_loc = y$location,
#       df_thin = y$daily,
#       individual_id = tag,   # example
#       idcol = "individual_local_identifier",
#       out_path = "../../../Dropbox/MPI/Noctule/Plots/ScanTrack/"
#     )
#   })
# }
# res$plot
#
#
# # view plot
# res$plot
#
# # see stats objects
# str(res$stats_full)
# str(res$stats_thin)
