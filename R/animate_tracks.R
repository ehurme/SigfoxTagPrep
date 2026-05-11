library(dplyr)
library(sf)
library(ggplot2)
library(gganimate)
library(rnaturalearth)

# ── animate_tracks() ──────────────────────────────────────────────────────────
# Animate bat migration tracks from a move2 sf object.
#
# Arguments:
#   m             move2 / sf object with geometry and timestamp column
#   out_file      output path; extension determines format:
#                   ".gif" → gifski renderer (default)
#                   ".mp4" → av renderer (requires av package)
#   color_by      column name to colour tracks by, OR one of the shortcuts:
#                   "individual" → individual_local_identifier (default)
#                   "species"    → species
#   palette       viridis option letter ("A"–"H") for discrete colour_by, OR
#                 a named character vector of colours matching levels of color_by
#   basemap       one of:
#                   "dark"      — black-fill country polygons (default)
#                   "light"     — light-grey country polygons
#                   "elevation" — elevatr DEM raster (requires elevatr, terra,
#                                 tidyterra; fetched at zoom elev_z)
#   country_scale rnaturalearth scale: 10 (detailed), 50, or 110 (fast)
#   elev_z        zoom level for elevatr (only used when basemap = "elevation")
#   group_col     column used to group track paths (default: same as color_by)
#   time_col      timestamp column name (default: "timestamp")
#   lon_col / lat_col  coordinate column names; NULL = extracted from geometry
#   title         animation title; use "{frame_along}" for running date/time
#   transition    "reveal" (trailing path, default) or "time" (no trail)
#   shadow        if TRUE and transition = "time", adds shadow_wake trail
#   shadow_length wake_length argument for shadow_wake (default 0.2)
#   fps           frames per second (default 10)
#   width / height output dimensions in pixels (default 900 × 600)
#   res           resolution in ppi passed to gifski/av (default 150)
#   nframes       number of frames; NULL = one frame per unique date
#   xlim / ylim   manual map extent; NULL = auto from data with buffer
#   buffer        degrees of padding around auto extent (default 1)
#   point_size    size of location points (default 2)
#   path_size     linewidth of track paths (default 0.8)
#   legend        show colour legend (default FALSE)
#
# Returns: path to the saved file (invisibly)
# ─────────────────────────────────────────────────────────────────────────────
animate_tracks <- function(
    m,
    out_file      = "track_animation.gif",
    color_by      = "individual",
    palette       = "A",
    basemap       = "dark",
    country_scale = 50,
    elev_z        = 5,
    group_col     = NULL,
    time_col      = "timestamp",
    lon_col       = NULL,
    lat_col       = NULL,
    title         = "Migration: {frame_along}",
    transition    = "reveal",
    shadow        = FALSE,
    shadow_length = 0.2,
    fps           = 10,
    width         = 900,
    height        = 600,
    res           = 150,
    nframes       = NULL,
    xlim          = NULL,
    ylim          = NULL,
    buffer        = 1,
    point_size    = 2,
    path_size     = 0.8,
    legend        = FALSE
) {

  # ── Resolve color_by shortcut ─────────────────────────────────────────────
  color_col <- switch(color_by,
    "individual" = "individual_local_identifier",
    "species"    = "species",
    color_by     # pass through any other column name as-is
  )
  if (is.null(group_col)) group_col <- color_col

  # ── Extract coordinates & flatten to data frame ───────────────────────────
  if (!inherits(m, "data.frame"))
    stop("m must be a data.frame / sf / move2 object")

  if (!is.null(lon_col) && !is.null(lat_col)) {
    df <- as.data.frame(m)
    df$.lon <- df[[lon_col]]
    df$.lat <- df[[lat_col]]
  } else {
    coords <- sf::st_coordinates(m)
    df <- sf::st_drop_geometry(m)
    df$.lon <- coords[, 1]
    df$.lat <- coords[, 2]
  }

  # Drop rows with missing coordinates or timestamp
  df <- df[!is.na(df$.lon) & !is.na(df$.lat) & !is.na(df[[time_col]]), ]

  # Ensure color_col exists
  if (!color_col %in% names(df))
    stop("color_by column '", color_col, "' not found in data. ",
         "Available columns: ", paste(names(df), collapse = ", "))

  # Date column for animation (date-level granularity for transition_reveal)
  df$.date <- as.Date(df[[time_col]])

  # ── Map extent ────────────────────────────────────────────────────────────
  if (is.null(xlim)) xlim <- range(df$.lon, na.rm = TRUE) + c(-buffer, buffer)
  if (is.null(ylim)) ylim <- range(df$.lat, na.rm = TRUE) + c(-buffer, buffer)

  # ── Colour scale ──────────────────────────────────────────────────────────
  # Determine if color col is discrete
  col_vals    <- df[[color_col]]
  is_discrete <- is.character(col_vals) || is.factor(col_vals)

  if (is.character(palette) && length(palette) == 1 && nchar(palette) == 1) {
    # Single viridis option letter
    colour_scale <- if (is_discrete)
      scale_color_viridis_d(option = palette, guide = if (legend) "legend" else "none")
    else
      scale_color_viridis_c(option = palette, guide = if (legend) "colorbar" else "none")
  } else {
    # Named character vector → manual scale
    colour_scale <- scale_color_manual(
      values = palette,
      guide  = if (legend) "legend" else "none"
    )
  }

  # ── Basemap ───────────────────────────────────────────────────────────────
  world <- rnaturalearth::ne_countries(scale = country_scale, returnclass = "sf")

  basemap_layers <- if (basemap == "dark") {
    list(
      geom_sf(data = world, fill = "black", col = "white", linewidth = 0.3),
      theme(
        panel.background = element_rect(fill = "black", color = "black"),
        plot.background  = element_rect(fill = "black", color = "black"),
        panel.grid       = element_line(color = "gray25"),
        text             = element_text(color = "white"),
        axis.text        = element_text(color = "white"),
        axis.title       = element_text(color = "white"),
        plot.title       = element_text(color = "white", face = "bold")
      )
    )
  } else if (basemap == "light") {
    list(
      geom_sf(data = world, fill = "lightgrey", col = "white", linewidth = 0.3),
      theme(
        panel.background = element_rect(fill = "aliceblue", color = NA),
        plot.background  = element_rect(fill = "white", color = NA),
        plot.title       = element_text(face = "bold")
      )
    )
  } else if (basemap == "elevation") {
    if (!requireNamespace("elevatr",   quietly = TRUE)) stop("Package 'elevatr' required for basemap='elevation'")
    if (!requireNamespace("terra",     quietly = TRUE)) stop("Package 'terra' required for basemap='elevation'")
    if (!requireNamespace("tidyterra", quietly = TRUE)) stop("Package 'tidyterra' required for basemap='elevation'")
    message("Fetching elevation raster (z=", elev_z, ") ...")
    m_sf   <- if (inherits(m, "sf")) m else sf::st_as_sf(m)
    e_rast <- terra::rast(elevatr::get_elev_raster(m_sf, z = elev_z, expand = 1))
    terra::values(e_rast)[terra::values(e_rast) < 0] <- 0
    list(
      tidyterra::geom_spatraster(data = e_rast),
      tidyterra::scale_fill_hypso_c(name = "Elevation (m)", palette = "arctic_bathy",
                                    limits = c(0, 4000)),
      geom_sf(data = world, fill = NA, col = "white", linewidth = 0.3),
      theme(
        panel.background = element_rect(fill = "white", color = NA),
        plot.background  = element_rect(fill = "white", color = NA),
        plot.title       = element_text(face = "bold")
      )
    )
  } else {
    stop("basemap must be one of 'dark', 'light', or 'elevation'")
  }

  # ── Build base ggplot ─────────────────────────────────────────────────────
  p <- ggplot() +
    basemap_layers +
    geom_path(
      data = df,
      aes(.lon, .lat,
          colour = .data[[color_col]],
          group  = .data[[group_col]]),
      linewidth = path_size,
      lineend   = "round"
    ) +
    geom_point(
      data = df,
      aes(.lon, .lat,
          colour = .data[[color_col]]),
      size = point_size
    ) +
    colour_scale +
    coord_sf(xlim = xlim, ylim = ylim, expand = FALSE) +
    labs(title = title, x = "Longitude", y = "Latitude") +
    theme_linedraw() +
    theme(legend.position = if (legend) "right" else "none")

  # ── Animation transition ──────────────────────────────────────────────────
  if (transition == "reveal") {
    anim <- p + gganimate::transition_reveal(.data[[".date"]]) +
      gganimate::ease_aes("linear")
  } else if (transition == "time") {
    anim <- p + gganimate::transition_time(.data[[time_col]]) +
      gganimate::ease_aes("linear")
    if (shadow)
      anim <- anim + gganimate::shadow_wake(wake_length = shadow_length, alpha = FALSE)
  } else {
    stop("transition must be 'reveal' or 'time'")
  }

  # ── Render ────────────────────────────────────────────────────────────────
  if (is.null(nframes))
    nframes <- length(unique(df$.date))
  nframes <- max(nframes, 10L) # gganimate needs at least a few frames

  ext <- tools::file_ext(out_file)

  if (tolower(ext) == "mp4") {
    if (!requireNamespace("av", quietly = TRUE))
      stop("Package 'av' required for MP4 output. Install with install.packages('av')")
    renderer <- gganimate::av_renderer(out_file)
    gganimate::animate(anim,
      nframes  = nframes,
      fps      = fps,
      width    = width,
      height   = height,
      res      = res,
      renderer = renderer
    )
  } else {
    # GIF (default)
    if (!requireNamespace("gifski", quietly = TRUE))
      stop("Package 'gifski' required for GIF output. Install with install.packages('gifski')")
    renderer <- gganimate::gifski_renderer(out_file)
    gganimate::animate(anim,
      nframes  = nframes,
      fps      = fps,
      width    = width,
      height   = height,
      res      = res,
      renderer = renderer
    )
  }

  message("Saved: ", out_file)
  invisible(out_file)
}
