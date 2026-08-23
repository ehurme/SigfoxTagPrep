#' Overlay raw and regularized tracks, with movement + pairwise-distance summaries
#'
#' Plots each individual's raw (irregular) fixes against the CTCRW/linear
#' interpolated track produced by \code{\link{regularize_tracks}}, so
#' interpolation quality can be sanity-checked by eye. Also computes basic
#' movement statistics (distance travelled, duration, average speed) for both
#' the raw and regularized tracks, and -- when two or more individuals are
#' requested -- pairwise distances between them at shared regularized
#' timestamps via \code{\link{pairwise_track_distance}}.
#'
#' @param raw A move2/sf object or data.frame of the original, irregular
#'   fixes (the same input passed to \code{regularize_tracks}).
#' @param reg A data.table as returned by \code{\link{regularize_tracks}}.
#' @param ids Character vector of individuals to include; \code{NULL}
#'   (default) uses every individual present in \code{reg}.
#' @param id_col,time_col Column names in \code{raw} identifying the
#'   individual and timestamp (matching what was passed to
#'   \code{regularize_tracks}).
#' @param facet If \code{TRUE} (default) and more than one individual is
#'   plotted, facet the map by individual; if \code{FALSE}, overlay all
#'   individuals on one panel, coloured by id.
#' @param show_se If \code{TRUE} (default), size regularized points by CTCRW
#'   prediction uncertainty (\eqn{\sqrt{se_x^2+se_y^2}}); skipped where every
#'   value is \code{NA} (linear fallback).
#' @param pairwise If \code{TRUE} (default) and \code{length(ids) >= 2},
#'   compute pairwise distances/summaries and add a distance-over-time panel.
#' @param out_path Optional; if given, the combined plot is saved here as a PNG.
#' @param point_size,path_size Plot aesthetics.
#' @param verbose Print progress messages.
#' @return A list with:
#' \describe{
#'   \item{plot}{The combined ggplot (map, plus pairwise-distance panel when
#'     applicable).}
#'   \item{map}{The map panel alone.}
#'   \item{track_stats}{data.table, one row per individual: raw vs.
#'     regularized fix count, distance (km), duration (h), average speed
#'     (km/h), interpolation \code{method}, and mean CTCRW SE (m).}
#'   \item{pairwise}{data.table of pairwise distances at shared timestamps
#'     (\code{NULL} if \code{pairwise = FALSE} or fewer than 2 individuals).}
#'   \item{pairwise_summary}{One row per dyad summary (\code{NULL} under the
#'     same conditions as \code{pairwise}).}
#' }
#' @importFrom sf st_geometry st_coordinates st_transform st_crs st_as_sf st_is_empty
#' @importFrom move2 mt_track_id
#' @importFrom data.table data.table setDT setorder
plot_regularized_track <- function(raw, reg, ids = NULL,
                                    id_col = "individual_local_identifier",
                                    time_col = "timestamp",
                                    facet = TRUE, show_se = TRUE,
                                    pairwise = TRUE,
                                    out_path = NULL,
                                    point_size = 1.4, path_size = 0.7,
                                    verbose = TRUE) {
  requireNamespace("sf")
  requireNamespace("data.table")
  suppressPackageStartupMessages(library(ggplot2))

  # ── 0. Prep `reg` (grab attrs before subsetting -- data.table's `[`
  #      doesn't preserve custom attributes) ────────────────────────────────
  data.table::setDT(reg)
  crs    <- attr(reg, "crs")
  dt_min <- attr(reg, "dt_min")
  if (is.null(ids)) ids <- unique(reg$id)
  reg <- reg[id %in% ids]
  if (nrow(reg) == 0) stop("No rows in `reg` for the requested `ids`.")
  many <- length(ids) > 1
  reg[, color_var := if (many) id else method]

  # ── 1. Pull raw lon/lat/id/time into a flat table ──────────────────────────
  raw_df <- .plot_regtrack_extract_lonlat(raw, id_col, time_col)
  raw_df <- raw_df[id %in% ids & !is.na(lon) & !is.na(lat) & !is.na(timestamp)]
  if (nrow(raw_df) == 0) stop("No raw fixes found for the requested `ids`.")
  data.table::setorder(raw_df, id, timestamp)

  # ── 2. Movement stats (raw vs. regularized) ────────────────────────────────
  if (verbose) message("Computing track stats...")
  track_stats <- .plot_regtrack_stats(raw_df, reg, crs)

  # ── 3. Pairwise distances between individuals ──────────────────────────────
  pw <- pw_summary <- NULL
  if (isTRUE(pairwise) && many) {
    if (!exists("pairwise_track_distance", mode = "function"))
      source("./R/pairwise_track_distance.R")
    if (verbose) message("Computing pairwise distances...")
    pw <- pairwise_track_distance(reg[, .(id, timestamp, x, y)])
    if (nrow(pw) > 0) pw_summary <- summarize_pairwise_distance(pw)
  }

  # ── 4. Map: raw fixes (x) vs. regularized track ────────────────────────────
  p_map <- ggplot() +
    geom_path(data = reg, aes(lon, lat, group = id, color = color_var),
              linewidth = path_size, alpha = 0.9) +
    { if (isTRUE(show_se) && any(!is.na(reg$se_x)))
        geom_point(data = reg[!is.na(se_x)],
                   aes(lon, lat, size = sqrt(se_x^2 + se_y^2)),
                   alpha = 0.4, shape = 16, color = "grey30") } +
    geom_point(data = raw_df, aes(lon, lat), shape = 4, size = point_size,
               color = "black", alpha = 0.8) +
    { if (many && isTRUE(facet)) facet_wrap(~id, scales = "free") } +
    scale_color_viridis_d(option = "turbo") +
    labs(x = "Longitude", y = "Latitude",
         color = if (many) "Individual" else "Method",
         size = "CTCRW SE (m)",
         title = "Raw fixes (×) vs. regularized track",
         subtitle = sprintf("dt = %s min | %d individual(s)",
                             if (is.null(dt_min)) "?" else dt_min, length(ids))) +
    theme_minimal(base_size = 11)

  # ── 5. Optional pairwise-distance panel ─────────────────────────────────────
  p_all <- p_map
  if (!is.null(pw) && nrow(pw) > 0) {
    p_pw <- ggplot(pw, aes(timestamp, distance / 1000, color = interaction(id1, id2))) +
      geom_path(alpha = 0.7) +
      labs(x = "Time", y = "Pairwise distance (km)", color = "Dyad") +
      theme_minimal(base_size = 11)

    if (requireNamespace("ggpubr", quietly = TRUE)) {
      p_all <- ggpubr::ggarrange(p_map, p_pw, ncol = 1, heights = c(2, 1))
    } else {
      if (verbose) message("ggpubr not available; returning map and distance panels separately.")
      p_all <- list(map = p_map, pairwise_distance = p_pw)
    }
  }

  # both plain ggplot objects and ggpubr::ggarrange() results carry class "gg";
  # only the no-ggpubr fallback (a plain list of two plots) doesn't.
  if (!is.null(out_path) && inherits(p_all, "gg"))
    ggsave(out_path, p_all, width = 8, height = if (!is.null(pw)) 9 else 6, dpi = 300)

  list(plot = p_all, map = p_map, track_stats = track_stats[],
       pairwise = pw, pairwise_summary = pw_summary)
}

# ── helpers ──────────────────────────────────────────────────────────────────

# Pull id/timestamp/lon/lat out of an sf/move2 object or data.frame.
# Mirrors the extraction step in regularize_tracks().
.plot_regtrack_extract_lonlat <- function(x, id_col, time_col) {
  if (inherits(x, "sf")) {
    geom <- sf::st_geometry(x)
    keep <- !sf::st_is_empty(geom)
    ll <- sf::st_coordinates(sf::st_transform(x[keep, ], 4326))
    id <- if (id_col %in% names(x)) x[[id_col]][keep] else move2::mt_track_id(x)[keep]
    ts <- x[[time_col]][keep]
    data.table::data.table(id = as.character(id), timestamp = ts,
                            lon = ll[, "X"], lat = ll[, "Y"])
  } else if (is.data.frame(x)) {
    lon_col <- intersect(c("lon", "location_long", ".lon"), names(x))[1]
    lat_col <- intersect(c("lat", "location_lat", ".lat"), names(x))[1]
    if (is.na(lon_col) || is.na(lat_col))
      stop("Could not find longitude/latitude columns in `raw`.")
    data.table::data.table(id = as.character(x[[id_col]]), timestamp = x[[time_col]],
                            lon = x[[lon_col]], lat = x[[lat_col]])
  } else {
    stop("`raw` must be an sf/move2 object or a data.frame.")
  }
}

# Distance/duration/speed for one ordered x/y path.
.plot_regtrack_path_stats <- function(x, y, timestamp) {
  o <- order(timestamp)
  x <- x[o]; y <- y[o]; timestamp <- timestamp[o]
  n <- length(timestamp)
  if (n < 2) return(list(n = n, distance_km = NA_real_, duration_h = NA_real_, speed_kmh = NA_real_))
  dist_km <- sum(sqrt(diff(x)^2 + diff(y)^2), na.rm = TRUE) / 1000
  dur_h <- as.numeric(difftime(timestamp[n], timestamp[1], units = "hours"))
  list(n = n, distance_km = dist_km, duration_h = dur_h,
       speed_kmh = if (dur_h > 0) dist_km / dur_h else NA_real_)
}

# Per-individual raw vs. regularized distance/duration/speed, projected into
# the same metric CRS regularize_tracks() used (so the two are comparable).
.plot_regtrack_stats <- function(raw_df, reg, crs) {
  raw_xy <- sf::st_coordinates(sf::st_transform(
    sf::st_as_sf(raw_df, coords = c("lon", "lat"), crs = 4326, remove = FALSE), crs))
  raw_df[, `:=`(x = raw_xy[, "X"], y = raw_xy[, "Y"])]

  raw_stats <- raw_df[, {
    s <- .plot_regtrack_path_stats(x, y, timestamp)
    data.table::data.table(n_raw_fix = s$n, raw_distance_km = s$distance_km,
                            duration_h = s$duration_h, raw_speed_kmh = s$speed_kmh)
  }, by = id]

  reg_stats <- reg[, {
    s <- .plot_regtrack_path_stats(x, y, timestamp)
    data.table::data.table(
      n_reg_points = s$n, reg_distance_km = s$distance_km, reg_speed_kmh = s$speed_kmh,
      method = method[1],
      mean_se_m = if (all(is.na(se_x))) NA_real_ else mean(sqrt(se_x^2 + se_y^2), na.rm = TRUE))
  }, by = id]

  merge(raw_stats, reg_stats, by = "id", all = TRUE)
}
