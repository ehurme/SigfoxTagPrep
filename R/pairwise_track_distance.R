#' Find pairs of individuals whose tracks overlap in time
#'
#' Uses a sort-and-sweep over each track's [start, end] interval so that,
#' for deployments spanning many years/sites, individuals are only ever
#' compared against others that were tracked at the same time (and,
#' optionally, in the same group) -- not against every other individual in
#' the dataset.
#'
#' @param intervals A data.table with columns \code{id}, \code{t_min},
#'   \code{t_max}, and optionally \code{group}.
#' @return A data.table with columns \code{id1}, \code{id2}, \code{group}
#'   (one row per overlapping, same-group pair).
#' @importFrom data.table data.table rbindlist setorder
.overlapping_pairs <- function(intervals) {
  data.table::setorder(intervals, t_min)
  has_group <- "group" %in% names(intervals)
  n <- nrow(intervals)
  active <- integer(0)
  out <- vector("list", 0)
  for (i in seq_len(n)) {
    if (length(active)) {
      active <- active[intervals$t_max[active] >= intervals$t_min[i]]
    }
    if (length(active)) {
      for (j in active) {
        if (!has_group || identical(intervals$group[i], intervals$group[j])) {
          out[[length(out) + 1L]] <- data.table::data.table(
            id1 = intervals$id[j], id2 = intervals$id[i],
            group = if (has_group) intervals$group[i] else NA_character_)
        }
      }
    }
    active <- c(active, i)
  }
  if (!length(out)) {
    return(data.table::data.table(id1 = character(), id2 = character(), group = character()))
  }
  data.table::rbindlist(out)
}

#' Pairwise distances between regularized trajectories
#'
#' Computes the distance between every pair of individuals at every shared
#' (grid-aligned) timestamp, restricted to pairs whose tracks overlap in
#' time and, if a \code{group} column is present in \code{tracks} (e.g.
#' roost/colony/region), restricted to pairs within the same group.
#'
#' @param tracks A data.table as returned by \code{\link{regularize_tracks}}
#'   (or \code{\link{shift_tracks}}), with columns \code{id}, \code{timestamp},
#'   \code{x}, \code{y}, and optionally \code{group}.
#' @param max_pairs_warn Warn if more candidate pairs than this are found
#'   (a sign that grouping or time-overlap filtering isn't narrowing things
#'   down and this may be slow).
#' @return A long data.table: \code{id1}, \code{id2}, \code{group},
#'   \code{timestamp}, \code{distance} (metres, in the CRS used by
#'   \code{regularize_tracks}).
#' @importFrom data.table setDT setkey rbindlist
pairwise_track_distance <- function(tracks, max_pairs_warn = 20000) {
  data.table::setDT(tracks)
  has_group <- "group" %in% names(tracks)

  ivl_cols <- c("id", "t_min", "t_max", if (has_group) "group")
  ivl <- tracks[, .(t_min = min(timestamp), t_max = max(timestamp),
                     group = if (has_group) group[1] else NA_character_), by = id]

  pairs <- .overlapping_pairs(ivl)
  if (nrow(pairs) == 0) {
    return(data.table::data.table(id1 = character(), id2 = character(),
                                   group = character(), timestamp = as.POSIXct(character()),
                                   distance = numeric()))
  }
  if (nrow(pairs) > max_pairs_warn)
    warning(nrow(pairs), " overlapping pairs found; this may be slow. ",
            "Consider narrowing with a `group` column.")

  data.table::setkey(tracks, id)
  out <- vector("list", nrow(pairs))
  for (i in seq_len(nrow(pairs))) {
    a <- tracks[.(pairs$id1[i]), .(timestamp, x, y)]
    b <- tracks[.(pairs$id2[i]), .(timestamp, x, y)]
    m <- merge(a, b, by = "timestamp", suffixes = c("_1", "_2"))
    if (nrow(m) == 0) next
    out[[i]] <- data.table::data.table(
      id1 = pairs$id1[i], id2 = pairs$id2[i], group = pairs$group[i],
      timestamp = m$timestamp,
      distance = sqrt((m$x_1 - m$x_2)^2 + (m$y_1 - m$y_2)^2))
  }
  data.table::rbindlist(out)
}

#' Summarize pairwise distances into one row per dyad
#'
#' @param pw Output of \code{\link{pairwise_track_distance}}.
#' @param threshold_m Optional proximity threshold (metres); if given, adds
#'   the proportion of shared fixes within this distance.
#' @return A data.table with one row per (id1, id2) pair.
#' @importFrom data.table setDT
summarize_pairwise_distance <- function(pw, threshold_m = NULL) {
  data.table::setDT(pw)
  if (nrow(pw) == 0) {
    return(data.table::data.table(id1 = character(), id2 = character(), group = character(),
                                   n_fixes = integer(), overlap_start = as.POSIXct(character()),
                                   overlap_end = as.POSIXct(character()), mean_distance_m = numeric(),
                                   median_distance_m = numeric(), min_distance_m = numeric(),
                                   prop_within_threshold = numeric()))
  }
  pw[, .(
    n_fixes = .N,
    overlap_start = min(timestamp),
    overlap_end = max(timestamp),
    mean_distance_m = mean(distance),
    median_distance_m = stats::median(distance),
    min_distance_m = min(distance),
    prop_within_threshold = if (!is.null(threshold_m)) mean(distance <= threshold_m) else NA_real_
  ), by = .(id1, id2, group)]
}
