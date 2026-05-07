#' Detect tag fall-off events for multiple tag types
#'
#' Determines whether a tag has likely fallen off an animal by identifying the
#' last point at which the animal was active and marking all subsequent fixes as
#' a potential detachment event. Supports TinyFox, NanoFox, and uWasp tags.
#'
#' @param df A data frame or move2 object.
#' @param method Character; tag type, one of \code{"tinyfox"}, \code{"nanofox"},
#'   or \code{"uwasp"}.
#' @param tag_col Column name for the individual/tag identifier. Default
#'   \code{"individual_local_identifier"}.
#' @param time_col Column name for timestamps. Default \code{"timestamp"}.
#' @param vedba_col Column name for VeDBA or activity rate (TinyFox and NanoFox).
#'   Default \code{"vpm"}.
#' @param activity_col Column name for percent activity (TinyFox only). Default
#'   \code{"tinyfox_activity_percent_last_24h"}.
#' @param dist_col Column name for inter-fix distance (NanoFox and uWasp).
#'   Default \code{"dist_prev"}.
#' @param temp_col Column name for temperature (NanoFox only, used to detect
#'   thermal stasis). Default \code{"temperature"}.
#' @param vedba_threshold Numeric; VeDBA below this value is considered inactive.
#'   Default corresponds to a motionless TinyFox tag.
#' @param activity_threshold Numeric; activity percent threshold. Default \code{0}.
#' @param dist_threshold Numeric; minimum inter-fix distance (same units as
#'   \code{dist_col}) to be considered active. Default \code{25}.
#' @param dtemp_threshold Numeric; minimum absolute temperature change (°C)
#'   between successive fixes considered active (NanoFox only). Default \code{0.5}.
#' @param min_inactive_run Integer; minimum consecutive inactive rows required
#'   after the last active row to declare fall-off. Default \code{3}.
#' @param keep_last_row_if_all_na Logical; if the final row has all-NA key
#'   metrics, do not flag it as fell-off. Default \code{TRUE}.
#' @param verbose Logical; print per-tag messages. Default \code{FALSE}.
#' @return The input data frame with an additional logical column
#'   \code{tag_fell_off}. \code{TRUE} indicates fixes after the last active
#'   point and a sustained inactive period.
#' @seealso \code{\link{tag_fell_off_tinyfox}}, \code{\link{tag_fell_off_nanofox}},
#'   \code{\link{tag_fell_off_uwasp}}
detect_tag_fell_off <- function(
    df,
    method = c("tinyfox", "nanofox", "uwasp"),
    tag_col  = "individual_local_identifier",
    time_col = "timestamp",

    # Tinyfox fields
    vedba_col     = "vpm",
    activity_col  = "tinyfox_activity_percent_last_24h",

    # Nanofox/uWasp fields
    dist_col      = "dist_prev",   # distance between fixes (same units as dist_threshold)
    temp_col      = "temperature", # temperature column for nanofox (e.g., external temp)

    # Thresholds
    vedba_threshold     = 280800 * 3.9 / 1000 / (60 * 24),
    activity_threshold  = 0,
    dist_threshold      = 25,       # choose based on your distance units + sampling interval
    dtemp_threshold     = 0.5,      # °C change between successive points to count as "active"

    # Robustness / edge handling
    min_inactive_run    = 3,        # require at least N consecutive inactive rows after last active
    keep_last_row_if_all_na = TRUE, # avoid labeling a final all-NA row as fell_off
    verbose = FALSE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyverse)
    library(move2)
    library(sf)
    library(rlang)
  })

  method <- match.arg(method)

  required_cols <- c(tag_col, time_col)
  missing_req <- setdiff(required_cols, names(df))
  if (length(missing_req) > 0) {
    stop("Missing required columns: ", paste(missing_req, collapse = ", "))
  }

  # Helper: safe column fetch
  col_exists <- function(x) x %in% names(df)

  # Validate method-specific columns
  if (method == "tinyfox") {
    if (!col_exists(vedba_col)) stop("Missing vedba_col: ", vedba_col)
    if (!col_exists(activity_col)) stop("Missing activity_col: ", activity_col)
  }
  if (method %in% c("nanofox", "uwasp")) {
    if (!col_exists(dist_col)) stop("Missing dist_col: ", dist_col)
  }
  if (method == "nanofox") {
    if (!col_exists(vedba_col)) stop("Missing vedba_col (for nanofox): ", vedba_col)
    if (!col_exists(temp_col))  stop("Missing temp_col (for nanofox): ", temp_col)
  }

  df <- df %>%
    mutate(tag_fell_off = FALSE)

  tags <- unique(df[[tag_col]])

  for (tag in tags) {
    idx <- which(df[[tag_col]] == tag)
    if (length(idx) == 0) next

    tag_df <- df[idx, , drop = FALSE] %>%
      arrange(.data[[time_col]])

    n <- nrow(tag_df)
    if (n <= 1) next

    # ---- define "active" per row depending on tag type ----
    active <- rep(FALSE, n)

    if (method == "tinyfox") {
      v <- tag_df[[vedba_col]]
      a <- tag_df[[activity_col]]

      active <- (v >= vedba_threshold) | (a > activity_threshold)
      active[is.na(active)] <- FALSE
    }

    if (method == "uwasp") {
      d <- tag_df[[dist_col]]
      active <- (d >= dist_threshold)
      active[is.na(active)] <- FALSE
    }

    if (method == "nanofox") {
      d <- tag_df[[dist_col]]
      v <- tag_df[[vedba_col]]
      t <- tag_df[[temp_col]]

      # temp change between successive fixes
      dt <- c(NA_real_, abs(diff(t)))

      # A row is "active" if ANY of these suggests movement/activity
      active <- (d >= dist_threshold) | (v >= vedba_threshold) | (dt >= dtemp_threshold)

      active[is.na(active)] <- FALSE
    }

    # ---- find fall-off start: first row AFTER last active ----
    if (!any(active)) {
      # Never active: mark as fell off from the first row
      fell_start <- 1L
    } else {
      last_active <- max(which(active))
      fell_start <- last_active + 1L
    }

    # If fall-start is beyond data, nothing to mark
    if (fell_start > n) {
      if (verbose) message("Tag ", tag, ": no fall-off segment detected (fell_start > n).")
      next
    }

    # ---- confirm "never returns above thresholds" ----
    inactive <- !active
    inactive[is.na(inactive)] <- TRUE

    # Require that from fell_start onward, ALL rows are inactive
    all_inactive_to_end <- all(inactive[fell_start:n])

    # Optional guard: require minimum run length of inactive rows
    run_len <- n - fell_start + 1L
    ok_run  <- run_len >= min_inactive_run

    if (all_inactive_to_end && ok_run) {
      tag_df$tag_fell_off[fell_start:n] <- TRUE

      # Optional: if the final row has all NA in key metric(s), keep it FALSE
      if (keep_last_row_if_all_na) {
        last_i <- n

        all_na_last <- switch(
          method,
          tinyfox = all(is.na(tag_df[last_i, c(vedba_col, activity_col), drop = TRUE])),
          uwasp   = is.na(tag_df[last_i, dist_col, drop = TRUE]),
          nanofox = all(is.na(tag_df[last_i, c(dist_col, vedba_col, temp_col), drop = TRUE]))
        )

        if (isTRUE(all_na_last) && fell_start <= (n - 1L) && isFALSE(tag_df$tag_fell_off[n - 1L])) {
          tag_df$tag_fell_off[n] <- FALSE
        }
      }

      if (verbose) {
        message("Tag ", tag, ": fell off from row ", fell_start,
                " (", as.character(tag_df[[time_col]][fell_start]), ").")
      }
    } else {
      if (verbose) message("Tag ", tag, ": inactivity not sustained to end or run too short.")
    }

    # write back (preserving original row order within tag)
    # We arranged tag_df by time; map back via ordering
    ord <- order(df[idx, time_col])
    df$tag_fell_off[idx[ord]] <- tag_df$tag_fell_off
  }

  return(df)
}

# ---- convenience wrappers ----

#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_tinyfox()} is a convenience wrapper for
#'   \code{detect_tag_fell_off(method = "tinyfox")}.
tag_fell_off_tinyfox <- function(
    df,
    vedba_threshold = 280800 * 3.9 / 1000 / (60 * 24),
    activity_threshold = 0,
    ...
) {
  detect_tag_fell_off(
    df,
    method = "tinyfox",
    vedba_threshold = vedba_threshold,
    activity_threshold = activity_threshold,
    ...
  )
}

#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_nanofox()} is a convenience wrapper for
#'   \code{detect_tag_fell_off(method = "nanofox")}.
tag_fell_off_nanofox <- function(
    df,
    dist_threshold = 25,
    dtemp_threshold = 0.5,
    vedba_threshold = 280800 * 3.9 / 1000 / (60 * 24),
    dist_col = "dist_prev",
    temp_col = "temperature",
    vedba_col = "vpm",
    ...
) {
  detect_tag_fell_off(
    df,
    method = "nanofox",
    dist_threshold = dist_threshold,
    dtemp_threshold = dtemp_threshold,
    vedba_threshold = vedba_threshold,
    dist_col = dist_col,
    temp_col = temp_col,
    vedba_col = vedba_col,
    ...
  )
}

#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_uwasp()} is a convenience wrapper for
#'   \code{detect_tag_fell_off(method = "uwasp")}.
tag_fell_off_uwasp <- function(
    df,
    dist_threshold = 25,
    dist_col = "dist_prev",
    ...
) {
  detect_tag_fell_off(
    df,
    method = "uwasp",
    dist_threshold = dist_threshold,
    dist_col = dist_col,
    ...
  )
}

# load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")
# bats_loc %>% filter(tag_type == "uWasp") %>% tag_fell_off_uwasp()
# library(mapview)
# bat_tracks <- mt_track_lines(bats_loc)
# mapview(bat_tracks, zcol = "individual_local_identifier", legend = FALSE)


