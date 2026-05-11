#' Detect tag fall-off events
#'
#' Determines whether a tag has likely fallen off an animal by identifying the
#' last transmission at which the animal was active and flagging all subsequent
#' fixes as a potential detachment event.
#'
#' The logic per tag type:
#' \describe{
#'   \item{nanofox}{Uses \code{vedba_sum} (sum of per-transmission VeDBA sensor
#'     rows) as the primary activity indicator.  A value above
#'     \code{vedba_threshold} marks the row as active.}
#'   \item{tinyfox}{Uses \code{tinyfox_activity_percent_last_24h} OR
#'     \code{vedba_sum} (from \code{tinyfox_total_vedba}).  A row is active if
#'     either exceeds its threshold.}
#'   \item{uwasp}{Uses inter-fix distance only.  Sigfox location jitter makes
#'     this unreliable; a conservative \code{dist_threshold} is recommended.}
#' }
#'
#' The \emph{first} post-active location is kept as \code{tag_fell_off = FALSE}
#' when \code{keep_first_inactive = TRUE} (default) because the tag physically
#' moved to that coordinate before detachment.  Fall-off marking begins at the
#' second consecutive inactive row.
#'
#' @param df A data frame or move2 object.
#' @param method Character; tag type — one of \code{"nanofox"}, \code{"tinyfox"},
#'   or \code{"uwasp"}.
#' @param tag_col Column name for the deployment/individual identifier.
#'   Default \code{"individual_local_identifier"}.
#' @param time_col Column name for timestamps. Default \code{"timestamp"}.
#' @param vedba_col Column name for VeDBA or activity rate. Default
#'   \code{"vedba_sum"} (the column added by \code{import_nanofox_movebank}).
#' @param activity_col Column name for percent activity (TinyFox only). Default
#'   \code{"tinyfox_activity_percent_last_24h"}.
#' @param dist_col Column name for inter-fix distance (uWasp). Default
#'   \code{"distance"} (km, as computed by \code{import_nanofox_movebank}).
#' @param vedba_threshold Numeric; VeDBA strictly above this value is active.
#'   Default \code{0} (any detected activity).
#' @param activity_threshold Numeric; activity percent strictly above this
#'   value is active. Default \code{0}.
#' @param dist_threshold Numeric; distance (km) strictly above this value is
#'   active (uWasp only). Default \code{0.5} to absorb Sigfox location jitter.
#' @param min_inactive_run Integer; minimum consecutive inactive rows after the
#'   last active row required to declare fall-off.  Counts from the first
#'   inactive row even though that row is kept as \code{FALSE}.  Default
#'   \code{3}.
#' @param keep_first_inactive Logical; if \code{TRUE} (default), the first
#'   post-active location is kept as \code{tag_fell_off = FALSE} — the tag was
#'   physically at that location just before detachment.
#' @param verbose Logical; print per-tag messages. Default \code{FALSE}.
#' @return The input object with an additional logical column
#'   \code{tag_fell_off}.  \code{TRUE} indicates that the fix occurred after the
#'   tag is likely to have detached.
#' @seealso \code{\link{tag_fell_off_nanofox}}, \code{\link{tag_fell_off_tinyfox}},
#'   \code{\link{tag_fell_off_uwasp}}
#' @export
detect_tag_fell_off <- function(
    df,
    method           = c("nanofox", "tinyfox", "uwasp"),
    tag_col          = "individual_local_identifier",
    time_col         = "timestamp",
    vedba_col        = "vedba_sum",
    activity_col     = "tinyfox_activity_percent_last_24h",
    dist_col         = "distance",
    vedba_threshold  = 0,
    activity_threshold = 0,
    dist_threshold   = 0.5,
    min_inactive_run = 3L,
    keep_first_inactive = TRUE,
    verbose          = FALSE
) {
  require(dplyr)

  method <- match.arg(method)

  col_exists <- function(x) x %in% names(df)

  # Soft validation: warn and return unchanged rather than stopping
  if (!col_exists(tag_col)) {
    warning("detect_tag_fell_off: tag_col '", tag_col, "' not found. Returning unchanged.")
    df$tag_fell_off <- FALSE
    return(df)
  }
  if (!col_exists(time_col)) {
    warning("detect_tag_fell_off: time_col '", time_col, "' not found. Returning unchanged.")
    df$tag_fell_off <- FALSE
    return(df)
  }

  if (method == "nanofox" && !col_exists(vedba_col)) {
    warning("detect_tag_fell_off [nanofox]: vedba_col '", vedba_col,
            "' not found. Returning unchanged.")
    df$tag_fell_off <- FALSE
    return(df)
  }
  if (method == "tinyfox" && !col_exists(vedba_col) && !col_exists(activity_col)) {
    warning("detect_tag_fell_off [tinyfox]: neither '", vedba_col, "' nor '",
            activity_col, "' found. Returning unchanged.")
    df$tag_fell_off <- FALSE
    return(df)
  }
  if (method == "uwasp" && !col_exists(dist_col)) {
    warning("detect_tag_fell_off [uwasp]: dist_col '", dist_col,
            "' not found. Returning unchanged.")
    df$tag_fell_off <- FALSE
    return(df)
  }

  if (!"tag_fell_off" %in% names(df)) df$tag_fell_off <- FALSE

  tags <- unique(df[[tag_col]])

  for (tag in tags) {
    idx <- which(df[[tag_col]] == tag)
    if (length(idx) == 0L) next

    tag_df <- df[idx, , drop = FALSE]
    time_order <- order(tag_df[[time_col]])
    tag_df <- tag_df[time_order, , drop = FALSE]
    n <- nrow(tag_df)
    if (n <= 1L) next

    # ---- define active rows ----
    active <- rep(FALSE, n)

    if (method == "nanofox") {
      if (col_exists(vedba_col)) {
        v <- as.numeric(tag_df[[vedba_col]])
        active <- active | (!is.na(v) & v > vedba_threshold)
      }
    }

    if (method == "tinyfox") {
      if (col_exists(vedba_col)) {
        v <- as.numeric(tag_df[[vedba_col]])
        active <- active | (!is.na(v) & v > vedba_threshold)
      }
      if (col_exists(activity_col)) {
        a <- as.numeric(tag_df[[activity_col]])
        active <- active | (!is.na(a) & a > activity_threshold)
      }
    }

    if (method == "uwasp") {
      if (col_exists(dist_col)) {
        d <- as.numeric(tag_df[[dist_col]])
        active <- active | (!is.na(d) & d > dist_threshold)
      }
    }

    # ---- find boundary ----
    # first_inactive: first row that is not active (check full span to end)
    # fell_start:     first row to be MARKED as tag_fell_off
    #   when keep_first_inactive = TRUE, the tag physically occupied that
    #   location before detaching — keep it as FALSE
    if (!any(active)) {
      first_inactive <- 1L
    } else {
      last_active    <- max(which(active))
      first_inactive <- last_active + 1L
    }

    if (first_inactive > n) {
      if (verbose) message("Tag ", tag, ": active through final record.")
      next
    }

    inactive <- !active

    # Require inactivity sustained from first_inactive through end (no recovery)
    all_inactive_to_end <- all(inactive[first_inactive:n])

    # Minimum run counts from first_inactive (including kept row)
    run_len <- n - first_inactive + 1L
    ok_run  <- run_len >= min_inactive_run

    if (all_inactive_to_end && ok_run) {
      fell_start <- if (isTRUE(keep_first_inactive)) {
        min(first_inactive + 1L, n + 1L)  # keep first inactive; start marking at +2
      } else {
        first_inactive
      }

      if (fell_start <= n) {
        tag_df$tag_fell_off[fell_start:n] <- TRUE
      }

      if (verbose) {
        kept_msg <- if (isTRUE(keep_first_inactive) && fell_start > first_inactive) {
          paste0(" (row ", first_inactive, " kept as valid last location)")
        } else ""
        message("Tag ", tag, ": fell off from row ", fell_start,
                " (", as.character(tag_df[[time_col]][min(fell_start, n)]), ")", kept_msg)
      }
    } else {
      if (verbose) message("Tag ", tag, ": inactivity not sustained to end or run too short.")
    }

    # Write back to original row order
    orig_order        <- order(time_order)
    df$tag_fell_off[idx] <- tag_df$tag_fell_off[orig_order]
  }

  df
}


#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_nanofox()} is a convenience wrapper for
#'   NanoFox tags, defaulting to \code{vedba_sum} as the activity column.
#' @export
tag_fell_off_nanofox <- function(
    df,
    vedba_threshold  = 0,
    vedba_col        = "vedba_sum",
    min_inactive_run = 3L,
    ...
) {
  detect_tag_fell_off(
    df,
    method           = "nanofox",
    vedba_col        = vedba_col,
    vedba_threshold  = vedba_threshold,
    min_inactive_run = min_inactive_run,
    ...
  )
}


#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_tinyfox()} is a convenience wrapper for
#'   TinyFox tags using activity percent and total VeDBA.
#' @export
tag_fell_off_tinyfox <- function(
    df,
    vedba_threshold    = 280800 * 3.9 / 1000 / (60 * 24),
    activity_threshold = 0,
    vedba_col          = "vedba_sum",
    activity_col       = "tinyfox_activity_percent_last_24h",
    min_inactive_run   = 3L,
    ...
) {
  detect_tag_fell_off(
    df,
    method             = "tinyfox",
    vedba_col          = vedba_col,
    activity_col       = activity_col,
    vedba_threshold    = vedba_threshold,
    activity_threshold = activity_threshold,
    min_inactive_run   = min_inactive_run,
    ...
  )
}


#' @rdname detect_tag_fell_off
#' @description \code{tag_fell_off_uwasp()} is a convenience wrapper for uWasp
#'   tags.  Sigfox location jitter (~500 m) makes this unreliable — a
#'   \code{dist_threshold} above the expected jitter radius is recommended.
#' @export
tag_fell_off_uwasp <- function(
    df,
    dist_threshold   = 0.5,
    dist_col         = "distance",
    min_inactive_run = 3L,
    ...
) {
  detect_tag_fell_off(
    df,
    method           = "uwasp",
    dist_col         = dist_col,
    dist_threshold   = dist_threshold,
    min_inactive_run = min_inactive_run,
    ...
  )
}

# load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")
# bats_loc %>% filter(tag_type == "uWasp") %>% tag_fell_off_uwasp()
# library(mapview)
# bat_tracks <- mt_track_lines(bats_loc)
# mapview(bat_tracks, zcol = "individual_local_identifier", legend = FALSE)
