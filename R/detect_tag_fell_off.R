#' Detect likely tag fall-off events
#'
#' Determines whether a tag has likely fallen off an animal by identifying the
#' last transmission at which the animal was active and flagging subsequent
#' fixes as a potential detachment event.
#'
#' Logic by tag type:
#' \describe{
#'   \item{Nanofox}{Uses a VeDBA/activity column, usually \code{vedba_sum},
#'   with optional fallback columns. Values strictly above
#'   \code{vedba_threshold} are treated as active.}
#'   \item{TinyFox}{By default uses
#'   \code{tinyfox_activity_percent_last_24h} first. This avoids false activity
#'   caused by resets or jumps in cumulative \code{tinyfox_total_vedba}. VeDBA is
#'   used only when activity percent is missing, unless \code{tinyfox_signal}
#'   is changed.}
#'   \item{uWasp}{Uses inter-fix distance. This is conservative because Sigfox
#'   location jitter can mimic movement.}
#' }
#'
#' The first post-active/inactive location can be retained as
#' \code{tag_fell_off = FALSE} when \code{keep_first_inactive = TRUE}, because
#' the tag may physically have reached that coordinate before detachment.
#'
#' @param df A data frame, tibble, sf object, or move2 object.
#' @param method Character; one of \code{"Nanofox"}, \code{"Tinyfox"}, or
#'   \code{"uWasp"}. Lowercase variants are accepted.
#' @param tag_col Column name for the deployment/individual identifier.
#' @param time_col Column name for timestamps. Used to order fixes within tag.
#' @param vedba_col Primary VeDBA/activity column.
#' @param vedba_fallback_cols Optional fallback VeDBA/activity columns, tried in
#'   order if \code{vedba_col} is absent or all \code{NA} for a tag.
#' @param activity_col TinyFox activity-percent column.
#' @param dist_col uWasp inter-fix distance column, usually in km.
#' @param vedba_threshold Numeric; VeDBA/activity values strictly above this are
#'   active.
#' @param activity_threshold Numeric; TinyFox activity percent strictly above
#'   this is active.
#' @param dist_threshold Numeric; uWasp distance strictly above this is active.
#' @param min_inactive_run Integer; minimum consecutive inactive rows after the
#'   last active row required to declare fall-off.
#' @param keep_first_inactive Logical; keep the first post-active inactive row as
#'   \code{FALSE}. Fall-off then begins on the second inactive row.
#' @param tinyfox_signal TinyFox signal rule. \code{"activity_first"} is the
#'   recommended default. \code{"activity_only"}, \code{"vedba_only"}, and
#'   \code{"either"} are available for diagnostics.
#' @param tinyfox_total_vedba_col Cumulative TinyFox VeDBA counter. Used only as
#'   a last-resort fallback when no activity or VeDBA/rate column is available.
#' @param use_tinyfox_total_vedba_fallback Logical; if \code{TRUE}, derive a
#'   reset-safe diff/rate from \code{tinyfox_total_vedba_col} only when needed.
#' @param tinyfox_counter_mode For fallback from cumulative total, compare either
#'   \code{"rate"} or \code{"diff"} against \code{vedba_threshold}.
#' @param tinyfox_max_counter_rate Maximum plausible counter increase per hour
#'   when deriving a rate from cumulative total. Larger values are ignored as
#'   artifacts. Use \code{Inf} to disable.
#' @param tinyfox_max_counter_diff Maximum plausible counter increase between
#'   rows when deriving a diff from cumulative total. Larger values are ignored
#'   as artifacts. Use \code{Inf} to disable.
#' @param overwrite Logical; if \code{TRUE}, recompute \code{tag_fell_off} from
#'   scratch. If \code{FALSE}, preserve existing \code{TRUE} values.
#' @param verbose Logical; print per-tag diagnostic messages.
#'
#' @return The input object with logical column \code{tag_fell_off}.
#' @export
#'
#' @examples
#' # TinyFox recommended behavior:
#' # b2 <- tag_fell_off_tinyfox(b)
#' # which(b2$tag_fell_off)[1]
#'
detect_tag_fell_off <- function(
  df,
  method = c("Nanofox", "Tinyfox", "uWasp"),
  tag_col = "individual_local_identifier",
  time_col = "timestamp",
  vedba_col = "vedba_sum",
  vedba_fallback_cols = character(0),
  activity_col = "tinyfox_activity_percent_last_24h",
  dist_col = "distance",
  vedba_threshold = 1,
  activity_threshold = 1,
  dist_threshold = 0.5,
  min_inactive_run = 3L,
  keep_first_inactive = TRUE,
  tinyfox_signal = c("activity_first", "activity_only", "vedba_only", "either"),
  tinyfox_total_vedba_col = "tinyfox_total_vedba",
  use_tinyfox_total_vedba_fallback = TRUE,
  tinyfox_counter_mode = c("rate", "diff"),
  tinyfox_max_counter_rate = 50000,
  tinyfox_max_counter_diff = Inf,
  overwrite = TRUE,
  verbose = FALSE
) {
  method <- match.arg(tolower(method[1]), c("nanofox", "tinyfox", "uwasp"))
  tinyfox_signal <- match.arg(tinyfox_signal)
  tinyfox_counter_mode <- match.arg(tinyfox_counter_mode)

  if (!is.data.frame(df)) {
    stop("detect_tag_fell_off: 'df' must be a data frame, tibble, sf object, or move2 object.", call. = FALSE)
  }

  if (!tag_col %in% names(df)) {
    warning("detect_tag_fell_off: tag_col '", tag_col, "' not found. Returning unchanged.", call. = FALSE)
    if (!"tag_fell_off" %in% names(df)) df$tag_fell_off <- FALSE
    return(df)
  }

  if (!time_col %in% names(df)) {
    warning("detect_tag_fell_off: time_col '", time_col, "' not found. Using row order within each tag.", call. = FALSE)
  }

  min_inactive_run <- as.integer(min_inactive_run)
  if (is.na(min_inactive_run) || min_inactive_run < 1L) {
    stop("detect_tag_fell_off: 'min_inactive_run' must be an integer >= 1.", call. = FALSE)
  }

  if (isTRUE(overwrite) || !"tag_fell_off" %in% names(df)) {
    df$tag_fell_off <- FALSE
  } else {
    df$tag_fell_off <- as.logical(df$tag_fell_off)
    df$tag_fell_off[is.na(df$tag_fell_off)] <- FALSE
  }

  col_has_data <- function(dat, col) {
    is.character(col) && length(col) == 1L && !is.na(col) &&
      col %in% names(dat) && any(!is.na(dat[[col]]))
  }

  numeric_col <- function(dat, col) {
    if (!col_has_data(dat, col)) return(rep(NA_real_, nrow(dat)))
    suppressWarnings(as.numeric(dat[[col]]))
  }

  pick_numeric_col <- function(dat, primary, fallback = character(0)) {
    candidates <- unique(c(primary, fallback))
    candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
    for (cc in candidates) {
      if (col_has_data(dat, cc)) return(cc)
    }
    NA_character_
  }

  time_diff_hours <- function(times) {
    n <- length(times)
    out <- rep(NA_real_, n)
    if (n <= 1L) return(out)

    out[-1L] <- tryCatch(
      as.numeric(difftime(times[-1L], times[-n], units = "hours")),
      error = function(e) rep(NA_real_, n - 1L)
    )

    out[!is.finite(out) | out <= 0] <- NA_real_
    out
  }

  tinyfox_counter_signal <- function(tag_df) {
    n <- nrow(tag_df)
    out <- rep(FALSE, n)

    if (!isTRUE(use_tinyfox_total_vedba_fallback)) return(out)
    if (!col_has_data(tag_df, tinyfox_total_vedba_col)) return(out)

    total <- numeric_col(tag_df, tinyfox_total_vedba_col)
    d <- c(NA_real_, diff(total))

    # Counter reset / overflow / firmware restart. Example: 1.34e6 -> 1.99.
    d[!is.na(d) & d < 0] <- NA_real_

    if (tinyfox_counter_mode == "rate") {
      dt_h <- if (time_col %in% names(tag_df)) {
        time_diff_hours(tag_df[[time_col]])
      } else {
        rep(NA_real_, n)
      }

      rate <- d / dt_h
      rate[!is.finite(rate)] <- NA_real_
      rate[!is.na(rate) & rate > tinyfox_max_counter_rate] <- NA_real_
      out <- !is.na(rate) & rate > vedba_threshold
    } else {
      d[!is.na(d) & d > tinyfox_max_counter_diff] <- NA_real_
      out <- !is.na(d) & d > vedba_threshold
    }

    out
  }

  tag_values <- unique(as.character(df[[tag_col]]))
  tag_values <- tag_values[!is.na(tag_values)]

  for (tag in tag_values) {
    idx <- which(as.character(df[[tag_col]]) == tag)
    if (length(idx) <= 1L) next

    tag_df <- df[idx, , drop = FALSE]

    if (time_col %in% names(tag_df)) {
      time_order <- order(tag_df[[time_col]], na.last = TRUE)
    } else {
      time_order <- seq_len(nrow(tag_df))
    }

    tag_df <- tag_df[time_order, , drop = FALSE]
    n <- nrow(tag_df)

    active <- rep(FALSE, n)
    has_signal <- FALSE
    signal_used <- NA_character_

    if (method == "nanofox") {
      chosen_vedba_col <- pick_numeric_col(tag_df, vedba_col, vedba_fallback_cols)
      has_signal <- !is.na(chosen_vedba_col)

      if (has_signal) {
        v <- numeric_col(tag_df, chosen_vedba_col)
        active <- !is.na(v) & v > vedba_threshold
        signal_used <- chosen_vedba_col
      }
    }

    if (method == "tinyfox") {
      has_activity <- col_has_data(tag_df, activity_col)
      chosen_vedba_col <- pick_numeric_col(tag_df, vedba_col, vedba_fallback_cols)
      has_vedba <- !is.na(chosen_vedba_col)

      a_active <- rep(FALSE, n)
      v_active <- rep(FALSE, n)

      if (has_activity) {
        a <- numeric_col(tag_df, activity_col)
        a_active <- !is.na(a) & a > activity_threshold
      }

      if (has_vedba) {
        v <- numeric_col(tag_df, chosen_vedba_col)
        v_active <- !is.na(v) & v > vedba_threshold
      } else if (isTRUE(use_tinyfox_total_vedba_fallback) && col_has_data(tag_df, tinyfox_total_vedba_col)) {
        v_active <- tinyfox_counter_signal(tag_df)
        has_vedba <- TRUE
        chosen_vedba_col <- paste0(tinyfox_total_vedba_col, "_derived_", tinyfox_counter_mode)
      }

      active <- switch(
        tinyfox_signal,
        activity_only  = a_active,
        vedba_only     = v_active,
        either         = a_active | v_active,
        activity_first = if (has_activity) a_active else v_active
      )

      has_signal <- switch(
        tinyfox_signal,
        activity_only  = has_activity,
        vedba_only     = has_vedba,
        either         = has_activity || has_vedba,
        activity_first = has_activity || has_vedba
      )

      signal_used <- switch(
        tinyfox_signal,
        activity_only  = if (has_activity) activity_col else NA_character_,
        vedba_only     = if (has_vedba) chosen_vedba_col else NA_character_,
        either         = paste(c(if (has_activity) activity_col, if (has_vedba) chosen_vedba_col), collapse = " | "),
        activity_first = if (has_activity) activity_col else if (has_vedba) chosen_vedba_col else NA_character_
      )
    }

    if (method == "uwasp") {
      has_signal <- col_has_data(tag_df, dist_col)

      if (has_signal) {
        d <- numeric_col(tag_df, dist_col)
        active <- !is.na(d) & d > dist_threshold
        signal_used <- dist_col
      }
    }

    if (!has_signal) {
      if (isTRUE(verbose)) {
        message("detect_tag_fell_off: tag ", tag, " skipped; no usable signal for method '", method, "'.")
      }
      next
    }

    if (!any(active)) {
      first_inactive <- 1L
    } else {
      first_inactive <- max(which(active)) + 1L
    }

    if (first_inactive > n) {
      if (isTRUE(verbose)) {
        message("detect_tag_fell_off: tag ", tag, " no fall-off; active until final row. Signal: ", signal_used, ".")
      }
      next
    }

    inactive <- !active
    run_len <- n - first_inactive + 1L
    all_inactive_to_end <- all(inactive[first_inactive:n])
    ok_run <- run_len >= min_inactive_run

    if (all_inactive_to_end && ok_run) {
      fell_start <- if (isTRUE(keep_first_inactive)) first_inactive + 1L else first_inactive

      if (fell_start <= n) {
        tag_df$tag_fell_off[fell_start:n] <- TRUE
      }

      if (isTRUE(verbose)) {
        message(
          "detect_tag_fell_off: tag ", tag,
          " fall-off from sorted row ", fell_start,
          " after inactive run of ", run_len,
          " rows. Signal: ", signal_used, "."
        )
      }
    } else if (isTRUE(verbose)) {
      message(
        "detect_tag_fell_off: tag ", tag,
        " no fall-off; inactive run = ", run_len,
        ", all inactive to end = ", all_inactive_to_end,
        ". Signal: ", signal_used, "."
      )
    }

    # Map sorted tag rows back to the original row order for this tag.
    orig_order <- order(time_order)
    df$tag_fell_off[idx] <- tag_df$tag_fell_off[orig_order]
  }

  df
}

#' @rdname detect_tag_fell_off
#' @description Convenience wrapper for Nanofox tags.
#' @export
tag_fell_off_nanofox <- function(
  df,
  vedba_threshold = 0,
  vedba_col = "vedba_sum",
  vedba_fallback_cols = c("vedba"),
  min_inactive_run = 3L,
  ...
) {
  detect_tag_fell_off(
    df,
    method = "Nanofox",
    vedba_col = vedba_col,
    vedba_fallback_cols = vedba_fallback_cols,
    vedba_threshold = vedba_threshold,
    min_inactive_run = min_inactive_run,
    ...
  )
}

#' @rdname detect_tag_fell_off
#' @description Convenience wrapper for TinyFox/TinyFoxBatt tags. The default is
#'   activity-first, so cumulative VeDBA resets or jumps do not delay fall-off
#'   detection when \code{tinyfox_activity_percent_last_24h} is available.
#' @export
tag_fell_off_tinyfox <- function(
  df,
  vedba_threshold = 2440 * 3.9,
  activity_threshold = 1,
  vedba_col = "tinyfox_vedba_rate",
  vedba_fallback_cols = c("tinyfox_diff_vedba", "vedba_sum", "vedba"),
  activity_col = "tinyfox_activity_percent_last_24h",
  min_inactive_run = 3L,
  tinyfox_signal = "activity_first",
  ...
) {
  detect_tag_fell_off(
    df,
    method = "Tinyfox",
    vedba_col = vedba_col,
    vedba_fallback_cols = vedba_fallback_cols,
    activity_col = activity_col,
    vedba_threshold = vedba_threshold,
    activity_threshold = activity_threshold,
    min_inactive_run = min_inactive_run,
    tinyfox_signal = tinyfox_signal,
    ...
  )
}

#' @rdname detect_tag_fell_off
#' @description Convenience wrapper for uWasp tags. Distance-based fall-off is
#'   conservative because Sigfox location jitter can mimic movement.
#' @export
tag_fell_off_uwasp <- function(
  df,
  dist_threshold = 0.5,
  dist_col = "distance",
  min_inactive_run = 3L,
  ...
) {
  detect_tag_fell_off(
    df,
    method = "uWasp",
    dist_col = dist_col,
    dist_threshold = dist_threshold,
    min_inactive_run = min_inactive_run,
    ...
  )
}
