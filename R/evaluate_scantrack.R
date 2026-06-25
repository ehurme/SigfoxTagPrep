#' Evaluate scantrack plot quality and diagnose common issues
#'
#' Audits each tag's sensor column availability, tag_fell_off distribution, and
#' tag-type detection.  Returns a tidy summary and threshold-sweep table so you
#' can identify why a panel was blank or why tag_fell_off looks wrong, then
#' re-run scan_tracks() or detect_tag_fell_off() with corrected settings.
#'
#' The threshold sweep is SCALE-AWARE:
#'   - tinyfox_total_vedba   is a *cumulative* counter; the sweep uses per-step
#'     deltas (diff) so the thresholds are in "activity units per transmission"
#'     rather than the ever-growing total.  signal_type = "cumulative_delta".
#'   - tinyfox_activity_percent_last_24h is 0-100%; thresholds are drawn from
#'     the actual distribution (percentiles).  signal_type = "percent".
#'   - vedba_sum / vedba / tinyfox_vedba_burst_sum are per-transmission sums;
#'     thresholds are drawn from the actual distribution.
#'     signal_type = "per_tx".
#'
#' Thresholds in the sweep are always data-adaptive (percentiles of the
#' non-zero signal values for that tag).  You may also supply extra absolute
#' thresholds via extra_thresholds; these are labelled with the prefix "abs_".
#'
#' Typical workflow:
#'   audit  <- evaluate_scantrack(bats_full)
#'   audit$column_audit                       # which sensor panels would be blank
#'   audit$fell_off_audit                     # tag_fell_off distribution per tag
#'   audit$threshold_sweep                    # sweep across adaptive thresholds
#'   audit$flags %>% filter(any_flag)         # tags with problems
#'
#'   # Pick a threshold from the sweep for a given tag:
#'   audit$threshold_sweep %>%
#'     filter(tag == "NL_1234", at_end_only, !all_fell_off, !none_fell_off)
#'
#'   # Re-run fell-off detection with a revised threshold for flagged tags:
#'   bats_full <- revise_tag_fell_off(bats_full, audit, new_threshold = 5)
#'
#' @param bats_full  move2/sf/data.frame with all sensor rows.
#' @param tags       Character vector of individual IDs to check.
#'   NULL (default) = all tags in bats_full (via deployment_id or tag_col).
#' @param tag_col    Column name for individual identifier.
#' @param time_col   Column name for timestamps.
#' @param activity_threshold  TinyFox activity-percent threshold used in the
#'   current fell-off logic (passed through to the sweep).
#' @param min_inactive_run  Minimum consecutive inactive rows to call fell-off.
#' @param extra_thresholds  Optional numeric vector of additional absolute
#'   threshold values to test on top of the adaptive set (e.g. c(5, 50, 200)).
#'   Useful when you already know the approximate scale for a tag type.
#' @param sweep_quantiles  Quantile levels (0-1) used to generate adaptive
#'   thresholds from the signal distribution.  Default covers the lower tail
#'   and median well, which is where fell-off discrimination usually lives.
#' @param verbose    Print per-tag progress messages.
#'
#' @return Named list:
#'   $column_audit      — data.frame, one row per tag × sensor panel
#'   $fell_off_audit    — data.frame, one row per tag, tag_fell_off stats
#'   $threshold_sweep   — data.frame, one row per tag × threshold candidate
#'   $flags             — wide summary, one row per tag, logical flag columns
#'   $suggested_settings — list with recommended parameters per tag
evaluate_scantrack <- function(
    bats_full,
    tags               = NULL,
    tag_col            = "individual_local_identifier",
    time_col           = "timestamp",
    activity_threshold = 1,
    min_inactive_run   = 3L,
    # Absolute threshold values to include in the sweep in addition to the
    # data-adaptive percentile set.  Also accepted as 'test_thresholds' for
    # backward compatibility.
    extra_thresholds   = NULL,
    test_thresholds    = NULL,   # alias for extra_thresholds
    sweep_quantiles    = c(0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95),
    verbose            = TRUE
) {
  # Resolve alias
  if (!is.null(test_thresholds) && is.null(extra_thresholds))
    extra_thresholds <- test_thresholds
  require(dplyr)

  # ── helpers ─────────────────────────────────────────────────────────────────
  .drop_geo <- function(df) {
    if (inherits(df, "sf")) sf::st_drop_geometry(df) else as.data.frame(df)
  }
  .has_data <- function(df, col) {
    col %in% names(df) && any(!is.na(df[[col]]))
  }
  .pct <- function(x) round(100 * mean(x, na.rm = TRUE), 1)

  # ── classify how a VeDBA column should be interpreted ───────────────────────
  # Returns one of: "cumulative_delta", "percent", "per_tx"
  # This drives both the signal transformation and how thresholds are labelled.
  .vedba_scale <- function(col_name) {
    if (is.na(col_name) || is.null(col_name)) return(NA_character_)
    if (col_name == "tinyfox_total_vedba")              return("cumulative_delta")
    if (grepl("activity_percent", col_name))            return("percent")
    return("per_tx")
  }

  # ── expected sensor columns per tag type (checked in priority order) ─────────
  NANO_COLS <- list(
    vedba    = c("vedba_sum", "vedba", "tinyfox_vedba_burst_sum"),
    temp     = c("external_temperature", "avg_temp", "temperature",
                 "temperature_min", "temperature_max"),
    pressure = c("barometric_pressure"),
    altitude = c("altitude_m")
  )
  TINY_COLS <- list(
    vedba    = c("tinyfox_total_vedba", "tinyfox_vedba_burst_sum", "vedba_sum", "vedba"),
    activity = c("tinyfox_activity_percent_last_24h"),
    temp     = c("tinyfox_temperature_min_last_24h", "tinyfox_temperature_max_last_24h"),
    pressure = c("tinyfox_pressure_min_last_24h"),
    altitude = c("altitude_m")
  )
  UWASP_COLS <- list(
    vedba    = c("vedba_sum", "vedba"),
    temp     = c("temperature"),
    pressure = c("barometric_pressure"),
    altitude = c("altitude_m")
  )

  # ── prep ────────────────────────────────────────────────────────────────────
  df <- .drop_geo(bats_full)

  if (!tag_col %in% names(df))
    stop("tag_col '", tag_col, "' not found. Available: ",
         paste(names(df), collapse = ", "))
  if (!time_col %in% names(df))
    stop("time_col '", time_col, "' not found.")

  if (is.null(tags)) {
    tags <- if ("deployment_id" %in% names(df))
      unique(df$deployment_id)
    else
      unique(df[[tag_col]])
  }

  col_rows  <- list()
  fo_rows   <- list()
  sweep_rows <- list()
  sugg_list  <- list()

  for (tag in tags) {
    id_col <- if ("deployment_id" %in% names(df)) "deployment_id" else tag_col
    td <- df[df[[id_col]] == tag, , drop = FALSE]
    if (nrow(td) == 0) {
      if (verbose) message("Tag ", tag, ": 0 rows — skipping")
      next
    }
    td <- td[order(td[[time_col]]), , drop = FALSE]
    n  <- nrow(td)

    # ── 1. Tag-type detection ──────────────────────────────────────────────
    tag_type <- NA_character_
    if ("tag_type" %in% names(td) && any(!is.na(td$tag_type))) {
      raw <- tolower(as.character(td$tag_type[!is.na(td$tag_type)][1]))
      if (grepl("tiny", raw))      tag_type <- "tinyfox"
      else if (grepl("nano", raw)) tag_type <- "nanofox"
      else if (grepl("wasp", raw)) tag_type <- "uwasp"
    }
    if (is.na(tag_type) && "model" %in% names(td) && any(!is.na(td$model))) {
      raw <- tolower(as.character(td$model[!is.na(td$model)][1]))
      if (grepl("tiny", raw))      tag_type <- "tinyfox"
      else if (grepl("nano", raw)) tag_type <- "nanofox"
      else if (grepl("wasp", raw)) tag_type <- "uwasp"
    }

    firmware <- if ("tag_firmware" %in% names(td) && any(!is.na(td$tag_firmware)))
      as.character(td$tag_firmware[!is.na(td$tag_firmware)][1])
    else NA_character_

    type_detection_issue <- is.na(tag_type)

    expected_cols <- switch(tag_type,
      tinyfox = TINY_COLS,
      nanofox = NANO_COLS,
      uwasp   = UWASP_COLS,
      NANO_COLS
    )

    # ── 2. Column audit ───────────────────────────────────────────────────
    for (panel in names(expected_cols)) {
      candidates <- expected_cols[[panel]]
      found_col  <- NA_character_
      all_na     <- TRUE
      for (cand in candidates) {
        if (cand %in% names(td)) {
          if (any(!is.na(td[[cand]]))) {
            found_col <- cand; all_na <- FALSE; break
          } else {
            found_col <- cand  # present but all-NA
          }
        }
      }
      col_rows[[length(col_rows) + 1]] <- data.frame(
        tag          = tag,
        tag_type     = tag_type,
        firmware     = firmware,
        panel        = panel,
        candidates   = paste(candidates, collapse = "|"),
        found_col    = found_col,
        col_present  = !is.na(found_col),
        col_has_data = !is.na(found_col) && !all_na,
        stringsAsFactors = FALSE
      )
    }

    # ── 3. tag_fell_off audit ─────────────────────────────────────────────
    has_fo_col      <- "tag_fell_off" %in% names(td)
    fo_pct          <- NA_real_
    fo_contiguous   <- NA
    fo_starts_early <- NA
    fo_all_true     <- NA
    fo_all_false    <- NA

    if (has_fo_col && n > 0) {
      fo_vec <- as.logical(td$tag_fell_off)
      fo_vec[is.na(fo_vec)] <- FALSE
      fo_pct       <- .pct(fo_vec)
      fo_all_true  <- all(fo_vec)
      fo_all_false <- !any(fo_vec)
      if (any(fo_vec)) {
        first_true      <- which(fo_vec)[1]
        fo_contiguous   <- all(!fo_vec[seq_len(first_true - 1)])
        fo_starts_early <- first_true <= max(1L, round(0.25 * n))
      } else {
        fo_contiguous   <- TRUE
        fo_starts_early <- FALSE
      }
    }

    # ── 4. Identify VeDBA signal & its scale type ─────────────────────────
    # Priority: for TinyFox, prefer cumulative total → burst sum → activity %.
    # For NanoFox/uWasp, prefer vedba_sum → vedba.
    # The scale type determines how thresholds are generated and applied.

    vedba_col_found    <- NA_character_
    activity_col_found <- NA_character_

    if (isTRUE(tag_type == "tinyfox")) {
      for (v in c("tinyfox_total_vedba", "tinyfox_vedba_burst_sum",
                  "vedba_sum", "vedba")) {
        if (.has_data(td, v)) { vedba_col_found <- v; break }
      }
      if (.has_data(td, "tinyfox_activity_percent_last_24h"))
        activity_col_found <- "tinyfox_activity_percent_last_24h"
    } else {
      for (v in c("vedba_sum", "vedba")) {
        if (.has_data(td, v)) { vedba_col_found <- v; break }
      }
    }

    scale_type <- .vedba_scale(vedba_col_found)

    # ── 5. Build the activity signal appropriate for this scale type ──────
    # cumulative_delta: use per-step changes so the threshold is in
    #   "activity per transmission interval" — a biologically meaningful unit
    #   that doesn't inflate over deployment duration.
    # percent / per_tx: use raw values.

    signal <- NULL
    if (!is.na(vedba_col_found)) {
      raw_v  <- as.numeric(td[[vedba_col_found]])
      signal <- switch(scale_type,
        cumulative_delta = c(NA_real_, diff(raw_v)),
        raw_v
      )
    }

    # ── 6. Generate data-adaptive thresholds ─────────────────────────────
    # Percentiles of the positive, non-NA signal values for this tag.
    # This ensures the sweep covers the right range regardless of tag type
    # or firmware: a NanoFox vedba_sum might range 0-800; a cumulative delta
    # might range 0-150; an activity percent 0-100.
    # Fixed extra_thresholds can still be appended for explicit override.

    adaptive_thr <- numeric(0)
    if (!is.null(signal)) {
      pos_signal <- signal[!is.na(signal) & signal > 0]
      if (length(pos_signal) >= 3) {
        adaptive_thr <- unique(round(
          quantile(pos_signal, probs = sweep_quantiles, na.rm = TRUE), 2
        ))
      } else {
        # Too few positive values — fall back to 0 and max/2 and max
        mx <- max(signal, na.rm = TRUE)
        adaptive_thr <- unique(c(0, round(mx / 2, 2), round(mx, 2)))
      }
    }

    # Label each threshold with its origin for clarity in output
    thr_labels   <- paste0("q", round(sweep_quantiles * 100))
    if (length(adaptive_thr) != length(thr_labels))
      thr_labels <- paste0("q", seq_along(adaptive_thr))  # fallback labels

    extra_vals   <- if (!is.null(extra_thresholds)) extra_thresholds else numeric(0)
    all_thr      <- c(adaptive_thr, extra_vals)
    all_labels   <- c(thr_labels, paste0("abs_", extra_vals))

    # ── 7. Threshold sweep ────────────────────────────────────────────────
    for (ti in seq_along(all_thr)) {
      thr   <- all_thr[ti]
      label <- all_labels[ti]

      n_true   <- NA_integer_
      pct_true <- NA_real_
      at_end   <- NA
      all_t    <- NA
      none_t   <- NA

      if (!is.null(signal)) {
        # "active" based on the transformed signal
        active <- !is.na(signal) & signal > thr

        # TinyFox: also incorporate activity percent as OR signal
        if (!is.na(activity_col_found)) {
          a      <- as.numeric(td[[activity_col_found]])
          active <- active | (!is.na(a) & a > activity_threshold)
        }

        last_active  <- if (any(active)) max(which(active)) else 0L
        inactive_run <- n - last_active

        if (last_active < n &&
            inactive_run >= min_inactive_run &&
            (last_active == 0L || all(!active[(last_active + 1):n]))) {
          fell_start <- min(last_active + 2L, n + 1L)
          fo_new <- rep(FALSE, n)
          if (fell_start <= n) fo_new[fell_start:n] <- TRUE
        } else {
          fo_new <- rep(FALSE, n)
        }

        n_true   <- sum(fo_new)
        pct_true <- .pct(fo_new)
        all_t    <- all(fo_new)
        none_t   <- !any(fo_new)
        at_end   <- if (any(fo_new)) {
          ft <- which(fo_new)[1]
          all(!fo_new[seq_len(max(0L, ft - 1L))])
        } else TRUE
      }

      sweep_rows[[length(sweep_rows) + 1]] <- data.frame(
        tag           = tag,
        tag_type      = tag_type,
        firmware      = firmware,
        vedba_col     = vedba_col_found,
        signal_type   = scale_type,     # cumulative_delta | percent | per_tx
        thr_label     = label,          # q0, q50, abs_5, etc.
        threshold     = thr,
        n_fell_off    = n_true,
        pct_fell_off  = pct_true,
        at_end_only   = at_end,
        all_fell_off  = all_t,
        none_fell_off = none_t,
        stringsAsFactors = FALSE
      )
    }

    fo_rows[[length(fo_rows) + 1]] <- data.frame(
      tag                   = tag,
      tag_type              = tag_type,
      firmware              = firmware,
      n_rows                = n,
      type_detection_issue  = type_detection_issue,
      has_fell_off_col      = has_fo_col,
      fell_off_pct          = fo_pct,
      fell_off_all_true     = fo_all_true,
      fell_off_all_false    = fo_all_false,
      fell_off_contiguous   = fo_contiguous,
      fell_off_starts_early = fo_starts_early,
      vedba_col             = vedba_col_found,
      signal_type           = scale_type,
      activity_col          = activity_col_found,
      stringsAsFactors      = FALSE
    )

    # ── 8. Suggested threshold for this tag ───────────────────────────────
    # Prefer the lowest threshold where: fell_off is at end only, not all TRUE,
    # and not all FALSE.  This tends to be the most conservative valid threshold.
    sweep_tag <- do.call(rbind, sweep_rows[sapply(sweep_rows, function(r) r$tag == tag)])
    suggested_threshold <- NA_real_
    if (!is.null(sweep_tag) && nrow(sweep_tag) > 0) {
      good <- sweep_tag[!is.na(sweep_tag$at_end_only) &
                          sweep_tag$at_end_only == TRUE &
                          !is.na(sweep_tag$all_fell_off) &
                          sweep_tag$all_fell_off == FALSE &
                          !is.na(sweep_tag$none_fell_off) &
                          sweep_tag$none_fell_off == FALSE, ]
      if (nrow(good) > 0)
        suggested_threshold <- good$threshold[1]
    }

    sugg_list[[tag]] <- list(
      tag_type            = tag_type,
      firmware            = firmware,
      vedba_col           = vedba_col_found,
      signal_type         = scale_type,
      activity_col        = activity_col_found,
      suggested_threshold = suggested_threshold
    )

    if (verbose)
      message(sprintf(
        "  %-36s | %-8s | fw=%-24s | n=%d | fell_off=%s%% | %s [%s]",
        tag,
        ifelse(is.na(tag_type), "unknown", tag_type),
        ifelse(is.na(firmware), "?", firmware),
        n,
        ifelse(is.na(fo_pct), "?", as.character(fo_pct)),
        ifelse(is.na(vedba_col_found), "(no vedba col)", vedba_col_found),
        ifelse(is.na(scale_type), "?", scale_type)
      ))
  }

  # ── Assemble outputs ───────────────────────────────────────────────────────
  col_audit  <- do.call(rbind, col_rows)
  fo_audit   <- do.call(rbind, fo_rows)
  sweep_tbl  <- do.call(rbind, sweep_rows)

  rownames(col_audit) <- rownames(fo_audit) <- rownames(sweep_tbl) <- NULL

  col_flags <- col_audit %>%
    group_by(tag) %>%
    summarise(
      any_panel_missing = any(!col_has_data),
      missing_panels    = paste(panel[!col_has_data], collapse = ", "),
      .groups = "drop"
    )

  # isTRUE() is not vectorised — use explicit NA-safe comparisons
  .is_true  <- function(x) !is.na(x) & x == TRUE
  .is_false <- function(x) !is.na(x) & x == FALSE

  flags <- fo_audit %>%
    left_join(col_flags, by = "tag") %>%
    mutate(
      flag_type_unknown       = .is_true(type_detection_issue),
      flag_panel_missing      = .is_true(any_panel_missing),
      flag_fell_off_all       = .is_true(fell_off_all_true),
      flag_fell_off_none      = .is_true(fell_off_all_false) & !is.na(vedba_col),
      flag_fell_off_early     = .is_true(fell_off_starts_early),
      flag_fell_off_scattered = .is_false(fell_off_contiguous) & .is_false(fell_off_all_false),
      any_flag = flag_type_unknown | flag_panel_missing |
                 flag_fell_off_all | flag_fell_off_early | flag_fell_off_scattered
    )

  n_flagged <- sum(flags$any_flag, na.rm = TRUE)
  message(sprintf("\n── evaluate_scantrack ── %d tags audited, %d flagged ──",
                  length(tags), n_flagged))
  if (n_flagged > 0) {
    flagged <- flags[flags$any_flag, ]
    for (i in seq_len(nrow(flagged))) {
      r    <- flagged[i, ]
      bits <- c()
      if (isTRUE(r$flag_type_unknown))       bits <- c(bits, "unknown type")
      if (isTRUE(r$flag_panel_missing))      bits <- c(bits, paste("missing panels:", r$missing_panels))
      if (isTRUE(r$flag_fell_off_all))       bits <- c(bits, "tag_fell_off ALL TRUE")
      if (isTRUE(r$flag_fell_off_none))      bits <- c(bits, "tag_fell_off ALL FALSE (vedba present)")
      if (isTRUE(r$flag_fell_off_early))     bits <- c(bits, "fell_off starts early (<25%)")
      if (isTRUE(r$flag_fell_off_scattered)) bits <- c(bits, "fell_off not contiguous")
      message(sprintf("  %-40s  %s", r$tag, paste(bits, collapse = " | ")))
    }
  }

  invisible(list(
    column_audit       = col_audit,
    fell_off_audit     = fo_audit,
    threshold_sweep    = sweep_tbl,
    flags              = flags,
    suggested_settings = sugg_list
  ))
}


#' Re-run tag_fell_off detection with revised thresholds
#'
#' Takes the output of evaluate_scantrack() and applies per-tag or global
#' revised thresholds using detect_tag_fell_off().
#'
#' When the signal_type is "cumulative_delta" (tinyfox_total_vedba), the
#' threshold in suggested_settings is on the DELTA scale; detect_tag_fell_off
#' operates on the raw column, so the threshold is automatically back-converted
#' to a cumulative value by summing the expected per-step activity.
#' In practice, for cumulative VeDBA the best approach is to use
#' tinyfox_vedba_burst_sum or tinyfox_activity_percent_last_24h instead — set
#' override_col to switch to those if available.
#'
#' @param bats_full     Original move2/sf/data.frame.
#' @param audit         Output of evaluate_scantrack().
#' @param new_threshold Single numeric — override ALL tags with this threshold.
#'   NULL (default) uses audit$suggested_settings per tag.
#' @param override_col  Optional: use this column name instead of the one in
#'   audit$suggested_settings (e.g. "tinyfox_activity_percent_last_24h").
#' @param tag_col       Individual identifier column.
#' @param verbose       Print per-tag messages.
#' @return bats_full with `tag_fell_off` column updated.
revise_tag_fell_off <- function(
    bats_full,
    audit,
    new_threshold = NULL,
    override_col  = NULL,
    tag_col       = "individual_local_identifier",
    verbose       = TRUE
) {
  if (!exists("detect_tag_fell_off", mode = "function")) {
    for (.p in c("R/detect_tag_fell_off.R",
                 "../SigfoxTagPrep/R/detect_tag_fell_off.R")) {
      if (file.exists(.p)) { source(.p); break }
    }
  }

  id_col <- if ("deployment_id" %in% names(bats_full)) "deployment_id" else tag_col

  for (tag in names(audit$suggested_settings)) {
    s   <- audit$suggested_settings[[tag]]
    thr <- if (!is.null(new_threshold)) new_threshold else s$suggested_threshold

    if (is.na(thr) || is.null(thr)) {
      if (verbose) message("Tag ", tag, ": no suggested threshold — skipping")
      next
    }

    use_col <- if (!is.null(override_col)) override_col else s$vedba_col

    # Warn if the suggested threshold was on the delta scale but detect_tag_fell_off
    # will receive the raw cumulative column — the units won't match.
    if (isTRUE(s$signal_type == "cumulative_delta") && is.null(override_col)) {
      warning(
        "Tag ", tag, ": vedba_col is 'tinyfox_total_vedba' (cumulative). ",
        "The suggested threshold was computed on per-step deltas, but ",
        "detect_tag_fell_off() will receive the raw cumulative values. ",
        "Consider using override_col='tinyfox_activity_percent_last_24h' ",
        "or 'tinyfox_vedba_burst_sum' instead."
      )
    }

    method <- switch(s$tag_type,
      tinyfox = "Tinyfox",
      uwasp   = "uWasp",
      "Nanofox"  # default covers nanofox and NA
    )

    idx <- which(bats_full[[id_col]] == tag)
    if (length(idx) == 0L) next

    sub <- bats_full[idx, ]
    sub <- detect_tag_fell_off(
      sub,
      method          = method,
      tag_col         = tag_col,
      vedba_col       = if (!is.na(use_col)) use_col else "vedba_sum",
      activity_col    = if (!is.na(s$activity_col)) s$activity_col
                        else "tinyfox_activity_percent_last_24h",
      vedba_threshold = thr,
      verbose         = verbose
    )
    bats_full$tag_fell_off[idx] <- sub$tag_fell_off
  }

  bats_full
}


#' One-shot: audit → revise tag_fell_off → regenerate PNG for flagged tags
#'
#' scan_track_audit(bats_full, bats_loc, new_threshold = 5)
#'
#' @param bats_full     move2/sf/data.frame with all sensor rows.
#' @param bats_loc      move2/sf/data.frame with location-only rows.
#' @param tags          NULL = all tags; character vector = subset.
#' @param out_dir       Directory to write PNG files.
#' @param new_threshold Numeric threshold override for ALL tags.  NULL = use
#'   per-tag suggested thresholds from the audit.
#' @param extra_thresholds  Passed to evaluate_scantrack().
#' @param sweep_quantiles   Passed to evaluate_scantrack().
#' @param verbose       Progress messages.
#' @return Invisible list: $audit, $bats_revised.
scan_track_audit <- function(
    bats_full,
    bats_loc,
    tags            = NULL,
    out_dir         = "../../../Dropbox/MPI/Noctule/Plots/ScanTrack/",
    new_threshold   = NULL,
    extra_thresholds = NULL,
    sweep_quantiles = c(0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95),
    verbose         = TRUE
) {
  for (.p in c("R/scantrack.R", "../SigfoxTagPrep/R/scantrack.R")) {
    if (file.exists(.p) && !exists("scan_tracks", mode = "function")) {
      source(.p); break
    }
  }
  for (.p in c("R/detect_tag_fell_off.R", "../SigfoxTagPrep/R/detect_tag_fell_off.R")) {
    if (file.exists(.p) && !exists("detect_tag_fell_off", mode = "function")) {
      source(.p); break
    }
  }

  message("── Step 1: Auditing scantrack data ──")
  audit <- evaluate_scantrack(
    bats_full,
    tags             = tags,
    extra_thresholds = extra_thresholds,
    sweep_quantiles  = sweep_quantiles,
    verbose          = verbose
  )

  message("\n── Step 2: Revising tag_fell_off ──")
  bats_revised <- revise_tag_fell_off(
    bats_full,
    audit         = audit,
    new_threshold = new_threshold,
    verbose       = verbose
  )

  flagged_tags <- audit$flags$tag[isTRUE(audit$flags$any_flag)]
  if (length(flagged_tags) == 0) {
    message("No flagged tags — no plots regenerated.")
  } else {
    message(sprintf("\n── Step 3: Regenerating %d flagged plots ──", length(flagged_tags)))
    scan_tracks(
      bats_full = bats_revised,
      bats_loc  = bats_loc,
      out_dir   = out_dir,
      tags      = flagged_tags,
      overwrite = TRUE
    )
  }

  invisible(list(audit = audit, bats_revised = bats_revised))
}
