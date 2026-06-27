# =============================================================================
# correct_time_drift.R
# Clock drift detection and timestamp correction for biologger tag data
#
# Background
# ----------
# Tags without a real-time clock accumulate timing error between
# transmissions. Drift is observable from the difference between the
# observed inter-message interval and the programmed interval, and has
# two components:
#   1. A tag-specific baseline offset (distinct per device)
#   2. A temperature-dependent component (greater drift at low temperatures)
#
# The receipt timestamp anchors the *end* of each measurement window.
# Corrected start times are derived by scaling the programmed window
# duration by (1 + predicted_drift).
# =============================================================================

library(dplyr)
library(lme4)


# -----------------------------------------------------------------------------
#' Detect and correct clock drift in wildcloud biologger messages
#'
#' @param data          Data frame; one row per received message.
#' @param tag_col       Column identifying the tag/device (character or factor).
#' @param timestamp_col Column with message receipt time (POSIXct). This
#'                      anchors the *end* of the measurement window.
#' @param start_col     Column with programmed window start time (POSIXct).
#' @param end_col       Column with programmed window end time (POSIXct).
#' @param temp_col      Column with mean temperature over the window (°C).
#'                      Set to NULL to skip the temperature term.
#' @param programmed_interval  Expected inter-message interval in seconds.
#'                      If NULL, inferred per tag as the median observed lag
#'                      (suitable when most messages are on-schedule).
#' @param poly_degree   Polynomial degree for the temperature fixed effect
#'                      (default 2; matches your empirical model).
#' @param max_drift_filter  Maximum |drift| fraction retained for model
#'                      fitting (default 0.10 = 10%; removes garbled/
#'                      duplicate messages before fitting).
#' @param min_obs_per_tag   Minimum per-tag observations required for that
#'                      tag to contribute its own random effect (default 5).
#'                      Tags below this threshold still get predictions via
#'                      the population-level fixed effects.
#'
#' @return  The input data frame with additional columns:
#'   * `observed_lag_s`    – Observed inter-message interval (s)
#'   * `programmed_interval_s` – Reference interval used (s)
#'   * `drift_observed`    – Empirical drift fraction for this lag
#'   * `drift_predicted`   – Model-predicted drift fraction
#'   * `drift_source`      – Which model branch produced the prediction
#'   * `window_duration_s` – Original programmed window length (s)
#'   * `true_window_s`     – Drift-corrected window length (s)
#'   * `start_corrected`   – Corrected window start (POSIXct)
#'   * `end_corrected`     – Corrected window end = receipt timestamp (POSIXct)
#'
#'   The fitted lme4 model is attached as `attr(result, "drift_model")`.
# -----------------------------------------------------------------------------
correct_time_drift <- function(
    data,
    tag_col              = "tag_id",
    timestamp_col        = "timestamp",
    start_col            = "start_time",
    end_col              = "end_time",
    temp_col             = "avg_temp",
    programmed_interval  = NULL,
    poly_degree          = 2L,
    max_drift_filter     = 0.10,
    min_obs_per_tag      = 5L
) {

  # ── 1. Input validation ────────────────────────────────────────────────────
  required_cols <- c(tag_col, timestamp_col, start_col, end_col)
  missing_cols  <- setdiff(required_cols, names(data))
  if (length(missing_cols))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))

  use_temp <- !is.null(temp_col) && temp_col %in% names(data)
  if (!is.null(temp_col) && !use_temp)
    warning("Temperature column '", temp_col,
            "' not found — fitting without temperature.")

  for (col in c(timestamp_col, start_col, end_col))
    if (!inherits(data[[col]], "POSIXct"))
      data[[col]] <- as.POSIXct(data[[col]])

  # ── 2. Sort and compute inter-message lag per tag ──────────────────────────
  data <- data |>
    arrange(.data[[tag_col]], .data[[timestamp_col]]) |>
    group_by(.data[[tag_col]]) |>
    mutate(
      observed_lag_s = as.numeric(
        difftime(.data[[timestamp_col]],
                 lag(.data[[timestamp_col]]),
                 units = "secs")
      )
    ) |>
    ungroup()

  # ── 3. Programmed window / inter-message interval ──────────────────────────
  # Measurement window as recorded by the tag's (drifting) clock
  data <- data |>
    mutate(window_duration_s = as.numeric(
      difftime(.data[[end_col]], .data[[start_col]], units = "secs")
    ))

  if (is.null(programmed_interval)) {
    tag_intervals <- data |>
      filter(!is.na(observed_lag_s)) |>
      group_by(.data[[tag_col]]) |>
      summarise(programmed_interval_s = median(observed_lag_s, na.rm = TRUE),
                .groups = "drop")
    data <- left_join(data, tag_intervals, by = tag_col)
    message("[drift] Programmed interval inferred per tag from median observed lag.")
  } else {
    data$programmed_interval_s <- as.numeric(programmed_interval)
    message("[drift] Fixed programmed interval: ", programmed_interval, " s.")
  }

  # ── 4. Observed drift fraction ─────────────────────────────────────────────
  # drift > 0  →  clock ran slow (took longer than programmed)
  # drift < 0  →  clock ran fast (arrived earlier than programmed)
  data <- data |>
    mutate(
      drift_observed = (observed_lag_s - programmed_interval_s) / programmed_interval_s
    )

  # ── 5. Build model-fitting subset ─────────────────────────────────────────
  #
  # lme4's predict.merMod does not reliably handle non-syntactic column names

  # (e.g. "temperature [°C]") in formulas or newdata. We work around this by
  # creating safe aliases (.tag, .temp) for fitting and prediction, then
  # dropping them afterwards.
  # ─────────────────────────────────────────────────────────────────────────

  data$.tag <- data[[tag_col]]
  if (use_temp) data$.temp <- data[[temp_col]]

  fit_data <- data |>
    filter(
      !is.na(drift_observed),
      !is.na(observed_lag_s),
      abs(drift_observed) <= max_drift_filter
    )

  if (use_temp)
    fit_data <- filter(fit_data, !is.na(.temp))

  tag_counts    <- count(fit_data, .tag, name = "n_obs")
  tags_enough   <- filter(tag_counts, n_obs >= min_obs_per_tag) |>
    pull(.tag)
  fit_data_full <- filter(fit_data, .tag %in% tags_enough)

  message(sprintf(
    "[drift] Model fitting: %d obs from %d tags (≥%d obs/tag); %d tags will use pop. fixed effects.",
    nrow(fit_data_full), length(tags_enough), min_obs_per_tag,
    n_distinct(data$.tag) - length(tags_enough)
  ))

  # ── 6. Fit drift model (waterfall of fallbacks) ────────────────────────────
  model      <- NULL
  model_type <- NA_character_

  ## 6a. Best case: mixed model with polynomial temperature
  if (use_temp && nrow(fit_data_full) >= 10) {
    fmla <- as.formula(sprintf(
      "drift_observed ~ poly(.temp, %d, raw = TRUE) + (.temp | .tag)",
      poly_degree
    ))
    model <- tryCatch(
      lmer(fmla, data = fit_data_full, REML = TRUE,
           control = lmerControl(optimizer = "bobyqa")),
      error = function(e) {
        warning("[drift] Mixed temperature model failed (", conditionMessage(e),
                ") — trying simpler random slope.")
        NULL
      }
    )
    if (!is.null(model)) model_type <- "mixed_poly_temp"
  }

  ## 6b. Fallback: random intercept + linear temperature
  if (is.null(model) && use_temp && nrow(fit_data_full) >= 10) {
    fmla <- drift_observed ~ .temp + (1 | .tag)
    model <- tryCatch(
      lmer(fmla, data = fit_data_full, REML = TRUE),
      error = function(e) {
        warning("[drift] Linear temperature model failed (", conditionMessage(e), ").")
        NULL
      }
    )
    if (!is.null(model)) model_type <- "mixed_linear_temp"
  }

  ## 6c. Fallback: random intercept only (no temperature)
  if (is.null(model) && nrow(fit_data) >= 5) {
    fmla <- drift_observed ~ 1 + (1 | .tag)
    model <- tryCatch(
      lmer(fmla, data = fit_data, REML = TRUE),
      error = function(e) {
        warning("[drift] Random-intercept model failed (", conditionMessage(e), ").")
        NULL
      }
    )
    if (!is.null(model)) model_type <- "random_intercept"
  }

  message("[drift] Model fitted: ", ifelse(is.null(model), "NONE", model_type))

  # ── 7. Predict drift for all rows ─────────────────────────────────────────
  #    data already has .tag and .temp so predict can find them
  if (!is.null(model)) {
    data$drift_predicted <- tryCatch(
      as.numeric(predict(model, newdata = data,
                         re.form  = NULL,
                         allow.new.levels = TRUE)),
      error = function(e) {
        warning("[drift] Full prediction failed; using fixed effects only: ",
                conditionMessage(e))
        as.numeric(predict(model, newdata = data, re.form = NA))
      }
    )
    # Record whether tag had enough data for its own random effect
    data$drift_source <- ifelse(
      data$.tag %in% tags_enough,
      model_type,
      paste0(model_type, "_popRE")  # population random effect only
    )
  } else {
    # Last resort: global median of observed drifts
    global_drift <- median(fit_data$drift_observed, na.rm = TRUE)
    data$drift_predicted <- global_drift
    data$drift_source    <- "global_median"
    warning(sprintf(
      "[drift] No model fitted. Applying global median drift = %.5f to all rows.",
      global_drift
    ))
  }

  # Clean up internal aliases
  data$.tag  <- NULL
  data$.temp <- NULL

  # ── 8. Correct window times ────────────────────────────────────────────────
  # Receipt timestamp reliably anchors the window END.
  # True (real-clock) window duration = programmed_duration × (1 + drift)
  # because a slow clock (drift > 0) means more real time elapsed than
  # the tag's clock counted.
  data <- data |>
    mutate(
      true_window_s   = window_duration_s * (1 + drift_predicted),
      end_corrected   = .data[[timestamp_col]],
      start_corrected = end_corrected - true_window_s
    )

  # ── 9. Print summary ───────────────────────────────────────────────────────
  summ <- data |>
    summarise(
      mean_pct  = mean(drift_predicted * 100, na.rm = TRUE),
      sd_pct    = sd(drift_predicted   * 100, na.rm = TRUE),
      max_pct   = max(abs(drift_predicted) * 100, na.rm = TRUE),
      shift_s   = mean(true_window_s - window_duration_s, na.rm = TRUE)
    )
  message(sprintf(
    "[drift] Correction summary — mean: %+.3f%%  SD: %.3f%%  max|drift|: %.3f%%  mean window shift: %+.1f s",
    summ$mean_pct, summ$sd_pct, summ$max_pct, summ$shift_s
  ))

  attr(data, "drift_model")      <- model
  attr(data, "drift_model_type") <- model_type
  data
}


# -----------------------------------------------------------------------------
#' Diagnostic plots for a drift-corrected dataset
#'
#' @param corrected_data  Output of `correct_time_drift()`.
#' @param tag_col         Same tag column name used in correction.
#' @param temp_col        Temperature column name, or NULL.
#' @param n_tags_sample   Max tags to show in the per-tag panel (default 16).
#'
#' @return A named list of ggplot objects:
#'   $drift_vs_temp   – Population-level drift ~ temperature with model line
#'   $drift_by_tag    – Per-tag drift distributions
#'   $window_shift    – Histogram of window length corrections (seconds)
# -----------------------------------------------------------------------------
plot_drift_diagnostics <- function(
    corrected_data,
    tag_col       = "tag_id",
    temp_col      = "avg_temp",
    n_tags_sample = 16L
) {
  if (!requireNamespace("ggplot2", quietly = TRUE))
    stop("ggplot2 is required for diagnostic plots.")
  library(ggplot2)

  plots <- list()

  # -- drift vs temperature ---------------------------------------------------
  use_temp <- !is.null(temp_col) && temp_col %in% names(corrected_data)
  if (use_temp) {
    plot_df <- corrected_data |>
      filter(!is.na(drift_observed), !is.na(.data[[temp_col]]),
             abs(drift_observed) <= 0.15)

    plots$drift_vs_temp <- ggplot(plot_df,
                                  aes(x = .data[[temp_col]], y = drift_observed * 100)) +
      geom_point(alpha = 0.08, size = 0.8, colour = "steelblue") +
      geom_line(aes(y = drift_predicted * 100), colour = "firebrick",
                linewidth = 0.9) +
      labs(
        title    = "Clock drift vs. temperature",
        subtitle = "Points = observed; red line = model prediction (pop. level)",
        x        = "Mean temperature (°C)",
        y        = "Drift (%)"
      ) +
      theme_bw()
  }

  # -- per-tag drift distribution --------------------------------------------
  all_tags   <- unique(corrected_data[[tag_col]])
  shown_tags <- if (length(all_tags) > n_tags_sample)
    sample(all_tags, n_tags_sample) else all_tags

  tag_df <- corrected_data |>
    filter(.data[[tag_col]] %in% shown_tags, !is.na(drift_observed),
           abs(drift_observed) <= 0.15) |>
    mutate(tag_fct = forcats::fct_reorder(
      factor(.data[[tag_col]]), drift_observed, median, na.rm = TRUE
    ))

  plots$drift_by_tag <- ggplot(tag_df,
                               aes(x = tag_fct, y = drift_observed * 100)) +
    geom_boxplot(outlier.size = 0.5, fill = "steelblue", alpha = 0.5) +
    coord_flip() +
    labs(
      title    = sprintf("Per-tag drift distribution (n = %d shown)", length(shown_tags)),
      x        = NULL,
      y        = "Observed drift (%)"
    ) +
    theme_bw()

  # -- window shift histogram ------------------------------------------------
  shift_df <- corrected_data |>
    filter(!is.na(true_window_s), !is.na(window_duration_s)) |>
    mutate(shift_s = true_window_s - window_duration_s)

  plots$window_shift <- ggplot(shift_df, aes(x = shift_s)) +
    geom_histogram(bins = 60, fill = "steelblue", colour = "white", alpha = 0.8) +
    geom_vline(xintercept = 0, linetype = "dashed", colour = "firebrick") +
    labs(
      title    = "Distribution of window-length corrections",
      subtitle = "Positive = window extended (clock was slow)",
      x        = "Correction (seconds)",
      y        = "Count"
    ) +
    theme_bw()

  plots
}


# =============================================================================
# Example usage
# =============================================================================
if (FALSE) {

  bats_corrected <- correct_time_drift(
    data                = bats,
    tag_col             = "tag_id",
    timestamp_col       = "timestamp",       # message receipt time
    start_col           = "start_time",      # programmed window start
    end_col             = "end_time",        # programmed window end
    temp_col            = "avg_temp",        # NULL to omit temperature term
    programmed_interval = 3 * 3600,          # 3-hour schedule; NULL to infer
    poly_degree         = 2L,
    max_drift_filter    = 0.10,
    min_obs_per_tag     = 5L
  )

  # Retrieve the fitted lme4 model for inspection
  drift_model <- attr(bats_corrected, "drift_model")
  summary(drift_model)

  # Diagnostic plots
  diag_plots <- plot_drift_diagnostics(bats_corrected,
                                       tag_col  = "tag_id",
                                       temp_col = "avg_temp")
  diag_plots$drift_vs_temp
  diag_plots$drift_by_tag
  diag_plots$window_shift
}
