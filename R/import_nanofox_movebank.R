#' Download and process bat tracking data from Movebank
#'
#' A high-level pipeline that downloads one or more Movebank studies containing
#' NanoFox, TinyFox, or uWasp tag data, computes movement metrics, converts
#' pressure to altitude, and produces three outputs per study: the full
#' multi-sensor object, a location-only object with speed/distance/bearing, and
#' a daily thinned dataset.
#'
#' @details
#'
#' \strong{Nanofox VEDBA Scaling Correction}
#'
#' As of August 2026, Nanofox VEDBA in Movebank export uses per-sample scaling
#' (÷504). The import pipeline auto-detects and corrects this to per-burst scaling
#' (÷18) on import via the `.correct_vedba_scaling()` function.
#'
#' Auto-detection: if median(vedba_sum) < 5 m/s², assumes per-sample scaling and
#' applies correction factor of 28 (= 504/18).
#'
#' After correction, vedba_sum represents per-burst values (~10-200 m/s² typical range).
#' Original per-sample values are preserved in the \code{vedba_sum_orig} column.
#' The \code{.vedba_scaling_note} metadata column tracks whether correction was applied.
#'
#' The same correction is also applied to raw sensor-level \code{vedba} values in the
#' full multi-sensor object, preserving originals in \code{vedba_orig} and recording
#' the status in \code{.vedba_sensor_scaling_note}. This makes \code{full$vedba}
#' directly comparable to Tinyfox per-burst VEDBA and to the corrected \code{vedba_sum}.
#'
#' \strong{Tinyfox V13P/V14P VEDBA Scaling Correction}
#'
#' Some Tinyfox V13P and V14P firmware batches report cumulative VeDBA values
#' that are scaled higher than V13. The pipeline detects this from the stationary
#' baseline (post-fall-off or activity = 0) and applies a data-driven correction
#' factor so that V13P/V14P per-burst VEDBA is on the same scale as V13.
#'
#' Detection: compare the stationary per-burst baseline of V13P/V14P against the
#' V13 baseline in the same study. If the ratio exceeds \code{threshold_ratio}
#' (default 2), the data-driven factor is applied.
#'
#' Columns added: \code{tinyfox_total_vedba_orig}, \code{tinyfox_diff_vedba_orig},
#' \code{tinyfox_vedba_rate_orig}, \code{.tinyfox_scaling_note}, and
#' \code{.tinyfox_scaling_factor}. Use \code{tinyfox_v13p_v14p_force_factor} to
#' override auto-detection.
#'
#' \strong{Flight Altitude Metrics}
#'
#' Climb rate (\code{climb_rate_ms}) is computed for every fix-to-fix interval
#' as \code{delta_altitude_m / dt_prev}. Each interval is also flagged
#' \code{is_flight} when \code{vedba_sum > flight_vedba_threshold} (default
#' 50 m/s\eqn{^2}, per-burst-corrected scale). \code{delta_altitude_flight_m}
#' and \code{climb_rate_flight_ms} mirror \code{delta_altitude_m} and
#' \code{climb_rate_ms} but are \code{NA} outside flight intervals.
#'
#' @param study_id Integer or character vector of Movebank study IDs.
#' @param tag_type Character or named list. When tag type cannot be inferred
#'   automatically from tag metadata, supply it here: a single string for all
#'   studies, or a named list (e.g. \code{list("12345" = "nanofox")}) for
#'   specific studies. Allowed values: \code{"uWasp"}, \code{"nanofox"},
#'   \code{"tinyfox"}.
#' @param sensor_external_ids Character vector of Movebank sensor external IDs
#'   to download. Default covers acceleration, accessory measurements,
#'   barometer, sigfox-geolocation, and derived sensors.
#' @param sensor_labels Character vector (same length as \code{sensor_external_ids})
#'   of human-readable labels used as \code{sensor_type} values in the output.
#' @param merge_studies Logical; if \code{TRUE} (default), stack all downloaded
#'   studies into single merged objects. If \code{FALSE}, return a list per study.
#' @param track_combine Character; strategy for merging track data across studies
#'   in \code{\link[move2]{mt_stack}}. Default \code{"merge"}.
#' @param run_elevation Logical; retrieve ground elevation via \pkg{elevatr}.
#'   Default \code{TRUE}. When enabled, the synthetic/marked "start" row
#'   inserted by \code{.add_start()} (the capture/release fix at
#'   \code{deploy_on_location}) has its \code{altitude_m} set to this ground
#'   elevation rather than a barometric reading, since bats are caught and
#'   released at ground level.
#' @param run_moonlit Logical; attach moonlight/twilight covariates via
#'   \code{\link{add_moonlit_to_move2}} (requires the \pkg{moonlit} package
#'   from GitHub). Default \code{TRUE}.
#' @param moonlit_e Numeric or \code{NULL}; atmospheric extinction coefficient
#'   passed to \code{add_moonlit_to_move2()}. Default \code{NULL}.
#' @param moonlit_e_from_altitude Logical; derive \code{moonlit_e} from ground
#'   elevation instead of using a fixed value. Default \code{FALSE}.
#' @param moonlit_altitude_col Character; elevation column used when
#'   \code{moonlit_e_from_altitude = TRUE}. Default \code{"elevation"}.
#' @param run_daily_metrics Logical; compute the daily thinned dataset. When
#'   \code{FALSE}, \code{daily} is \code{NULL}. Default \code{TRUE}.
#' @param daily_method Character; method for selecting the daily representative
#'   fix: \code{"solar_noon"} (default), \code{"daytime_only"}, or
#'   \code{"noon_roost"}.
#' @param compute_cum_dist Logical; compute cumulative distance per track.
#'   Default \code{TRUE}.
#' @param verbose Logical; print progress messages. Default \code{TRUE}.
#' @param tz Time zone string. Default \code{"UTC"}.
#' @param movebank_max_retries Integer; maximum Movebank download retries.
#'   Default \code{3}.
#' @param movebank_retry_wait Integer; initial wait between retries in seconds.
#'   Default \code{5}.
#' @param tinyfox_v13p_v14p_force_factor Numeric or \code{NULL}; when non-NULL,
#'   this scaling factor is applied to all Tinyfox V13P/V14P VEDBA values,
#'   bypassing the data-driven detection. Use this only when the dataset contains
#'   no V13 reference and you know the correct factor. Default \code{NULL}.
#' @param flight_vedba_threshold Numeric; \code{vedba_sum} (per-burst-corrected
#'   scale) above which a fix-to-fix interval is flagged as \code{is_flight}
#'   for the purposes of \code{delta_altitude_flight_m} and
#'   \code{climb_rate_flight_ms}. Default \code{50}.
#' @return A list with elements:
#' \describe{
#'   \item{\code{sensors_table}}{Full Movebank sensor type table.}
#'   \item{\code{sensors_selected}}{Subset of sensors actually requested.}
#'   \item{\code{studies}}{Per-study list (each with \code{full}, \code{location},
#'     \code{daily}).}
#'   \item{\code{full}}{Merged full multi-sensor move2 object (or list).}
#'   \item{\code{location}}{Merged location-only move2 object (or list).}
#'   \item{\code{daily}}{Merged daily move2 object (or list).}
#' }
#' @examples
#' \dontrun{
#'   out <- import_nanofox_movebank(study_id = 123456789)
#'   bats_loc   <- out$location
#'   bats_daily <- out$daily
#' }
#' @export
import_nanofox_movebank <- function(
  study_id,
  tag_type = NULL,
  sensor_external_ids = c(
    "acceleration",
    "accessory-measurements",
    "barometer",
    "sigfox-geolocation",
    "derived"
  ),
  sensor_labels = c(
    "VeDBA",
    "avg.temp",
    "min.baro.pressure",
    "location",
    "min.temp"
  ),
  merge_studies = TRUE,
  track_combine = "merge",
  run_elevation = TRUE,
  run_moonlit = TRUE,
  moonlit_e = NULL,
  moonlit_e_from_altitude = FALSE,
  moonlit_altitude_col = "elevation",
  run_daily_metrics = TRUE,
  daily_method = c("solar_noon", "daytime_only", "noon_roost"),
  compute_cum_dist = TRUE,
  verbose = TRUE,
  tz = "UTC",
  movebank_max_retries = 3,
  movebank_retry_wait = 5,
  tinyfox_v13p_v14p_force_factor = NULL,
  flight_vedba_threshold = 50
) {
  if (isTRUE(run_elevation) && !requireNamespace("elevatr", quietly = TRUE)) {
    warning("run_elevation=TRUE but package 'elevatr' is not installed; ",
            "ground elevation will be skipped.", call. = FALSE)
  }
  if (isTRUE(run_moonlit) && !requireNamespace("moonlit", quietly = TRUE)) {
    warning(
      "run_moonlit=TRUE but package 'moonlit' is not installed; ",
      "moonlight/twilight covariates will be skipped. Install with ",
      "devtools::install_github('msmielak/moonlit').",
      call. = FALSE
    )
  }

  old_opts <- options(SigfoxTagPrep.verbose = verbose)
  on.exit(options(old_opts), add = TRUE)
  daily_method <- match.arg(daily_method)

  # ---------------------------------------------------------------------------
  # Input checks
  # ---------------------------------------------------------------------------
  if (length(study_id) < 1) {
    stop("Provide at least one study_id.")
  }
  if (length(sensor_external_ids) != length(sensor_labels)) {
    stop("sensor_external_ids and sensor_labels must have the same length.")
  }

  # ---------------------------------------------------------------------------
  # Sensor lookup
  # ---------------------------------------------------------------------------
  sensors_tbl <- move2::movebank_retrieve(entity_type = "tag_type") %>%
    as_tibble()
  sensor_selected <- sensors_tbl %>%
    dplyr::filter(.data$external_id %in% sensor_external_ids) %>%
    mutate(
      sensor_type = sensor_labels[match(.data$external_id, sensor_external_ids)]
    )
  if (nrow(sensor_selected) == 0) {
    stop(
      "None of the requested sensor_external_ids found in movebank_retrieve(tag_type)."
    )
  }
  if (verbose) {
    .msg("Selected sensors:")
    print(
      sensor_selected %>%
        dplyr::select(id, name, external_id, sensor_type, is_location_sensor),
      n = 50
    )
  }

  # ---------------------------------------------------------------------------
  # Per-study download + processing
  # ---------------------------------------------------------------------------
  res_list <- lapply(
    study_id,
    .download_one_study,
    sensor_selected = sensor_selected,
    tag_type = tag_type,
    movebank_max_retries = movebank_max_retries,
    movebank_retry_wait = movebank_retry_wait,
    location_metric_args = list(
      compute_cum_dist = compute_cum_dist,
      run_elevation = run_elevation,
      run_moonlit = run_moonlit,
      moonlit_e = moonlit_e,
      moonlit_e_from_altitude = moonlit_e_from_altitude,
      moonlit_altitude_col = moonlit_altitude_col
    ),
    run_daily_metrics = run_daily_metrics,
    daily_method = daily_method,
    tz = tz,
    tinyfox_v13p_v14p_force_factor = tinyfox_v13p_v14p_force_factor,
    flight_vedba_threshold = flight_vedba_threshold
  )
  names(res_list) <- as.character(study_id)

  full_merged <- if (isTRUE(merge_studies)) {
    .merge_stack(track_combine = track_combine, lapply(res_list, `[[`, "full"))
  } else {
    lapply(res_list, `[[`, "full")
  }
  loc_merged <- if (isTRUE(merge_studies)) {
    .merge_stack(track_combine = track_combine, lapply(res_list, `[[`, "location"))
  } else {
    lapply(res_list, `[[`, "location")
  }
  daily_merged <- if (isTRUE(merge_studies)) {
    .merge_stack(track_combine = track_combine, lapply(res_list, `[[`, "daily"))
  } else {
    lapply(res_list, `[[`, "daily")
  }

  # ---------------------------------------------------------------------------
  # Post-merge: drop all-NA columns from merged outputs
  # ---------------------------------------------------------------------------
  if (isTRUE(merge_studies)) {
    if (!is.null(full_merged) && nrow(full_merged) > 0) {
      full_merged <- .drop_all_na_cols(full_merged)
    }
    if (!is.null(loc_merged) && nrow(loc_merged) > 0) {
      loc_merged <- .drop_all_na_cols(loc_merged)
    }
    if (!is.null(daily_merged) && nrow(daily_merged) > 0) {
      daily_merged <- .drop_all_na_cols(daily_merged)
    }
  }

  if (verbose && isTRUE(merge_studies)) {
    .msg("Merged studies: ", paste(study_id, collapse = ", "))
    .msg("Sensor types (merged full):")
    print(table(full_merged$sensor_type, useNA = "ifany"))
    .msg("Timestamp range (merged full):")
    print(summary(full_merged$timestamp))
    # Note: units are stripped from merged objects by .strip_units_cols() before
    # mt_stack(), so deparse_unit() would error here — report class instead.
    if (!is.null(loc_merged)) {
      if ("speed" %in% names(loc_merged)) {
        .msg("Speed column class (location): ", class(loc_merged$speed)[1])
      }
      if ("delta_altitude_m" %in% names(loc_merged)) {
        .msg(
          "Delta altitude column class (location): ",
          class(loc_merged$delta_altitude_m)[1]
        )
      }
      if ("cum_dist_km" %in% names(loc_merged)) {
        .msg("Cumulative distance column present (location): yes")
      }
    }
    .summarize_flight_altitude(full_merged, "full")
    .summarize_flight_altitude(loc_merged, "location")
    .summarize_flight_altitude(daily_merged, "daily")
  } else if (verbose) {
    .msg("Downloaded ", length(study_id), " studies (not merged).")
  }

  list(
    sensors_table = sensors_tbl,
    sensors_selected = sensor_selected,
    studies = res_list,
    full = full_merged,
    location = loc_merged,
    daily = daily_merged
  )
}
