library(dplyr)
library(lubridate)

#' Evaluate tag deployment quality
#'
#' Checks for suspicious deploy timestamps, implausible durations,
#' sparse transmissions (likely failed tags), and missing capture sheet fields.
#'
#' @param track_data  sf/move2 deployment metadata with at least:
#'   `individual_local_identifier`, `deploy_on_timestamp`, and any
#'   capture-sheet columns you want to audit.
#' @param bats_full   move2/data.frame of all fixes, with columns
#'   `individual_local_identifier`, `timestamp`, `tag_type`, `comments`.
#' @param tag_type    Character. Tag type to dplyr::filter on (default "nanofox").
#' @param max_duration_days  Numeric. Maximum plausible battery life in days
#'   (default 60 = ~2 months).
#' @param min_transmissions  Integer. Minimum messages to consider a deployment
#'   "successful" (default 5).
#' @param capture_sheet_cols  Character vector. Column names from `track_data`
#'   to audit for missingness. Defaults to a sensible core set.
#'
#' @return A named list with:
#'   - `$summary`          : one row per tag, all flags combined
#'   - `$timestamp_issues` : tags where deploy_on_timestamp has non-zero seconds
#'   - `$duration_issues`  : tags with implausible attachment duration
#'   - `$sparse_tags`      : tags with fewer than `min_transmissions` messages
#'   - `$missing_fields`   : tags × columns matrix of missingness
#'   - `$complete_tags`    : character vector of IDs passing all checks

evaluate_tag_deployments <- function(
    track_data,
    bats_full,
    tag_type             = "nanofox",
    max_duration_days    = 60,
    min_transmissions    = 5,
    capture_sheet_cols   = c(
      "deploy_on_timestamp", "animal_mass", "sex",
      "age", "capture_location", "individual_local_identifier"
    )
) {

  # ── 0. Normalise inputs ────────────────────────────────────────────────────
  track_df <- track_data %>% as.data.frame()
  bats_df  <- bats_full  %>% as.data.frame() %>%
    dplyr::filter(tag_type == !!tag_type)

  ids <- unique(bats_df$individual_local_identifier)

  # ── 1. Deployment timestamp check ─────────────────────────────────────────
  # Correctly entered timestamps are rounded to the minute; non-zero seconds
  # suggest the field was populated from the first GPS fix instead.
  ts_check <- track_df %>%
    dplyr::filter(individual_local_identifier %in% ids) %>%
    transmute(
      individual_local_identifier,
      deploy_on_timestamp,
      ts_seconds      = second(deploy_on_timestamp),
      flag_ts_seconds = ts_seconds != 0
    )

  # ── 2. Duration check ──────────────────────────────────────────────────────
  # Build start/end from the movement data; "start" comment marks tag-on.
  duration_check <- bats_df %>%
    group_by(individual_local_identifier) %>%
    reframe(
      n_fixes         = n(),
      start           = if (any(comments == "start", na.rm = TRUE))
        timestamp[which(comments == "start")[1]]
      else NA_POSIXct_,
      end             = max(timestamp, na.rm = TRUE),
      duration_days   = as.numeric(difftime(end, start, units = "days")),
      flag_no_start   = is.na(start),
      flag_long_dur   = !is.na(start) & duration_days > max_duration_days
    )

  # ── 3. Sparse-transmission check ──────────────────────────────────────────
  sparse_check <- duration_check %>%
    transmute(
      individual_local_identifier,
      n_fixes,
      flag_sparse = n_fixes < min_transmissions
    )

  # ── 4. Missing capture-sheet fields ───────────────────────────────────────
  # Restrict to columns that actually exist in the data
  audit_cols <- intersect(capture_sheet_cols, names(track_df))

  missing_check <- track_df %>%
    dplyr::filter(individual_local_identifier %in% ids) %>%
    dplyr::select(individual_local_identifier, all_of(audit_cols)) %>%
    mutate(across(
      all_of(audit_cols),
      ~ is.na(.) | (is.character(.) & trimws(.) == ""),
      .names = "miss_{.col}"
    )) %>%
    mutate(
      n_missing_fields = rowSums(across(starts_with("miss_"))),
      flag_missing     = n_missing_fields > 0
    )

  # ── 5. Combined summary ────────────────────────────────────────────────────
  summary_tbl <- ts_check %>%
    dplyr::select(individual_local_identifier, deploy_on_timestamp,
           ts_seconds, flag_ts_seconds) %>%
    left_join(duration_check, by = "individual_local_identifier") %>%
    left_join(sparse_check,   by = "individual_local_identifier") %>%
    left_join(
      missing_check %>%
        dplyr::select(individual_local_identifier, n_missing_fields, flag_missing),
      by = "individual_local_identifier"
    ) %>%
    mutate(
      any_flag = flag_ts_seconds | flag_no_start |
        flag_long_dur   | flag_sparse   | flag_missing
    ) %>%
    arrange(desc(any_flag), individual_local_identifier)

  # ── 6. Complete tags (pass all checks) ────────────────────────────────────
  complete_ids <- summary_tbl %>%
    dplyr::filter(!any_flag) %>%
    pull(individual_local_identifier)

  # ── 7. Console report ─────────────────────────────────────────────────────
  .report <- function() {
    n_total    <- nrow(summary_tbl)
    n_complete <- length(complete_ids)

    cli_or_message <- function(...) message(sprintf(...))

    cli_or_message("── Tag deployment QA (%s) ──────────────────────────", tag_type)
    cli_or_message("  Total deployments audited : %d", n_total)
    cli_or_message("  ✓ Passing all checks      : %d (%.0f%%)",
                   n_complete, 100 * n_complete / n_total)
    cli_or_message("")
    cli_or_message("  ✗ Timestamp has seconds   : %d  (likely from 1st GPS fix)",
                   sum(summary_tbl$flag_ts_seconds,  na.rm = TRUE))
    cli_or_message("  ✗ No 'start' comment      : %d",
                   sum(summary_tbl$flag_no_start,    na.rm = TRUE))
    cli_or_message("  ✗ Duration > %d days       : %d",
                   max_duration_days,
                   sum(summary_tbl$flag_long_dur,    na.rm = TRUE))
    cli_or_message("  ✗ Sparse (< %d fixes)      : %d",
                   min_transmissions,
                   sum(summary_tbl$flag_sparse,      na.rm = TRUE))
    cli_or_message("  ✗ Missing capture fields  : %d",
                   sum(summary_tbl$flag_missing,     na.rm = TRUE))
  }

  .report()

  # ── 8. Return ──────────────────────────────────────────────────────────────
  list(
    summary          = summary_tbl,
    timestamp_issues = dplyr::filter(summary_tbl, flag_ts_seconds),
    duration_issues  = dplyr::filter(summary_tbl, flag_long_dur | flag_no_start),
    sparse_tags      = dplyr::filter(summary_tbl, flag_sparse),
    missing_fields   = missing_check,
    complete_tags    = complete_ids
  )
}

qa <- evaluate_tag_deployments(
  track_data          = track_data,
  bats_full           = bats_full,
  tag_type            = "nanofox",
  max_duration_days   = 60,
  min_transmissions   = 2,
  capture_sheet_cols  = c(
    "deploy_on_timestamp", "animal_mass", "sex",
    "age", "capture_location", "individual_local_identifier"
  )
)



# Inspect flagged deployments
qa$summary %>% filter(any_flag) %>% View()

# Tags safe to use downstream
qa$complete_tags

# Duration histogram for complete tags only
qa$summary %>%
  filter(!flag_long_dur, !flag_no_start) %>%
  pull(duration_days) %>%
  hist(breaks = 30, main = "Deployment duration (days)", xlab = "days")