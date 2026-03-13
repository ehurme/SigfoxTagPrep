# Detect continuous nocturnal flight bouts on migratory nights ----
# 2026-03-12
# Edward Hurme

detect_continuous_night_flights <- function(
    loc_data,
    daily_data = NULL,

    id_col = "individual_local_identifier",
    time_col = "timestamp",
    tag_type_col = "tag_type",

    # columns in daily_data or loc_data indicating migratory night
    migration_col = "migration_night",

    # optional night identifiers
    night_id_col = NULL,          # e.g. "night_id" if already present
    date_col_daily = NULL,        # e.g. "date" in daily_data

    # optional nighttime filtering
    nighttime_col = NULL,         # logical column already indicating night
    sunset_col = NULL,            # optional
    sunrise_col = NULL,           # optional

    # tag-type expected intervals in minutes
    expected_intervals_min = c(
      nanofox = 180,
      uWasp   = 60,
      tinyfox = 30
    ),

    # gap tolerance as multiplier on expected interval
    # e.g. 1.5 means a 60-min tag allows gaps up to 90 min before splitting
    tolerance_mult = 1.5,

    # minimum number of fixes required in a bout
    min_fixes = 3,

    # minimum duration (hours) required in a bout
    min_duration_h = 1,

    # if TRUE, keep only the longest continuous bout per night
    keep_longest_bout_per_night = FALSE,

    tz = "UTC"
) {

  suppressPackageStartupMessages({
    library(dplyr)
    library(lubridate)
    library(rlang)
    library(tidyr)
    library(purrr)
  })

  # ---------------------------
  # helpers
  # ---------------------------
  id_sym   <- sym(id_col)
  time_sym <- sym(time_col)
  tag_sym  <- sym(tag_type_col)
  mig_sym  <- sym(migration_col)

  if (!inherits(loc_data[[time_col]], c("POSIXct", "POSIXt"))) {
    loc_data[[time_col]] <- as.POSIXct(loc_data[[time_col]], tz = tz)
  }

  if (!is.null(daily_data) && !inherits(daily_data[[time_col]], c("POSIXct", "POSIXt")) &&
      time_col %in% names(daily_data)) {
    daily_data[[time_col]] <- as.POSIXct(daily_data[[time_col]], tz = tz)
  }

  # Standardize tag types for lookup
  standardize_tag_type <- function(x) {
    x <- as.character(x)
    x_low <- tolower(x)

    dplyr::case_when(
      x_low %in% c("nanofox", "nano_fox", "nano fox") ~ "nanofox",
      x_low %in% c("uwasp", "uwasp", "u_wasp") ~ "uWasp",
      x_low %in% c("tinyfox", "tiny_fox", "tiny fox") ~ "tinyfox",
      TRUE ~ x
    )
  }

  lookup_expected_interval <- function(x) {
    key <- standardize_tag_type(x)
    out <- unname(expected_intervals_min[key])
    as.numeric(out)
  }

  # Define night date if not already provided:
  # points after noon belong to the upcoming night, otherwise previous night
  assign_night_date <- function(ts) {
    ts <- as.POSIXct(ts, tz = tz)
    as.Date(ifelse(hour(ts) >= 12, as.character(ts), as.character(ts - days(1))), tz = tz)
  }

  # Determine whether a point is at night
  calc_is_night <- function(df) {
    if (!is.null(nighttime_col) && nighttime_col %in% names(df)) {
      return(as.logical(df[[nighttime_col]]))
    }

    if (!is.null(sunset_col) && !is.null(sunrise_col) &&
        sunset_col %in% names(df) && sunrise_col %in% names(df)) {

      sunset_time  <- as.POSIXct(df[[sunset_col]], tz = tz)
      sunrise_time <- as.POSIXct(df[[sunrise_col]], tz = tz)
      ts           <- as.POSIXct(df[[time_col]], tz = tz)

      # assumes sunset and sunrise are already matched to the same biological night
      return(ts >= sunset_time & ts <= sunrise_time)
    }

    # fallback: keep all points and let migratory-night filtering do the work
    return(rep(TRUE, nrow(df)))
  }

  # ---------------------------
  # prepare location data
  # ---------------------------
  loc <- loc_data %>%
    mutate(
      .id = !!id_sym,
      .timestamp = as.POSIXct(!!time_sym, tz = tz),
      .tag_type_raw = !!tag_sym,
      .tag_type_std = standardize_tag_type(.tag_type_raw),
      .expected_interval_min = lookup_expected_interval(.tag_type_raw)
    )

  if (all(is.na(loc$.expected_interval_min))) {
    stop("No tag types matched expected_intervals_min. Check tag_type values.")
  }

  if (is.null(night_id_col)) {
    loc <- loc %>%
      mutate(.night_date = assign_night_date(.timestamp))
  } else {
    loc <- loc %>%
      mutate(.night_date = .data[[night_id_col]])
  }

  loc$.is_night <- calc_is_night(loc)

  # ---------------------------
  # join migratory night info
  # ---------------------------
  if (!is.null(daily_data)) {
    dd <- daily_data

    if (!migration_col %in% names(dd)) {
      stop("migration_col not found in daily_data.")
    }

    if (is.null(night_id_col)) {
      if (!is.null(date_col_daily) && date_col_daily %in% names(dd)) {
        dd <- dd %>%
          mutate(.night_date = as.Date(.data[[date_col_daily]]))
      } else if (time_col %in% names(dd)) {
        dd <- dd %>%
          mutate(.night_date = assign_night_date(.data[[time_col]]))
      } else {
        stop("Provide night_id_col, date_col_daily, or a timestamp column in daily_data.")
      }
    } else {
      dd <- dd %>%
        mutate(.night_date = .data[[night_id_col]])
    }

    dd <- dd %>%
      mutate(.id = .data[[id_col]]) %>%
      select(.id, .night_date, .migration_night = all_of(migration_col)) %>%
      distinct()

    loc <- loc %>%
      left_join(dd, by = c(".id", ".night_date"))
  } else {
    if (!migration_col %in% names(loc)) {
      stop("Either supply daily_data with migration_col, or loc_data must contain migration_col.")
    }
    loc <- loc %>%
      mutate(.migration_night = !!mig_sym)
  }

  # ---------------------------
  # restrict to migratory nights and nighttime fixes
  # ---------------------------
  x <- loc %>%
    filter(!is.na(.migration_night), .migration_night == 1, .is_night) %>%
    arrange(.id, .night_date, .timestamp)

  if (nrow(x) == 0) {
    return(list(
      bouts = tibble(),
      points = tibble(),
      night_summary = tibble()
    ))
  }

  # ---------------------------
  # split into continuous bouts
  # ---------------------------
  x <- x %>%
    group_by(.id, .night_date) %>%
    arrange(.timestamp, .by_group = TRUE) %>%
    mutate(
      .dt_prev_min = as.numeric(difftime(.timestamp, lag(.timestamp), units = "mins")),
      .expected_prev_min = lag(.expected_interval_min),
      .gap_limit_min = .expected_prev_min * tolerance_mult,
      .new_bout = case_when(
        row_number() == 1 ~ 1L,
        is.na(.dt_prev_min) ~ 1L,
        is.na(.gap_limit_min) ~ 1L,
        .dt_prev_min > .gap_limit_min ~ 1L,
        TRUE ~ 0L
      ),
      .bout_id = cumsum(.new_bout)
    ) %>%
    ungroup()

  # ---------------------------
  # summarize bouts
  # ---------------------------
  bout_summary <- x %>%
    group_by(.id, .night_date, .bout_id) %>%
    summarise(
      tag_type = first(.tag_type_std),
      tag_type_raw = first(.tag_type_raw),
      expected_interval_min = median(.expected_interval_min, na.rm = TRUE),
      start_time = min(.timestamp, na.rm = TRUE),
      end_time = max(.timestamp, na.rm = TRUE),
      n_fixes = n(),
      duration_h = as.numeric(difftime(end_time, start_time, units = "hours")),

      n_gaps = sum(!is.na(.dt_prev_min)),
      median_gap_min = median(.dt_prev_min, na.rm = TRUE),
      mean_gap_min = mean(.dt_prev_min, na.rm = TRUE),
      max_gap_min = max(.dt_prev_min, na.rm = TRUE),

      prop_on_time_gaps = mean(
        .dt_prev_min <= (.expected_prev_min * tolerance_mult),
        na.rm = TRUE
      ),

      # estimated missed transmissions across the bout
      missed_intervals = sum(
        pmax(round(.dt_prev_min / .expected_prev_min) - 1, 0),
        na.rm = TRUE
      ),

      # expected number of fixes between first and last point for this bout
      expected_fixes_in_bout = floor(
        as.numeric(difftime(end_time, start_time, units = "mins")) /
          expected_interval_min
      ) + 1,

      completeness = n_fixes / expected_fixes_in_bout,
      .groups = "drop"
    ) %>%
    mutate(
      is_continuous = n_fixes >= min_fixes & duration_h >= min_duration_h
    ) %>%
    filter(is_continuous)

  if (nrow(bout_summary) == 0) {
    return(list(
      bouts = tibble(),
      points = tibble(),
      night_summary = tibble()
    ))
  }

  if (keep_longest_bout_per_night) {
    bout_summary <- bout_summary %>%
      group_by(.id, .night_date) %>%
      slice_max(order_by = duration_h, n = 1, with_ties = FALSE) %>%
      ungroup()
  }

  # Keep only points belonging to retained bouts
  bout_key <- bout_summary %>%
    select(.id, .night_date, .bout_id)

  bout_points <- x %>%
    inner_join(bout_key, by = c(".id", ".night_date", ".bout_id")) %>%
    group_by(.id, .night_date, .bout_id) %>%
    arrange(.timestamp, .by_group = TRUE) %>%
    mutate(
      time_since_bout_start_min = as.numeric(difftime(.timestamp, first(.timestamp), units = "mins")),
      point_index = row_number()
    ) %>%
    ungroup()

  # Night-level summary
  night_summary <- bout_summary %>%
    group_by(.id, .night_date) %>%
    summarise(
      tag_type = first(tag_type),
      n_bouts = n(),
      total_bout_duration_h = sum(duration_h, na.rm = TRUE),
      total_fixes = sum(n_fixes, na.rm = TRUE),
      mean_completeness = mean(completeness, na.rm = TRUE),
      best_completeness = max(completeness, na.rm = TRUE),
      .groups = "drop"
    )

  # Clean output names
  bouts <- bout_summary %>%
    rename(
      individual_local_identifier = .id,
      night_date = .night_date,
      bout_id = .bout_id
    )

  points <- bout_points %>%
    rename(
      individual_local_identifier = .id,
      night_date = .night_date,
      bout_id = .bout_id,
      timestamp = .timestamp,
      tag_type = .tag_type_std,
      dt_prev_min = .dt_prev_min
    )

  night_summary <- night_summary %>%
    rename(
      individual_local_identifier = .id,
      night_date = .night_date
    )

  list(
    bouts = bouts,
    points = points,
    night_summary = night_summary
  )
}

# load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")
# bats_loc <- bats_loc %>% filter(species == "Nyctalus leisleri")
# load("../../../Dropbox/MPI/Leisleri/Data/rdata/Daily_Leisler_Env.robj")
# res <- detect_continuous_night_flights(
#   loc_data = bats_loc,
#   daily_data = b_daily_wind,
#   id_col = "individual_local_identifier",
#   time_col = "timestamp",
#   tag_type_col = "tag_type",
#   migration_col = "migration_night",
#   nighttime_col = "night",
#   date_col_daily = "date",
#   min_fixes = 3,
#   min_duration_h = 2,
#   tolerance_mult = 1.5
# )
