# identify migration bouts

mt_add_migration_bouts <- function(
    x,
    mig_col = "migration_night",   # logical/0-1 per fix; aggregated to day
    time_col = "timestamp",
    tz = "UTC",
    daily_agg = c("any", "all", "mean"),  # how to aggregate fixes within a day
    mean_threshold = 0.5,                 # used if daily_agg == "mean"
    gap_break = TRUE,                     # if TRUE: missing dates break bouts
    prefix = "mig"
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(lubridate)
    library(move2)
    library(tidyr)
  })

  daily_agg <- match.arg(daily_agg)

  if (!inherits(x, "move2")) stop("x must be a <move2> object.")
  if (!time_col %in% names(x)) stop("time_col not found in x.")
  if (!mig_col  %in% names(x)) stop("mig_col not found in x.")

  # Track IDs come from move2
  id <- move2::mt_track_id(x)
  if (anyNA(id)) stop("Track id(s) contain NA; cannot compute bouts reliably.")

  df <- tibble(
    ..row = seq_len(nrow(x)),
    ..id  = as.character(id),
    ..t   = x[[time_col]],
    ..mig = x[[mig_col]]
  )

  # Coerce migration column to numeric 0/1 where possible
  if (is.logical(df$..mig)) {
    df$..mig_num <- as.integer(df$..mig)
  } else if (is.numeric(df$..mig)) {
    df$..mig_num <- df$..mig
  } else if (is.factor(df$..mig) || is.character(df$..mig)) {
    # attempt common encodings: "0/1", "TRUE/FALSE"
    tmp <- tolower(as.character(df$..mig))
    df$..mig_num <- dplyr::case_when(
      tmp %in% c("true", "t", "yes", "y")  ~ 1,
      tmp %in% c("false", "f", "no", "n")  ~ 0,
      suppressWarnings(!is.na(as.numeric(tmp))) ~ as.numeric(tmp),
      TRUE ~ NA_real_
    )
  } else {
    stop("mig_col must be logical, numeric, or coercible character/factor.")
  }

  # Build per-day indicator
  df_day <- df %>%
    mutate(..date = as.Date(with_tz(..t, tz = tz))) %>%
    group_by(..id, ..date) %>%
    summarise(
      ..mig_day = dplyr::case_when(
        daily_agg == "any"  ~ as.integer(any(..mig_num == 1, na.rm = TRUE)),
        daily_agg == "all"  ~ as.integer(all(..mig_num == 1, na.rm = TRUE) && any(!is.na(..mig_num))),
        daily_agg == "mean" ~ as.integer(mean(..mig_num, na.rm = TRUE) >= mean_threshold),
        TRUE ~ NA_integer_
      ),
      .groups = "drop"
    ) %>%
    mutate(..mig_day = ifelse(is.na(..mig_day), 0L, ..mig_day))

  # Optionally "complete" missing days so that gaps break consecutiveness explicitly
  if (gap_break) {
    df_day <- df_day %>%
      group_by(..id) %>%
      complete(..date = seq(min(..date), max(..date), by = "1 day"),
               fill = list(..mig_day = 0L)) %>%
      ungroup()
  }

  # Compute bouts and consecutive day index within bout
  # A bout = consecutive days where ..mig_day == 1
  df_bouts <- df_day %>%
    arrange(..id, ..date) %>%
    group_by(..id) %>%
    mutate(
      ..is_mig = ..mig_day == 1L,
      ..bout_change = ..is_mig != lag(..is_mig, default = first(..is_mig)),
      ..run_id = cumsum(..bout_change)
    ) %>%
    group_by(..id, ..run_id) %>%
    mutate(
      ..bout_id = ifelse(first(..is_mig), dense_rank(interaction(..id, ..run_id)), NA_integer_),
      ..day_in_bout = ifelse(first(..is_mig), row_number(), 0L),
      ..bout_len = ifelse(first(..is_mig), n(), 0L)
    ) %>%
    ungroup() %>%
    dplyr::select(..id, ..date, ..mig_day, ..bout_id, ..day_in_bout, ..bout_len)

  # Add a "which migration bout is this?" (1st, 2nd, 3rd...) per individual
  df_bouts <- df_bouts %>%
    group_by(..id) %>%
    mutate(
      ..bout_order = ifelse(
        ..bout_id %in% NA_integer_, NA_integer_,
        match(..bout_id, unique(..bout_id[..bout_id > 0]))
      )
    ) %>%
    ungroup()

  # Join back onto each fix by id + date
  df_fix <- df %>%
    mutate(..date = as.Date(with_tz(..t, tz = tz))) %>%
    left_join(df_bouts, by = c("..id", "..date"))

  # Add columns to move2 object
  x[[paste0(prefix, "_day")]]         <- df_fix$..mig_day
  x[[paste0(prefix, "_bout_id")]]     <- df_fix$..bout_id
  x[[paste0(prefix, "_bout_order")]]  <- df_fix$..bout_order
  x[[paste0(prefix, "_day_in_bout")]] <- df_fix$..day_in_bout
  x[[paste0(prefix, "_bout_len")]]    <- df_fix$..bout_len
  x[[paste0(prefix, "_is_first_bout")]] <- ifelse(df_fix$..bout_order == 1L, TRUE, FALSE)

  x
}

mt_add_migration_bout_n <- function(
    x,
    prefix = "mig",
    out_col = paste0(prefix, "_n_bouts")
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(move2)
  })

  if (!inherits(x, "move2")) stop("x must be a <move2> object.")

  bout_order_col <- paste0(prefix, "_bout_order")
  if (!bout_order_col %in% names(x)) {
    stop("Missing ", bout_order_col, ". Run mt_add_migration_bouts() first.")
  }

  ids <- as.character(move2::mt_track_id(x))
  if (anyNA(ids)) stop("Track id(s) contain NA; cannot summarise bouts reliably.")

  df <- tibble(
    ..row = seq_len(nrow(x)),
    ..id  = ids,
    ..bout_order = x[[bout_order_col]]
  )

  # number of bouts per bat = max bout order (same as count distinct orders)
  bout_n <- df %>%
    group_by(..id) %>%
    summarise(..n_bouts = suppressWarnings(max(..bout_order, na.rm = TRUE)), .groups = "drop") %>%
    mutate(..n_bouts = ifelse(is.infinite(..n_bouts) | is.na(..n_bouts), 0L, as.integer(..n_bouts)))

  df <- df %>% left_join(bout_n, by = "..id")

  x[[out_col]] <- df$..n_bouts
  x
}



