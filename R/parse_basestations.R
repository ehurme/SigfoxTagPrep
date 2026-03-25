# =============================================================================
# parse_basestations.R
#
# Parses Sigfox base station strings into structured summaries.
#
# Supported input formats (auto-detected per cell):
#
#   Format A — JSON-like array:
#     [{ID:37AE,N:2,RSSI:-140},{ID:6353,N:1,RSSI:-139}]
#     Fields: ID (hex), N (frames received), RSSI (dBm)
#
#   Format B — pipe- or space-delimited:
#     28D1, -135, 1 | 28C8, -137, 1
#     277A, -127, 1
#     Fields: ID, RSSI, N   (note: different order from Format A)
#
# Each cell may contain multiple fixes separated by spaces (Format A)
# or be a single fix. The functions handle both.
#
# Main entry points:
#   parse_basestation_string(x)   — parse a single raw string, return a list
#   parse_basestations(x)         — vectorised over a column, return a data frame
#   add_basestation_cols(df, col) — attach summary columns to an existing data frame
# =============================================================================


# -----------------------------------------------------------------------------
# Internal: parse one station token into a named list(id, rssi, n)
# Returns NULL if the token cannot be parsed.
# -----------------------------------------------------------------------------
.parse_station_token <- function(token) {
  token <- trimws(token)
  if (nchar(token) == 0) return(NULL)

  # --- Format A: ID:37AE,N:2,RSSI:-140 (braces already stripped by .split_to_tokens) ----
  # Detect by presence of "ID:" key rather than "{" which is stripped upstream.
  if (grepl("(?i)ID:[0-9A-Fa-f]", token, perl = TRUE)) {
    id_m   <- regmatches(token, regexpr("(?i)ID:([0-9A-Fa-f]+)", token, perl = TRUE))
    rssi_m <- regmatches(token, regexpr("(?i)RSSI:\\s*(-?[0-9]+)", token, perl = TRUE))
    n_m    <- regmatches(token, regexpr("(?i)\\bN:\\s*([0-9]+)", token, perl = TRUE))

    id   <- if (length(id_m))   sub("(?i)ID:",   "", id_m,   perl = TRUE) else NA_character_
    rssi <- if (length(rssi_m)) as.integer(sub("(?i)RSSI:", "", rssi_m, perl = TRUE)) else NA_integer_
    n    <- if (length(n_m))    as.integer(sub("(?i)\\bN:", "", n_m,   perl = TRUE)) else NA_integer_

    if (is.na(id)) return(NULL)
    return(list(id = toupper(id), rssi = rssi, n = n))
  }

  # --- Format B: 28D1, -135, 1  (ID, RSSI, N) --------------------------------
  parts <- strsplit(token, ",")[[1]]
  parts <- trimws(parts)
  parts <- parts[nchar(parts) > 0]

  if (length(parts) >= 2) {
    id   <- toupper(parts[1])
    rssi <- suppressWarnings(as.integer(parts[2]))
    n    <- if (length(parts) >= 3) suppressWarnings(as.integer(parts[3])) else NA_integer_

    # Validate: ID should look like a hex string, rssi should be negative
    if (!grepl("^[0-9A-Fa-f]+$", id)) return(NULL)
    if (is.na(rssi)) return(NULL)

    return(list(id = id, rssi = rssi, n = n))
  }

  NULL
}


# -----------------------------------------------------------------------------
# Internal: split a raw string into individual station tokens
#
# A raw string may contain:
#   - Multiple Format-A blocks: "[{...}] [{...}]"
#   - Multiple Format-B entries separated by " | "
#   - A single entry of either format
# -----------------------------------------------------------------------------
.split_to_tokens <- function(raw) {
  raw <- trimws(raw)
  if (is.na(raw) || nchar(raw) == 0) return(character(0))

  # Format A: extract everything inside [...] brackets, split on },{ boundary
  if (grepl("[", raw, fixed = TRUE)) {
    blocks <- regmatches(raw, gregexpr("\\[[^\\]]*\\]", raw, perl = TRUE))[[1]]
    tokens <- character(0)
    for (blk in blocks) {
      inner <- gsub("^\\[|\\]$", "", blk, perl = TRUE)  # strip outer [ ]
      inner <- gsub("^\\{|\\}$", "", inner, perl = TRUE) # strip outer { }
      toks  <- strsplit(inner, "\\},\\s*\\{", perl = TRUE)[[1]]
      tokens <- c(tokens, trimws(toks))
    }
    return(tokens)
  }

  # Format B: split on pipe separator
  if (grepl("|", raw, fixed = TRUE)) {
    return(strsplit(raw, "\\s*\\|\\s*", perl = TRUE)[[1]])
  }

  # Single Format-B entry
  return(raw)
}


# -----------------------------------------------------------------------------
# parse_basestation_string(x)
#
# Parse a single raw basestation string.
#
# Returns a list with:
#   $raw          character  — original string, preserved verbatim
#   $canonical    character  — normalised representation: "ID:RSSI:N|ID:RSSI:N|..."
#                              stations sorted by RSSI descending (best first)
#   $n_stations   integer    — total number of stations detected
#   $best_rssi    integer    — highest (least negative) RSSI value
#   $best_id      character  — ID of the station with best RSSI
#   $mean_rssi    numeric    — mean RSSI across all stations
#   $total_frames integer    — sum of N (frames received) across all stations
#   $stations     data.frame — one row per station: id, rssi, n
# -----------------------------------------------------------------------------
parse_basestation_string <- function(x) {
  empty <- list(
    raw          = x,
    canonical    = NA_character_,
    n_stations   = 0L,
    best_rssi    = NA_integer_,
    best_id      = NA_character_,
    mean_rssi    = NA_real_,
    total_frames = NA_integer_,
    stations     = data.frame(id = character(), rssi = integer(), n = integer(),
                              stringsAsFactors = FALSE)
  )

  if (is.na(x) || trimws(x) == "") return(empty)

  tokens   <- .split_to_tokens(x)
  stations <- Filter(Negate(is.null), lapply(tokens, .parse_station_token))

  if (length(stations) == 0) return(empty)

  sta_df <- data.frame(
    id   = vapply(stations, `[[`, character(1), "id"),
    rssi = vapply(stations, function(s) if (is.na(s$rssi)) NA_integer_ else s$rssi, integer(1)),
    n    = vapply(stations, function(s) if (is.na(s$n))   NA_integer_ else s$n,    integer(1)),
    stringsAsFactors = FALSE
  )

  # sort best RSSI first
  sta_df <- sta_df[order(sta_df$rssi, decreasing = TRUE, na.last = TRUE), ]

  canonical <- paste(
    paste(sta_df$id, sta_df$rssi, sta_df$n, sep = ":"),
    collapse = "|"
  )

  list(
    raw          = x,
    canonical    = canonical,
    n_stations   = nrow(sta_df),
    best_rssi    = sta_df$rssi[1],
    best_id      = sta_df$id[1],
    mean_rssi    = if (all(is.na(sta_df$rssi))) NA_real_ else mean(sta_df$rssi, na.rm = TRUE),
    total_frames = if (all(is.na(sta_df$n)))    NA_integer_ else sum(sta_df$n, na.rm = TRUE),
    stations     = sta_df
  )
}


# -----------------------------------------------------------------------------
# parse_basestations(x)
#
# Vectorised version of parse_basestation_string().
#
# Args:
#   x  character vector of raw basestation strings (one per fix)
#
# Returns a data frame with one row per element of x:
#   canonical    — normalised "ID:RSSI:N|..." string (stations sorted best RSSI first)
#   n_stations   — number of base stations
#   best_rssi    — highest (least negative) RSSI
#   best_id      — ID of the best station
#   mean_rssi    — mean RSSI across stations
#   total_frames — total frames received (sum of N)
# -----------------------------------------------------------------------------
parse_basestations <- function(x) {
  parsed <- lapply(as.character(x), parse_basestation_string)

  data.frame(
    canonical    = vapply(parsed, `[[`, character(1), "canonical"),
    n_stations   = vapply(parsed, `[[`, integer(1),   "n_stations"),
    best_rssi    = vapply(parsed, `[[`, integer(1),   "best_rssi"),
    best_id      = vapply(parsed, `[[`, character(1), "best_id"),
    mean_rssi    = vapply(parsed, `[[`, double(1),    "mean_rssi"),
    total_frames = vapply(parsed, `[[`, integer(1),   "total_frames"),
    stringsAsFactors = FALSE
  )
}


# -----------------------------------------------------------------------------
# add_basestation_cols(df, col, prefix)
#
# Convenience wrapper: parse a column in a data frame and attach the summary
# columns back in place.
#
# Args:
#   df      data frame containing the raw basestation column
#   col     name of the column to parse (default: "manually_entered_comments")
#   prefix  prefix for new columns (default: "bs_")
#
# Returns the input data frame with these new columns appended:
#   <prefix>raw          — original string (copy, preserved)
#   <prefix>canonical    — normalised representation
#   <prefix>n_stations
#   <prefix>best_rssi
#   <prefix>best_id
#   <prefix>mean_rssi
#   <prefix>total_frames
# -----------------------------------------------------------------------------
add_basestation_cols <- function(df, col = "manually_entered_comments", prefix = "bs_") {
  if (!col %in% names(df)) {
    warning("Column '", col, "' not found in data frame. Returning df unchanged.")
    return(df)
  }

  summaries       <- parse_basestations(df[[col]])
  names(summaries) <- paste0(prefix, names(summaries))

  df[[paste0(prefix, "raw")]] <- df[[col]]
  cbind(df, summaries)
}


# =============================================================================
# Tests — run with: source("parse_basestations.R"); .run_tests()
# =============================================================================
.run_tests <- function() {
  cat("Running parse_basestation tests...\n\n")
  pass <- 0L; fail <- 0L

  .check <- function(label, got, expected) {
    ok <- isTRUE(all.equal(got, expected))
    if (ok) {
      cat(" PASS:", label, "\n"); pass <<- pass + 1L
    } else {
      cat(" FAIL:", label, "\n  got:     ", deparse(got),
          "\n  expected:", deparse(expected), "\n")
      fail <<- fail + 1L
    }
  }

  # --- Format A single block --------------------------------------------------
  a1 <- parse_basestation_string("[{ID:37AE,N:2,RSSI:-140},{ID:6353,N:1,RSSI:-139}]")
  .check("A: n_stations",   a1$n_stations,   2L)
  .check("A: best_rssi",    a1$best_rssi,    -139L)
  .check("A: best_id",      a1$best_id,      "6353")
  .check("A: total_frames", a1$total_frames, 3L)
  .check("A: canonical",    a1$canonical,    "6353:-139:1|37AE:-140:2")

  # --- Format A multiple blocks (space-separated) ----------------------------
  a2 <- parse_basestation_string(
    "[{ID:37AE,N:2,RSSI:-140},{ID:6353,N:1,RSSI:-139}] [{ID:CD24,N:1,RSSI:-132},{ID:CD0D,N:2,RSSI:-126}]"
  )
  .check("A-multi: n_stations", a2$n_stations, 4L)
  .check("A-multi: best_rssi",  a2$best_rssi,  -126L)
  .check("A-multi: best_id",    a2$best_id,    "CD0D")

  # --- Format B pipe-separated ------------------------------------------------
  b1 <- parse_basestation_string("28D1, -135, 1 | 28C8, -137, 1")
  .check("B-pipe: n_stations",   b1$n_stations,   2L)
  .check("B-pipe: best_rssi",    b1$best_rssi,    -135L)
  .check("B-pipe: best_id",      b1$best_id,      "28D1")
  .check("B-pipe: total_frames", b1$total_frames, 2L)
  .check("B-pipe: canonical",    b1$canonical,    "28D1:-135:1|28C8:-137:1")

  # --- Format B single (no pipe) ----------------------------------------------
  b2 <- parse_basestation_string("277A, -127, 1")
  .check("B-single: n_stations", b2$n_stations, 1L)
  .check("B-single: best_rssi",  b2$best_rssi,  -127L)
  .check("B-single: best_id",    b2$best_id,    "277A")

  # --- Format B multi-pipe ----------------------------------------------------
  b3 <- parse_basestation_string("20E6, -139, 1 | 25F2, -135, 1 | 28AC, -133, 1 | 28C8, -133, 1")
  .check("B-multi: n_stations",   b3$n_stations,   4L)
  .check("B-multi: best_rssi",    b3$best_rssi,    -133L)
  .check("B-multi: total_frames", b3$total_frames, 4L)

  # --- NA / empty input -------------------------------------------------------
  na1 <- parse_basestation_string(NA)
  .check("NA: n_stations", na1$n_stations, 0L)
  .check("NA: best_rssi",  na1$best_rssi,  NA_integer_)

  emp <- parse_basestation_string("")
  .check("empty: n_stations", emp$n_stations, 0L)

  # --- Vectorised parse_basestations() ----------------------------------------
  df <- parse_basestations(c(
    "[{ID:37AE,N:2,RSSI:-140}]",
    "28D1, -135, 1 | 28C8, -137, 1",
    NA
  ))
  .check("vec: nrow",       nrow(df),          3L)
  .check("vec: n_stations", df$n_stations,     c(1L, 2L, 0L))
  .check("vec: best_rssi",  df$best_rssi,      c(-140L, -135L, NA_integer_))

  cat("\nResults:", pass, "passed,", fail, "failed\n")
  invisible(list(pass = pass, fail = fail))
}
