# Wildcloud to Movebank ----
# Updated: 2026-03-25
# Edward Hurme

# Summary:
# Process wildcloud data to be in the correct format for movebank uploads.
# Includes optional clock-drift correction for tags without a real-time clock.
#
# Dependencies: dplyr, tidyr, stringr, lubridate, readr, fs, data.table, purrr
# Optional:     lme4 (for drift correction), ggplot2, forcats (for drift diagnostics)

# ---- Source drift correction module ----
# Expects correct_time_drift.R in the same directory (or adjust path)
# source("correct_time_drift.R")


# =============================================================================
# Helper functions
# =============================================================================

# ---- Helper: parse "14.06.2025, 17:56:24" as UTC ----
parse_wc_time_utc <- function(x) {
  require(lubridate)
  x <- as.character(x)
  x <- gsub(",", "", x)
  lubridate::dmy_hms(x, tz = "UTC")
}

# ---- Helper: split "lat, lon" in Position ----
split_position_wc <- function(df) {
  require(stringr)
  if (!"Position" %in% names(df)) return(df)
  parts <- stringr::str_split_fixed(df$Position, ",\\s*", 2)
  if (!("latitude [°]" %in% names(df)))  df$`latitude [°]`  <- suppressWarnings(as.numeric(parts[, 1]))
  if (!("longitude [°]" %in% names(df))) df$`longitude [°]` <- suppressWarnings(as.numeric(parts[, 2]))
  df
}

# ---- Extract max RSSI from base station string ----
# Expected input examples:
#   "123,-98,1;456,-105,2"
#   "123, -98, 1 | 456, -105, 2"
# Returns the strongest RSSI (highest numeric value; e.g. -82 > -101)
extract_max_rssi <- function(bs_string) {
  if (length(bs_string) == 0 || is.na(bs_string) || trimws(bs_string) == "") {
    return(NA_real_)
  }

  x <- as.character(bs_string)

  # Normalize separators between station records
  x <- gsub("\\|", ";", x)
  x <- gsub("\\s*;\\s*", ";", x)

  # Split into individual station records
  stations <- unlist(strsplit(x, ";", fixed = FALSE))
  stations <- trimws(stations)
  stations <- stations[nzchar(stations)]

  if (length(stations) == 0) return(NA_real_)

  # For each station record, split on commas and take the 2nd field as RSSI
  rssi_vals <- vapply(stations, function(st) {
    parts <- trimws(unlist(strsplit(st, ",", fixed = TRUE)))
    if (length(parts) >= 2) {
      suppressWarnings(as.numeric(parts[2]))
    } else {
      NA_real_
    }
  }, numeric(1))

  rssi_vals <- rssi_vals[!is.na(rssi_vals)]

  if (length(rssi_vals) == 0) NA_real_ else max(rssi_vals)
}

# =============================================================================
# Firmware sampling configuration
# =============================================================================
# VeDBA values in WildCloud CSVs are SUMS over a measurement window. To get
# the per-burst average (VeDBA_avg), divide by vedba_count — the number of
# accelerometer bursts sampled within that window. vedba_count varies by tag
# model and firmware/software version.
#
# This function returns the default lookup table. Users can extend or override
# it by passing a custom data frame to the sampling_config parameter.
#
# Sources: ICARUS SigFox sampling summary documentation

get_default_sampling_config <- function() {
  tibble::tribble(
    ~tag_model,    ~software_version,                        ~vedba_count, ~burst_duration_s, ~burst_rate_hz, ~sampling_interval_s, ~sampling_count, ~vedba_type,             ~notes,

    # =========================================================================
    # TinyFoxBatt (ICARUS TinyFox)
    # =========================================================================
    # VeDBA is a CUMULATIVE TOTAL transmitted once per message, not a windowed
    # sum. Each day: 1-second burst at 28 Hz, every 60 seconds = 1440 bursts/day.
    # To get daily VeDBA: difference between two consecutive days' totals.
    # vedba_count = 1440 per day (for computing per-burst average from the daily
    # difference). VeDBA resolution: 3.9 mg. Sensor noise floor: ~195 mg/burst.
    # Also transmits: activity % (proportion of 1440 samples > 1560 mg),
    # min/max temperature of last 24 hours.
    "TinyFox",     "V13",             1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    "TinyFox",     "V13P",            1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    "TinyFox",     "V14P",            1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    "TinyFox",     "TV1",             1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    "TinyFox",     "BBV6",            1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    "TinyFox",     "BBX5",            1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",
    # Fallback for any unlisted TinyFox firmware
    "TinyFox",     "all",             1440,     1.0,    28,    60,    1440,    "cumulative_daily",  "1s burst @ 28Hz, every 60s, 1440/day",

    # =========================================================================
    # NanoFox
    # =========================================================================
    # VeDBA is a WINDOWED SUM over a measurement window (not cumulative).
    # vedba_count = number of bursts within the window.
    #
    # 30Days: 1s bursts at 28 Hz, every 2 min, for 36 min per window
    #   vedba_count = 36min / 2min = 18 bursts per window → 18 × 28 = 504 samples
    #   Reports: 5 × VeDBA windows, 5 × avg temp windows, min pressure 3hr,
    #   min temp range 3hr
    "NanoFox",     "30Days",          504,      1.0,    28,    120,   18,      "windowed_sum",      "1s burst @ 28Hz, every 2min, 36min window",

    # FineScalePressure: same VeDBA as 30Days (504 vedba_count).
    #   Instead of 5 avg temperature windows, reports 5 PRESSURE measurements.
    #   Also reports max AND min temperature over last 3 hours.
    "NanoFox",     "FineScalePressure", 504,    1.0,    28,    120,   18,      "windowed_sum",      "same VeDBA as 30Days; 5×pressure instead of 5×temp",
    # DailyVeDBA: 1s bursts at 28 Hz, every 3 min, for 1 day
    "NanoFox",     "DailyVeDBA",      13440,    1.0,    28,    180,   480,     "windowed_sum",      "1s burst @ 28Hz, every 3min, 24hr window",

    # 10Day: 1s bursts at 28 Hz, every 2 min, for 12 min
    "NanoFox",     "10Day",           168,      1.0,    28,    120,   6,       "windowed_sum",      "1s burst @ 28Hz, every 2min, 12min window",

    # BBX5 NanoFox — assumed 30Days schedule until confirmed
    "NanoFox",     "BBX5",            504,      1.0,    28,    120,   18,      "windowed_sum",      "assumed same as 30Days — verify",

    # =========================================================================
    # TinyFoxTwo
    # =========================================================================
    # 1s bursts at 28 Hz, every 1 min, for 60 min per window
    "TinyFoxTwo",  "TINYFOXTWO BLACKBIRDS V", 1680, 1.0, 28,  60,    60,      "windowed_sum",      "1s burst @ 28Hz, every 1min, 60min window"
  )
}

# Match a tag model string to a known model family
match_tag_model <- function(model_str) {
  model_str <- tolower(trimws(as.character(model_str)))
  dplyr::case_when(
    grepl("tinyfox\\s*two|tinyfoxtwo", model_str)              ~ "TinyFoxTwo",
    grepl("tinyfox\\s*batt|tinyfoxbatt", model_str)            ~ "TinyFox",
    grepl("tinyfox|tiny\\s*fox", model_str)                    ~ "TinyFox",
    grepl("nanofox|nano\\s*fox", model_str)                    ~ "NanoFox",
    grepl("sigfox\\s*gh|georg|unikonstanz", model_str)         ~ "SigfoxGH",
    TRUE                                                       ~ NA_character_
  )
}


# =============================================================================
# 1a) Harmonize WildCloud column names across firmware versions
# =============================================================================
# Different firmware versions (V13, V13P, V15, …) and tag models (TinyFoxBatt,
# NanoFox, …) export CSVs with varying column names for the same measurements.
# This function maps known variants to canonical names so downstream code can
# use a single set of column references.
#
# Returns the data frame with renamed columns. Unknown columns pass through
# unchanged. Reports any renames performed.

harmonize_wc_columns <- function(df) {
  require(stringr)

  # Canonical name -> regex patterns that match known firmware variants.
  # Order matters: first match wins. Patterns are case-insensitive.
  canonical_map <- list(
    # ---- core identifiers ----
    "Device"                             = "^(Device|Device\\s*ID|device_id|Tag\\s*ID|tag_id)$",
    "Time (UTC)"                         = "^(Time\\s*\\(UTC\\)|Timestamp\\s*\\(UTC\\)|time_utc|datetime_utc)$",
    "Sequence Number"                    = "^(Sequence\\s*Number|Seq\\s*Number|sequence_number|seq_num)$",
    "Raw Data"                           = "^(Raw\\s*Data|raw_data|Payload)$",
    "Position"                           = "^(Position|Lat,\\s*Lon|position)$",

    # ---- signal metadata ----
    "Radius (m) (Source/Status)"         = "^(Radius\\s*\\(m\\)\\s*\\(Source/Status\\)|Radius\\s*\\(m\\)|radius_m|location_radius)$",
    "LQI"                                = "^(LQI|lqi|Link\\s*Quality\\s*Indicator)$",
    "Link Quality"                       = "^(Link\\s*Quality|link_quality|linkquality)$",
    "Operator Name"                      = "^(Operator\\s*Name|operator_name|operator)$",
    "Country Code"                       = "^(Country\\s*Code|country_code|countrycode)$",
    "Base Stations (ID, RSSI, Reps)"     = "^(Base\\s*Stations.*|base_stations|BS\\s*Info)$",
    "Compression"                        = "^(Compression|compression)$",

    # ---- summary sensors (3-hr or other window) ----
    # Min pressure: narrow pattern that will NOT match bin-style "Pressure X min ago"
    "Min pressure of last 3 hrs (mbar)"  = "^Min\\.?\\s*pressure",
    # Min temperature: matches legacy range-label ">10", new numeric, and FineScalePressure variants
    "Min temperature of last 3 hrs (°C)" =
      "^Min\\.?\\s*temp(erature)?\\s*(of|range|\\()",
    # Max temperature: new in FineScalePressure firmware
    "Max temperature of last 3 hrs (°C)" =
      "^Max\\.?\\s*temp(erature)?\\s*(of|\\()"
  )

  renames_done <- character(0)
  current_names <- names(df)

  for (canonical in names(canonical_map)) {
    pattern <- canonical_map[[canonical]]
    # Skip if canonical name already exists
    if (canonical %in% current_names) next
    # Find matches (case-insensitive)
    matches <- current_names[stringr::str_detect(current_names, regex(pattern, ignore_case = TRUE))]
    if (length(matches) == 1 && matches != canonical) {
      names(df)[names(df) == matches] <- canonical
      renames_done <- c(renames_done, paste0("'", matches, "' -> '", canonical, "'"))
      current_names <- names(df)
    }
  }

  # ---- VeDBA bin columns: normalize to "VeDBA sum X min ago" pattern ----
  vedba_variants <- current_names[stringr::str_detect(
    current_names, regex("^(vedba|VeDBA).*\\d+.*min", ignore_case = TRUE)
  )]
  for (vc in vedba_variants) {
    mins <- stringr::str_extract(vc, "\\d+")
    canonical_vc <- paste("VeDBA sum", mins, "min ago")
    if (vc != canonical_vc && !(canonical_vc %in% names(df))) {
      names(df)[names(df) == vc] <- canonical_vc
      renames_done <- c(renames_done, paste0("'", vc, "' -> '", canonical_vc, "'"))
    }
  }

  # ---- Temperature bin columns: normalize to "Average temperature X min ago" ----
  # Must match "X min ago" AND not start with "Min" or "Max" (those are summaries).
  temp_variants <- names(df)[stringr::str_detect(
    names(df),
    regex("^(avg|average|mean)?\\s*(temp|temperature).*\\d+\\s*min\\s*ago",
          ignore_case = TRUE)
  )]
  temp_variants <- temp_variants[!stringr::str_detect(
    temp_variants, regex("^(min|max)", ignore_case = TRUE)
  )]
  for (tc in temp_variants) {
    mins <- stringr::str_extract(tc, "\\d+")
    canonical_tc <- paste("Average temperature", mins, "min ago")
    if (tc != canonical_tc && !(canonical_tc %in% names(df))) {
      names(df)[names(df) == tc] <- canonical_tc
      renames_done <- c(renames_done, paste0("'", tc, "' -> '", canonical_tc, "'"))
    }
  }

  # ---- Pressure bin columns (FineScalePressure firmware) ----
  # 5 instantaneous pressure readings at 0/36/72/108/144 min ago; replaces the
  # temperature bins in NanoFox FineScalePressure firmware.
  pres_variants <- names(df)[stringr::str_detect(
    names(df), regex("^Pressure\\s+\\d+\\s*min", ignore_case = TRUE)
  )]
  for (pc in pres_variants) {
    mins <- stringr::str_extract(pc, "\\d+")
    canonical_pc <- paste("Pressure", mins, "min ago")
    if (pc != canonical_pc && !(canonical_pc %in% names(df))) {
      names(df)[names(df) == pc] <- canonical_pc
      renames_done <- c(renames_done, paste0("'", pc, "' -> '", canonical_pc, "'"))
    }
  }

  if (length(renames_done) > 0) {
    message("[harmonize] Renamed ", length(renames_done), " column(s): ",
            paste(renames_done, collapse = "; "))
  }

  df
}


# =============================================================================
# 1b) Expand NEW Wildcloud wide schema into a long table
# =============================================================================
#    Output contains:
#    - location rows (start=end=tx time)
#    - VeDBA (5 x 36-min windows) -> VeDBA [m/s²]
#    - temp  (5 x 36-min windows) -> temperature [°C]
#    - min pressure (3-hr)        -> pressure [mbar]
#    - min temp range label (3-hr)-> min_temp_range [°C]

wc_wide_to_mb_long <- function(df) {
  suppressPackageStartupMessages({
    require(dplyr)
    require(tidyr)
    require(stringr)
    require(lubridate)
  })

  if (!("Time (UTC)" %in% names(df))) return(df)

  vedba_cols <- names(df)[stringr::str_detect(names(df), "^VeDBA sum\\s+\\d+\\s+min ago")]
  temp_cols  <- names(df)[stringr::str_detect(names(df), "^Average temperature\\s+\\d+\\s+min ago")]
  # Pressure bins (FineScalePressure firmware): 5 instantaneous readings
  pres_bin_cols <- names(df)[stringr::str_detect(names(df), "^Pressure\\s+\\d+\\s+min ago")]

  # Detect min/max temp and min pressure summary columns flexibly
  min_temp_col <- NULL
  max_temp_col <- NULL
  min_pres_col <- NULL

  canonical_min_temp <- "Min temperature of last 3 hrs (°C)"
  canonical_max_temp <- "Max temperature of last 3 hrs (°C)"
  canonical_min_pres <- "Min pressure of last 3 hrs (mbar)"

  if (canonical_min_temp %in% names(df)) {
    min_temp_col <- canonical_min_temp
  } else {
    mt_match <- names(df)[stringr::str_detect(names(df),
                                              regex("^Min\\.?\\s*temp", ignore_case = TRUE))]
    if (length(mt_match) == 1) min_temp_col <- mt_match
  }

  if (canonical_max_temp %in% names(df)) {
    max_temp_col <- canonical_max_temp
  } else {
    mx_match <- names(df)[stringr::str_detect(names(df),
                                              regex("^Max\\.?\\s*temp", ignore_case = TRUE))]
    if (length(mx_match) == 1) max_temp_col <- mx_match
  }

  if (canonical_min_pres %in% names(df)) {
    min_pres_col <- canonical_min_pres
  } else {
    # "Min" + "press" only (NOT bin-style "Pressure X min ago")
    mp_match <- names(df)[stringr::str_detect(names(df),
                                              regex("^Min\\.?\\s*press", ignore_case = TRUE))]
    if (length(mp_match) == 1) min_pres_col <- mp_match
  }

  bs_col <- "Base Stations (ID, RSSI, Reps)"

  has_new <- length(vedba_cols) > 0 || length(temp_cols) > 0 ||
    length(pres_bin_cols) > 0 ||
    !is.null(min_pres_col) || !is.null(min_temp_col) || !is.null(max_temp_col)

  if (!has_new) return(df)

  message(sprintf(
    "[expand] Sensor columns found: %d VeDBA bins, %d temp bins, %d pressure bins, min_pressure=%s, min_temp=%s, max_temp=%s",
    length(vedba_cols), length(temp_cols), length(pres_bin_cols),
    ifelse(is.null(min_pres_col), "none", min_pres_col),
    ifelse(is.null(min_temp_col), "none", min_temp_col),
    ifelse(is.null(max_temp_col), "none", max_temp_col)
  ))

  df <- df %>%
    mutate(
      `timestamp SF transmission` = parse_wc_time_utc(`Time (UTC)`)
    ) %>%
    split_position_wc()

  # Coerce optional metadata columns if present
  if ("Sequence Number" %in% names(df)) df$`Sequence Number` <- suppressWarnings(as.numeric(df$`Sequence Number`))
  if ("Link Quality" %in% names(df))    df$`Link Quality`    <- suppressWarnings(as.numeric(df$`Link Quality`))
  if ("Country Code" %in% names(df))    df$`Country Code`    <- suppressWarnings(as.numeric(df$`Country Code`))

  # Compute max RSSI once per transmission row
  if (bs_col %in% names(df)) {
    df$max_rssi <- vapply(df[[bs_col]], extract_max_rssi, numeric(1))
  } else {
    df$max_rssi <- NA_real_
  }

  base_cols <- intersect(
    c("Device", "Time (UTC)", "Raw Data", "Position",
      "Radius (m) (Source/Status)", "Sequence Number",
      "LQI", "Link Quality", "Operator Name", "Country Code",
      "Base Stations (ID, RSSI, Reps)", "Compression",
      "max_rssi",
      "timestamp SF transmission", "latitude [°]", "longitude [°]"),
    names(df)
  )

  base <- df[, base_cols, drop = FALSE] %>%
    mutate(.row_id = dplyr::row_number())

  build_36min_long <- function(value_cols, value_name) {
    if (length(value_cols) == 0) return(NULL)

    df %>%
      mutate(.row_id = dplyr::row_number()) %>%
      dplyr::select(.row_id, all_of(value_cols)) %>%
      left_join(base, by = ".row_id") %>%
      tidyr::pivot_longer(
        cols = all_of(value_cols),
        names_to = "metric",
        values_to = value_name
      ) %>%
      mutate(
        minutes_ago = suppressWarnings(as.numeric(stringr::str_extract(metric, "\\d+"))),
        .end = `timestamp SF transmission` - as.difftime(minutes_ago, units = "mins"),
        `Time end`   = .end,
        `Time start` = .end - as.difftime(36, units = "mins")
      ) %>%
      dplyr::select(-metric, -minutes_ago, -.end)
  }

  vedba_long    <- build_36min_long(vedba_cols,    "VeDBA [m/s²]")
  temp_long     <- build_36min_long(temp_cols,     "temperature [°C]")
  # FineScalePressure: 5 instantaneous pressure readings (36-min bins)
  pres_bin_long <- build_36min_long(pres_bin_cols, "pressure [mbar]")

  if (!is.null(vedba_long))    vedba_long$`VeDBA [m/s²]`    <- suppressWarnings(as.numeric(vedba_long$`VeDBA [m/s²]`))
  if (!is.null(temp_long))     temp_long$`temperature [°C]` <- suppressWarnings(as.numeric(temp_long$`temperature [°C]`))
  if (!is.null(pres_bin_long)) pres_bin_long$`pressure [mbar]` <- suppressWarnings(as.numeric(pres_bin_long$`pressure [mbar]`))

  three_hr_base <- base %>%
    mutate(
      `Time start` = `timestamp SF transmission` - as.difftime(180, units = "mins"),
      `Time end`   = `timestamp SF transmission`
    )

  # 3-hr min pressure (legacy firmware: single aggregate value over the window)
  pres_long <- NULL
  if (!is.null(min_pres_col)) {
    pres_long <- three_hr_base %>%
      mutate(`pressure [mbar]` = suppressWarnings(as.numeric(df[[min_pres_col]]))) %>%
      dplyr::select(-.row_id)
  }

  # 3-hr min temp (character range label ">10" in legacy, numeric in new firmware)
  min_temp_long <- NULL
  if (!is.null(min_temp_col)) {
    min_temp_long <- three_hr_base %>%
      mutate(`min_temp_range [°C]` = as.character(df[[min_temp_col]])) %>%
      dplyr::select(-.row_id)
  }

  # 3-hr max temp (new in FineScalePressure firmware)
  max_temp_long <- NULL
  if (!is.null(max_temp_col)) {
    max_temp_long <- three_hr_base %>%
      mutate(`max_temp_range [°C]` = as.character(df[[max_temp_col]])) %>%
      dplyr::select(-.row_id)
  }

  loc_rows <- base %>%
    mutate(
      `Time start` = `timestamp SF transmission`,
      `Time end`   = `timestamp SF transmission`,
      mb_record_type = "location"
    ) %>%
    dplyr::select(-.row_id)

  out <- dplyr::bind_rows(
    loc_rows,
    if (!is.null(vedba_long))    vedba_long    %>% mutate(mb_record_type = "vedba")          %>% dplyr::select(-.row_id),
    if (!is.null(temp_long))     temp_long     %>% mutate(mb_record_type = "temperature")    %>% dplyr::select(-.row_id),
    if (!is.null(pres_bin_long)) pres_bin_long %>% mutate(mb_record_type = "pressure_bin")   %>% dplyr::select(-.row_id),
    if (!is.null(pres_long))     pres_long     %>% mutate(mb_record_type = "pressure"),
    if (!is.null(min_temp_long)) min_temp_long %>% mutate(mb_record_type = "min_temp_range"),
    if (!is.null(max_temp_long)) max_temp_long %>% mutate(mb_record_type = "max_temp_range")
  ) %>%
    dplyr::distinct(
      Device, `timestamp SF transmission`, `Time start`, `Time end`,
      `latitude [°]`, `longitude [°]`,
      mb_record_type,
      .keep_all = TRUE
    )

  out
}


# =============================================================================
# 2) Writer: standard Movebank upload CSV set, per project
# =============================================================================

write_movebank_upload_csvs <- function(
    loc_data, vedba_data, bar_data, temp_data, min_temp_data, deployment_data,
    max_temp_data = NULL,
    output_dir
) {
  suppressPackageStartupMessages({
    require(fs)
    require(readr)
    require(stringr)
    require(dplyr)
  })

  if (!fs::dir_exists(output_dir)) fs::dir_create(output_dir)

  projects <- unique(na.omit(loc_data$Movebank.Project))
  if (length(projects) == 0) projects <- unique(na.omit(deployment_data$Movebank.Project))

  for (proj in projects) {
    safe_proj_name <- stringr::str_replace_all(proj, "[^A-Za-z0-9_\\-]", "_")
    project_folder <- file.path(output_dir, "Projects", safe_proj_name)
    if (!fs::dir_exists(project_folder)) fs::dir_create(project_folder, recurse = TRUE)

    loc_proj      <- loc_data      %>% filter(Movebank.Project == proj)
    vedba_proj    <- vedba_data    %>% filter(Movebank.Project == proj)
    bar_proj      <- bar_data      %>% filter(Movebank.Project == proj)
    temp_proj     <- temp_data     %>% filter(Movebank.Project == proj)
    min_temp_proj <- min_temp_data %>% filter(Movebank.Project == proj)
    dep_proj      <- deployment_data %>%
      filter(Movebank.Project == proj) %>%
      dplyr::select(-Movebank.Project)

    readr::write_csv(loc_proj,      file.path(project_folder, "data.csv"), na = "")
    readr::write_csv(vedba_proj,    file.path(project_folder, "dataVeDBA.csv"), na = "")
    readr::write_csv(bar_proj,      file.path(project_folder, "dataBar.csv"), na = "")
    readr::write_csv(temp_proj,     file.path(project_folder, "dataTemp.csv"), na = "")
    readr::write_csv(min_temp_proj, file.path(project_folder, "dataMinTemp.csv"), na = "")
    readr::write_csv(dep_proj,      file.path(project_folder, "deployment.csv"), na = "")

    if (!is.null(max_temp_data) && nrow(max_temp_data) > 0) {
      max_temp_proj <- max_temp_data %>% filter(Movebank.Project == proj)
      readr::write_csv(max_temp_proj, file.path(project_folder, "dataMaxTemp.csv"), na = "")
    }

    message("Saved Movebank CSVs for project: ", proj, " -> ", project_folder)
  }

  invisible(TRUE)
}


# =============================================================================
# 3) Main pipeline function
# =============================================================================

wildcloud_to_movebank <- function(
    wc_path              = NULL,
    movebank_csv_path    = NULL,
    animals_path,
    output_dir,
    movebank_project_name,
    species              = NULL,
    life_stage           = NULL,
    animal_id_col        = NULL,
    location_abbr        = NULL,
    clean_lat_range      = NULL,
    clean_lon_range      = NULL,
    sampling_config      = NULL,
    force_tag_model_family = NULL,
    force_firmware_version = NULL,
    force_vedba_count      = NULL,
    correct_drift        = FALSE,
    programmed_interval  = NULL,
    drift_temp_col       = NULL,
    drift_poly_degree    = 2L,
    drift_max_filter     = 0.10,
    drift_min_obs        = 5L,
    drift_diagnostics    = FALSE
) {
  suppressPackageStartupMessages({
    require(dplyr)
    require(tidyr)
    require(stringr)
    require(lubridate)
    require(readr)
    require(fs)
    require(data.table)
  })

  # ---- checks ----
  if (is.null(wc_path) && is.null(movebank_csv_path)) {
    stop("Provide either 'wc_path' (WildCloud directory) or 'movebank_csv_path' (CSV).")
  }
  if (!is.null(wc_path) && !fs::dir_exists(wc_path)) stop("wc_path does not exist: ", wc_path)
  if (!is.null(movebank_csv_path) && !file.exists(movebank_csv_path)) stop("movebank_csv_path does not exist: ", movebank_csv_path)
  if (!file.exists(animals_path)) stop("animals_path does not exist: ", animals_path)
  if (!fs::dir_exists(output_dir)) fs::dir_create(output_dir, recurse = TRUE)

  if (correct_drift && !requireNamespace("lme4", quietly = TRUE))
    stop("Package 'lme4' required for drift correction. Install with install.packages('lme4').")

  # ---- animals sheet ----
  ext <- tolower(tools::file_ext(animals_path))
  if (ext %in% c("xlsx", "xls")) {
    if (!requireNamespace("readxl", quietly = TRUE))
      stop("Package 'readxl' required to read .xlsx/.xls files. ",
           "Install with install.packages('readxl').")
    animals <- readxl::read_excel(animals_path, sheet = 1) %>% as.data.frame(stringsAsFactors = FALSE)
    message("[IO] Read reference data from Excel: ", animals_path,
            " (", nrow(animals), " rows, ", ncol(animals), " cols)")
  } else {
    sep <- if (ext == "tsv") "\t" else ","
    animals <- read.csv(animals_path, header = TRUE, sep = sep, stringsAsFactors = FALSE)
    message("[IO] Read reference data from CSV: ", animals_path,
            " (", nrow(animals), " rows, ", ncol(animals), " cols)")
  }

  # ---- resolve Animal.ID via per-row priority cascade ----
  # Movebank's animal.id must identify the biological individual, not the tag.
  # Since bats can be retrapped and given new tags (19 cases in the Swiss data),
  # the ID must come from a permanent marker when available.
  #
  # Per-row priority: PIT tag > ring/band > animal.id column > tag ID
  # The cascade is applied ROW BY ROW so that rows missing a PIT tag
  # still get the best available fallback rather than NA.
  #
  # animal_id_col: explicit override — skips auto-detection entirely.

  # ---- auto-detect columns by flexible name matching ----
  detect_col <- function(df, patterns) {
    # Match against lowercase, stripped of dots/underscores/spaces
    clean <- function(x) tolower(gsub("[._ ]", "", x))
    cleaned_names <- clean(names(df))
    cleaned_pats  <- clean(patterns)
    matches <- names(df)[cleaned_names %in% cleaned_pats]
    if (length(matches) > 0) matches[1] else NULL
  }

  pit_patterns <- c("PIT tag ID", "animal.marker.id", "pit.tag", "pit.id",
                    "pit_tag", "pit_id", "pittagid", "transponder", "microchip")
  ring_patterns <- c("ring number", "ring.number", "ring_number", "animal.ring.id",
                     "ring.id", "ring_id", "band.id", "band_id", "forearm.band",
                     "ringnumber")
  tag_patterns <- c("tag.id", "tag_id", "tag ID", "tagid", "device")

  pit_col  <- detect_col(animals, pit_patterns)
  ring_col <- detect_col(animals, ring_patterns)
  tag_col  <- detect_col(animals, tag_patterns)

  if (is.null(tag_col))
    stop("No tag ID column found in reference CSV. ",
         "Expected one of: ", paste(tag_patterns, collapse = ", "), ". ",
         "Available columns: ", paste(names(animals), collapse = ", "))

  # ---- auto-detect remaining reference data columns ----
  species_col <- detect_col(animals, c("animal.taxon", "species", "taxon",
                                       "animal.species", "animaltaxon"))
  genus_col   <- detect_col(animals, c("genus", "animal.genus"))
  stage_col   <- detect_col(animals, c("animal.life.stage", "age", "life.stage",
                                       "life_stage", "lifestage", "animallifestage"))
  mass_col    <- detect_col(animals, c("animal.mass", "weight bat", "weight",
                                       "mass", "body.mass", "bodymass",
                                       "animalmass", "weightbat"))
  lat_col     <- detect_col(animals, c("capture.latitude", "Deploy.On.Latitude",
                                       "latitude_ycoord", "latitude", "lat",
                                       "capturelatitude", "deployonlatitude",
                                       "latitudeycoord"))
  lon_col     <- detect_col(animals, c("capture.longitude", "Deploy.On.Longitude",
                                       "longitude_xcoord", "longitude", "lon",
                                       "capturelongitude", "deployonlongitude",
                                       "longitudexcoord"))
  date_col    <- detect_col(animals, c("deploy.on.date", "capture date",
                                       "capture.date", "deployment.date",
                                       "deployondate", "capturedate",
                                       "deploymentdate"))
  deploy_time_col <- detect_col(animals, c("tag deployment time", "capture time",
                                           "capture.time", "deploy.on.time",
                                           "tagdeploymenttime", "capturetime",
                                           "deployontime"))
  sex_col     <- detect_col(animals, c("animal.sex", "sex", "animalsex"))
  fa_col      <- detect_col(animals, c("FA 1", "forearm", "forearm.length",
                                       "fa1", "fa", "forearmlength"))
  attach_col  <- detect_col(animals, c("tag attachment", "tag.attachment",
                                       "tag_attachment", "tagattachment",
                                       "attachment.type", "attachmenttype"))
  repro_col   <- detect_col(animals, c("reproductive state", "reproductive.state",
                                       "reproductive_state", "reproductivestate",
                                       "repro.state"))
  tag_wt_col  <- detect_col(animals, c("tag weight", "tag.weight", "tag_weight",
                                       "tagweight", "tag.mass", "tagmass"))
  model_col   <- detect_col(animals, c("tag model", "tag.model", "tag_model",
                                       "tagmodel", "sensor.type.id"))
  fw_col      <- detect_col(animals, c("firmware version", "firmware.version",
                                       "firmware_version", "firmwareversion",
                                       "software.version", "software_version"))

  message("[ref] Detected columns — ",
          "species: ", ifelse(is.null(species_col), "none", species_col), ", ",
          "life stage: ", ifelse(is.null(stage_col), "none", stage_col), ", ",
          "sex: ", ifelse(is.null(sex_col), "none", sex_col), ", ",
          "mass: ", ifelse(is.null(mass_col), "none", mass_col), ", ",
          "forearm: ", ifelse(is.null(fa_col), "none", fa_col), ", ",
          "attachment: ", ifelse(is.null(attach_col), "none", attach_col), ", ",
          "repro state: ", ifelse(is.null(repro_col), "none", repro_col), ", ",
          "tag weight: ", ifelse(is.null(tag_wt_col), "none", tag_wt_col), ", ",
          "lat: ", ifelse(is.null(lat_col), "none", lat_col), ", ",
          "lon: ", ifelse(is.null(lon_col), "none", lon_col), ", ",
          "deploy date: ", ifelse(is.null(date_col), "none", date_col), ", ",
          "deploy time: ", ifelse(is.null(deploy_time_col), "none", deploy_time_col), ", ",
          "tag model: ", ifelse(is.null(model_col), "none", model_col), ", ",
          "firmware: ", ifelse(is.null(fw_col), "none", fw_col))

  # Report detected columns
  message("[ID] Detected columns — ",
          "PIT: ", ifelse(is.null(pit_col),  "none", pit_col), ", ",
          "Ring: ", ifelse(is.null(ring_col), "none", ring_col), ", ",
          "Tag: ", ifelse(is.null(tag_col),   "none", tag_col))

  if (!is.null(animal_id_col)) {
    # ---- explicit override ----
    if (!animal_id_col %in% names(animals))
      stop("animal_id_col '", animal_id_col, "' not found in reference CSV. ",
           "Available columns: ", paste(names(animals), collapse = ", "))
    animals$Animal.ID <- as.character(animals[[animal_id_col]])
    animals$id_source <- "user_specified"
    message("[ID] Using user-specified column '", animal_id_col, "' as Animal.ID.")

  } else {
    # ---- per-row cascade: PIT > ring > animal.id > structured fallback ----
    # Build each candidate as character, NA if column absent, blank, or literal "NA"
    safe_char <- function(x) {
      x <- as.character(x)
      x <- trimws(x)
      # Treat literal NA strings as missing
      x <- ifelse(is.na(x) | x == "" | toupper(x) %in% c("NA", "N/A", "NAN", "NULL", "NONE"),
                  NA_character_, x)
      x
    }

    pit_vals  <- if (!is.null(pit_col))  safe_char(animals[[pit_col]])  else rep(NA_character_, nrow(animals))
    ring_vals <- if (!is.null(ring_col)) safe_char(animals[[ring_col]]) else rep(NA_character_, nrow(animals))
    aid_vals  <- if ("animal.id" %in% names(animals)) safe_char(animals$animal.id) else rep(NA_character_, nrow(animals))
    tag_vals  <- if (!is.null(tag_col))  safe_char(animals[[tag_col]])  else rep(NA_character_, nrow(animals))

    # ---- build structured fallback ID: Gspp + YY + NN + Loc + TagID ----
    # e.g. Nnoc + 25 + 01 + Swiss + 9EC016 -> "Nnoc25_01_Swiss_9EC016"
    # Counter increments per (species, year) combination for rows needing the fallback.

    # Species abbreviation: first letter of genus + first 3 of species (lowercase)
    species_abbr <- rep(NA_character_, nrow(animals))
    if (!is.null(genus_col) && !is.null(species_col)) {
      g <- safe_char(animals[[genus_col]])
      s <- safe_char(animals[[species_col]])
      species_abbr <- ifelse(
        !is.na(g) & !is.na(s),
        paste0(toupper(substr(g, 1, 1)), tolower(substr(s, 1, 3))),
        NA_character_
      )
    } else if (!is.null(species_col)) {
      # Use first 4 characters of species column if that's all we have
      s <- safe_char(animals[[species_col]])
      species_abbr <- ifelse(
        !is.na(s),
        paste0(toupper(substr(s, 1, 1)), tolower(substr(s, 2, 4))),
        NA_character_
      )
    }
    species_abbr[is.na(species_abbr)] <- "spp"

    # Year: 2-digit from deploy date
    year_vals <- rep(NA_character_, nrow(animals))
    if (!is.null(date_col)) {
      d_parsed <- suppressWarnings(lubridate::ymd(as.character(animals[[date_col]]),
                                                  truncated = 2, quiet = TRUE))
      year_vals <- ifelse(!is.na(d_parsed),
                          sprintf("%02d", lubridate::year(d_parsed) %% 100),
                          NA_character_)
    }
    year_vals[is.na(year_vals)] <- "00"

    # Location abbreviation: user-supplied via location_abbr argument.
    # Recommended: 3-6 character code like "Swiss", "Catal", "Flan".
    # Defaults to "Unk" if not provided.
    loc_abbr_val <- if (!is.null(location_abbr) && nzchar(trimws(location_abbr))) {
      gsub("[^A-Za-z0-9]", "", trimws(location_abbr))
    } else {
      "Unk"
    }
    loc_abbr <- rep(loc_abbr_val, nrow(animals))

    # Per-year-per-species counter — only for rows needing the fallback
    needs_fallback <- is.na(pit_vals) & is.na(ring_vals) & is.na(aid_vals) & !is.na(tag_vals)

    counter_vals <- rep(NA_character_, nrow(animals))
    if (any(needs_fallback)) {
      fb_df <- data.frame(
        .row = seq_len(nrow(animals)),
        .sp  = species_abbr,
        .yr  = year_vals,
        .fb  = needs_fallback,
        stringsAsFactors = FALSE
      )
      fb_df$.counter <- NA_integer_
      # Order by row then group by species+year, assign sequential counters
      for (grp in unique(paste(fb_df$.sp[fb_df$.fb], fb_df$.yr[fb_df$.fb]))) {
        sp <- strsplit(grp, " ", fixed = TRUE)[[1]][1]
        yr <- strsplit(grp, " ", fixed = TRUE)[[1]][2]
        idx <- which(fb_df$.fb & fb_df$.sp == sp & fb_df$.yr == yr)
        fb_df$.counter[idx] <- seq_along(idx)
      }
      counter_vals <- ifelse(!is.na(fb_df$.counter),
                             sprintf("%02d", fb_df$.counter),
                             NA_character_)
    }

    # Assemble structured fallback ID
    structured_fallback <- ifelse(
      needs_fallback,
      paste0(species_abbr, year_vals, "_", counter_vals, "_", loc_abbr, "_", tag_vals),
      NA_character_
    )

    animals$Animal.ID <- dplyr::coalesce(pit_vals, ring_vals, aid_vals, structured_fallback)

    # Track which source was used per row
    animals$id_source <- dplyr::case_when(
      !is.na(pit_vals)                          ~ "pit_tag",
      is.na(pit_vals) & !is.na(ring_vals)       ~ "ring",
      is.na(pit_vals) & is.na(ring_vals) &
        !is.na(aid_vals)                        ~ "animal_id_col",
      !is.na(structured_fallback)               ~ "structured_fallback",
      TRUE                                      ~ "missing"
    )

    # Report
    src_counts <- table(animals$id_source)
    message("[ID] Animal.ID sources: ",
            paste(names(src_counts), src_counts, sep = "=", collapse = ", "))

    # Warn about structured fallback rows
    n_fallback <- sum(animals$id_source == "structured_fallback")
    if (n_fallback > 0) {
      sample_ids <- head(animals$Animal.ID[animals$id_source == "structured_fallback"], 3)
      message(sprintf(
        "[ID] %d deployment(s) use structured fallback ID (e.g. %s). ",
        n_fallback, paste(sample_ids, collapse = ", ")),
        "These IDs are deterministic per species+year+location+tag — a retrap ",
        "with the same tag will produce the same ID, but a retrap with a ",
        "NEW tag will appear as a separate individual.")
      if (loc_abbr_val == "Unk") {
        message("[ID] Tip: pass location_abbr = \"Swiss\" (or similar) ",
                "to get more meaningful fallback IDs instead of the 'Unk' default.")
      }
    }
  }

  # Validate: warn about still-missing IDs
  n_missing <- sum(is.na(animals$Animal.ID) | animals$Animal.ID == "")
  if (n_missing > 0) {
    warning(sprintf("[ID] %d of %d deployments have missing Animal.ID. ",
                    n_missing, nrow(animals)),
            "These rows will fail to join with tracking data.")
  }

  # Report retraps: same animal with multiple tag deployments
  if (!is.null(tag_col)) {
    id_tag_map <- animals %>%
      filter(!is.na(Animal.ID), Animal.ID != "") %>%
      distinct(Animal.ID, .data[[tag_col]])
    multi_tag <- id_tag_map %>%
      count(Animal.ID) %>%
      filter(n > 1)
    if (nrow(multi_tag) > 0) {
      message(sprintf("[ID] %d individual(s) have multiple tag deployments (retraps):",
                      nrow(multi_tag)))
      for (i in seq_len(min(nrow(multi_tag), 10))) {
        aid <- multi_tag$Animal.ID[i]
        tags <- id_tag_map[[tag_col]][id_tag_map$Animal.ID == aid]
        message("      ", aid, " -> tags: ", paste(tags, collapse = ", "))
      }
      if (nrow(multi_tag) > 10)
        message("      ... and ", nrow(multi_tag) - 10, " more")
    }
  }

  # Store ring and PIT IDs separately for Movebank export
  # Clean literal "NA"/"na" strings to actual NA so they export as empty cells
  clean_id <- function(x) {
    x <- as.character(x)
    x <- trimws(x)
    ifelse(is.na(x) | x == "" | toupper(x) %in% c("NA", "N/A", "NAN", "NULL", "NONE"),
           NA_character_, x)
  }
  animals$Ring.ID <- if (!is.null(ring_col)) clean_id(animals[[ring_col]]) else NA_character_
  animals$PIT.ID  <- if (!is.null(pit_col))  clean_id(animals[[pit_col]])  else NA_character_

  # ---- build species string ----
  # Combine genus + species if both exist; fall back to species_col alone
  if (!is.null(species)) {
    species_vals <- rep(species, nrow(animals))
  } else if (!is.null(genus_col) && !is.null(species_col)) {
    species_vals <- trimws(paste(animals[[genus_col]], animals[[species_col]]))
  } else if (!is.null(species_col)) {
    species_vals <- as.character(animals[[species_col]])
  } else {
    species_vals <- rep(NA_character_, nrow(animals))
    warning("[ref] No species/taxon column found in reference data.")
  }

  # ---- safe column extractor (returns NA vector if column missing) ----
  safe_col <- function(col_name, as_type = "character") {
    if (is.null(col_name)) return(rep(NA, nrow(animals)))
    vals <- animals[[col_name]]
    switch(as_type,
           numeric   = suppressWarnings(as.numeric(vals)),
           character = as.character(vals),
           vals
    )
  }

  animals_mb <- animals %>%
    mutate(
      Tag.ID              = .data[[tag_col]],
      Species             = species_vals,
      `animal.life.stage` = if (!is.null(life_stage)) life_stage else safe_col(stage_col),
      `animal.sex`        = safe_col(sex_col),
      `Weight..g.`        = safe_col(mass_col, "numeric"),
      `forearm.length.mm` = safe_col(fa_col, "numeric"),
      `tag.attachment.raw` = safe_col(attach_col),
      `animal.reproductive.condition` = safe_col(repro_col),
      `tag.mass.g`        = safe_col(tag_wt_col, "numeric"),
      Deploy.On.Latitude  = safe_col(lat_col, "numeric"),
      Deploy.On.Longitude = safe_col(lon_col, "numeric"),
      tag_model_raw       = safe_col(model_col),
      firmware_version    = safe_col(fw_col),
      Movebank.Project    = movebank_project_name
    )

  # ---- standardize values ----

  # Sex: uppercase, standardize to M / F
  animals_mb$`animal.sex` <- toupper(trimws(animals_mb$`animal.sex`))
  animals_mb$`animal.sex` <- ifelse(
    animals_mb$`animal.sex` %in% c("M", "F"),
    animals_mb$`animal.sex`,
    NA_character_
  )

  # Life stage: lowercase, standardize
  animals_mb$`animal.life.stage` <- tolower(trimws(animals_mb$`animal.life.stage`))

  # Mass: round to nearest 0.1 g
  animals_mb$`Weight..g.` <- round(animals_mb$`Weight..g.`, 1)

  # Forearm: round to nearest 0.1 mm
  animals_mb$`forearm.length.mm` <- round(animals_mb$`forearm.length.mm`, 1)

  # Tag mass: round to nearest 0.01 g
  animals_mb$`tag.mass.g` <- round(animals_mb$`tag.mass.g`, 2)

  # Reproductive condition: lowercase, standardize
  animals_mb$`animal.reproductive.condition` <- tolower(
    trimws(animals_mb$`animal.reproductive.condition`)
  )
  # Map shorthand values
  animals_mb$`animal.reproductive.condition` <- dplyr::case_when(
    animals_mb$`animal.reproductive.condition` == "nr" ~ "non-reproductive",
    TRUE ~ animals_mb$`animal.reproductive.condition`
  )

  # Tag attachment: classify into type (glue / collar) and standardize
  attach_raw <- tolower(trimws(animals_mb$`tag.attachment.raw`))
  animals_mb$`tag.attachment.type` <- dplyr::case_when(
    grepl("collar", attach_raw)                                       ~ "collar",
    grepl("sauer|kryolan|ks|glue|bond|adhesive|epiglue", attach_raw)  ~ "glue",
    is.na(attach_raw) | attach_raw == ""                              ~ NA_character_,
    TRUE                                                              ~ attach_raw
  )

  # ---- build deploy-on timestamp: date from date_col + HMS from deploy_time_col ----
  # The date column has the correct YMD. The deployment time column has the
  # correct HMS but may carry an incorrect date (e.g. Excel time-only fields
  # default to 1899-12-31). Extract HMS from the time column and paste onto
  # the correct date.
  date_vals <- safe_col(date_col)

  if (!is.null(deploy_time_col)) {
    time_raw <- animals[[deploy_time_col]]
    # Extract HH:MM:SS from whatever format the time column is in.
    # readxl imports Excel time-only cells as hms/difftime or POSIXct objects;
    # CSVs may have plain character strings.
    time_chr <- as.character(time_raw)

    # Extract HH:MM or HH:MM:SS pattern from the string representation
    hms_str <- stringr::str_extract(time_chr, "\\d{1,2}:\\d{2}(:\\d{2})?")

    # Pad missing seconds (e.g. "11:00" -> "11:00:00")
    hms_str <- ifelse(
      !is.na(hms_str) & !grepl(":\\d{2}:\\d{2}", hms_str),
      paste0(hms_str, ":00"),
      hms_str
    )

    # Combine: take YMD from date_vals, append HMS
    date_ymd <- as.character(lubridate::as_date(date_vals))
    animals_mb$Timestamp.release <- ifelse(
      !is.na(date_ymd) & !is.na(hms_str),
      paste(date_ymd, hms_str),
      ifelse(!is.na(date_ymd), paste(date_ymd, "12:00:00"), NA_character_)
    )
    message("[ref] Deploy-on timestamp includes HMS from '", deploy_time_col, "'.")
  } else {
    animals_mb$Timestamp.release <- as.character(date_vals)
    message("[ref] No deployment time column found; deploy-on timestamp is date-only.")
  }

  # ---- resolve firmware sampling config per tag ----
  fw_config <- if (!is.null(sampling_config)) sampling_config else get_default_sampling_config()

  animals_mb$tag_model_family <- match_tag_model(animals_mb$tag_model_raw)

  if (!is.null(force_tag_model_family)) {
    animals_mb$tag_model_family <- force_tag_model_family
  }
  if (!is.null(force_firmware_version)) {
    animals_mb$firmware_version <- force_firmware_version
  }

  # ---- map firmware aliases to canonical names ----
  # Some firmware version strings in reference data are internal names that
  # correspond to a known firmware schedule. Map them before the config join.
  firmware_aliases <- c(
    "spring2025bat"                          = "30Days",
    "TenDay"                                 = "10Day",
    # FineScalePressure variants seen in reference data
    "NanofoxFineScalePressure"               = "FineScalePressure",
    "NanoFoxFineScalePressure"               = "FineScalePressure",
    "nanofox_fine_scale_pressure"            = "FineScalePressure",
    "FineScalePressure30Days"                = "FineScalePressure",
    "NANOFOX FINE-SCALE PRESSURE 30 DAYS V"  = "FineScalePressure"
  )
  animals_mb$firmware_version <- ifelse(
    animals_mb$firmware_version %in% names(firmware_aliases),
    firmware_aliases[animals_mb$firmware_version],
    animals_mb$firmware_version
  )
  # Report any remappings
  remapped <- firmware_aliases[firmware_aliases %in% animals_mb$firmware_version]
  if (length(remapped) > 0) {
    message("[firmware] Remapped firmware aliases: ",
            paste(names(remapped), "->", remapped, collapse = ", "))
  }

  tag_fw_lookup <- animals_mb %>%
    dplyr::select(Tag.ID, tag_model_family, firmware_version) %>%
    distinct() %>%
    left_join(
      fw_config %>%
        filter(software_version != "all") %>%
        dplyr::select(
          tag_model, software_version, vedba_count,
          burst_duration_s, burst_rate_hz,
          sampling_interval_s, sampling_count, vedba_type
        ),
      by = c("tag_model_family" = "tag_model",
             "firmware_version" = "software_version")
    )

  model_defaults <- fw_config %>%
    filter(software_version == "all") %>%
    dplyr::select(
      tag_model,
      vedba_count_default = vedba_count,
      burst_duration_s_default = burst_duration_s,
      burst_rate_hz_default = burst_rate_hz,
      sampling_interval_s_default = sampling_interval_s,
      sampling_count_default = sampling_count,
      vedba_type_default = vedba_type
    )

  tag_fw_lookup <- tag_fw_lookup %>%
    left_join(model_defaults, by = c("tag_model_family" = "tag_model")) %>%
    mutate(
      vedba_count         = dplyr::coalesce(vedba_count, vedba_count_default),
      burst_duration_s    = dplyr::coalesce(burst_duration_s, burst_duration_s_default),
      burst_rate_hz       = dplyr::coalesce(burst_rate_hz, burst_rate_hz_default),
      sampling_interval_s = dplyr::coalesce(sampling_interval_s, sampling_interval_s_default),
      sampling_count      = dplyr::coalesce(sampling_count, sampling_count_default),
      vedba_type          = dplyr::coalesce(vedba_type, vedba_type_default)
    ) %>%
    dplyr::select(
      Tag.ID, tag_model_family, firmware_version,
      vedba_count, burst_duration_s, burst_rate_hz,
      sampling_interval_s, sampling_count, vedba_type
    )

  animals_mb <- animals_mb %>%
    left_join(tag_fw_lookup, by = c("Tag.ID", "tag_model_family", "firmware_version"))

  if (!is.null(force_vedba_count)) {
    animals_mb$vedba_count <- force_vedba_count
  }

  message("[firmware] Sampling config per model/firmware:")
  fw_summary <- animals_mb %>%
    group_by(tag_model_family, firmware_version) %>%
    summarise(
      n_tags = n(),
      vedba_count = first(vedba_count),
      .groups = "drop"
    )

  for (i in seq_len(nrow(fw_summary))) {
    r <- fw_summary[i, ]
    message(sprintf(
      "      %s / %s : %d tag(s), vedba_count = %s",
      r$tag_model_family, r$firmware_version, r$n_tags, as.character(r$vedba_count)
    ))
  }

  # ---- read data ----
  # Each CSV is harmonized and expanded INDIVIDUALLY before merging.
  # This ensures firmware-specific column names are resolved per-file,
  # preventing silent data loss when bind_rows creates NA columns for
  # names that don't match across firmware versions.

  read_and_expand_wc <- function(path) {
    raw <- data.table::fread(path) %>% as_tibble()
    raw <- harmonize_wc_columns(raw)
    wc_wide_to_mb_long(raw)
  }

  if (!is.null(movebank_csv_path)) {
    message("Reading CSV: ", movebank_csv_path)
    movebank_df <- read_and_expand_wc(movebank_csv_path)
  } else {
    message("Reading all WildCloud CSVs in: ", wc_path)
    csvs <- fs::dir_ls(wc_path, glob = "*.csv")
    if (length(csvs) == 0) stop("No CSV files found in wc_path: ", wc_path)

    message(sprintf("[IO] Found %d CSV file(s). Processing individually ...", length(csvs)))
    movebank_df <- purrr::map_dfr(csvs, function(f) {
      message("  ", basename(f))
      read_and_expand_wc(f)
    })
    message(sprintf("[IO] Merged result: %d rows, %d columns.",
                    nrow(movebank_df), ncol(movebank_df)))
  }

  # ---- normalize core columns ----
  if ("Time (UTC)" %in% names(movebank_df) && !"timestamp SF transmission" %in% names(movebank_df)) {
    movebank_df <- movebank_df %>% mutate(`timestamp SF transmission` = parse_wc_time_utc(`Time (UTC)`))
  }
  if ("Position" %in% names(movebank_df) && !("latitude [°]" %in% names(movebank_df))) {
    movebank_df <- split_position_wc(movebank_df)
  }

  # Coerce types if present
  if ("Device" %in% names(movebank_df)) movebank_df$Device <- as.character(movebank_df$Device)
  if ("Sequence Number" %in% names(movebank_df)) movebank_df$`Sequence Number` <- suppressWarnings(as.numeric(movebank_df$`Sequence Number`))
  if ("Link Quality" %in% names(movebank_df)) movebank_df$`Link Quality` <- suppressWarnings(as.numeric(movebank_df$`Link Quality`))
  if ("Country Code" %in% names(movebank_df)) movebank_df$`Country Code` <- suppressWarnings(as.numeric(movebank_df$`Country Code`))
  if ("max_rssi" %in% names(movebank_df)) movebank_df$max_rssi <- suppressWarnings(as.numeric(movebank_df$max_rssi))
  if ("Time start" %in% names(movebank_df)) movebank_df$`Time start` <- lubridate::as_datetime(movebank_df$`Time start`)
  if ("Time end" %in% names(movebank_df)) movebank_df$`Time end` <- lubridate::as_datetime(movebank_df$`Time end`)
  if ("VeDBA [m/s²]" %in% names(movebank_df)) movebank_df$`VeDBA [m/s²]` <- suppressWarnings(as.numeric(movebank_df$`VeDBA [m/s²]`))
  if ("temperature [°C]" %in% names(movebank_df)) movebank_df$`temperature [°C]` <- suppressWarnings(as.numeric(movebank_df$`temperature [°C]`))
  if ("pressure [mbar]" %in% names(movebank_df)) movebank_df$`pressure [mbar]` <- suppressWarnings(as.numeric(movebank_df$`pressure [mbar]`))
  # ---- optional lat/lon cleaning ----
  if (!is.null(clean_lat_range) || !is.null(clean_lon_range)) {
    lat_min <- if (!is.null(clean_lat_range)) min(clean_lat_range, na.rm = TRUE) else -Inf
    lat_max <- if (!is.null(clean_lat_range)) max(clean_lat_range, na.rm = TRUE) else  Inf
    lon_min <- if (!is.null(clean_lon_range)) min(clean_lon_range, na.rm = TRUE) else -Inf
    lon_max <- if (!is.null(clean_lon_range)) max(clean_lon_range, na.rm = TRUE) else  Inf

    movebank_df <- movebank_df %>%
      mutate(
        keep_row = ifelse(
          !is.na(`latitude [°]`) & !is.na(`longitude [°]`),
          `latitude [°]` >= lat_min & `latitude [°]` <= lat_max &
            `longitude [°]` >= lon_min & `longitude [°]` <= lon_max,
          TRUE
        )
      ) %>%
      filter(keep_row) %>%
      dplyr::select(-keep_row)
  }

  # ==========================================================================
  # DRIFT CORRECTION — estimated at TRANSMISSION level, applied to all rows
  # ==========================================================================
  # After wc_wide_to_mb_long(), each transmission fans out into ~12 rows
  # (location + 5 VeDBA + 5 temp + pressure + min_temp). Drift must be

  # estimated from inter-transmission intervals (one row per Device × receipt
  # time), then the predicted drift is joined back and used to scale every
  # row's window offsets from the receipt timestamp.
  #
  # Correction logic:
  #   offset_from_receipt = receipt_time - Time_xxx  (measured by drifting clock)
  #   corrected_Time_xxx  = receipt_time - offset * (1 + drift_predicted)
  # ==========================================================================

  drift_model <- NULL

  if (correct_drift) {
    message("[pipeline] Applying clock-drift correction ...")

    # ---- 1. Build transmission-level table (one row per Device × receipt) ----
    tx_level <- movebank_df %>%
      filter(!is.na(`timestamp SF transmission`)) %>%
      distinct(Device, `timestamp SF transmission`) %>%
      arrange(Device, `timestamp SF transmission`)

    # correct_time_drift needs start/end columns to compute window_duration_s.
    # At the transmission level the nominal window spans the full programmed
    # interval ending at receipt time.
    if (!is.null(programmed_interval)) {
      tx_level <- tx_level %>%
        mutate(
          `Time end`   = `timestamp SF transmission`,
          `Time start` = `timestamp SF transmission` - as.difftime(programmed_interval, units = "secs")
        )
    } else {
      # If inferring, use the lag to the previous transmission as the window
      tx_level <- tx_level %>%
        group_by(Device) %>%
        mutate(
          `Time end`   = `timestamp SF transmission`,
          `Time start` = dplyr::coalesce(
            lag(`timestamp SF transmission`),
            `timestamp SF transmission`
          )
        ) %>%
        ungroup()
    }

    # ---- 2. Optionally attach mean temperature per transmission ----
    if (!is.null(drift_temp_col) && drift_temp_col %in% names(movebank_df)) {
      temp_summary <- movebank_df %>%
        filter(!is.na(.data[[drift_temp_col]])) %>%
        group_by(Device, `timestamp SF transmission`) %>%
        summarise(!!drift_temp_col := mean(.data[[drift_temp_col]], na.rm = TRUE),
                  .groups = "drop")
      tx_level <- left_join(tx_level, temp_summary,
                            by = c("Device", "timestamp SF transmission"))
    }

    n_tx <- nrow(tx_level)
    message(sprintf("[pipeline] %d unique transmissions across %d tags.",
                    n_tx, n_distinct(tx_level$Device)))

    # ---- 3. Run drift correction at transmission level ----
    if (n_tx > 0) {
      tx_corrected <- correct_time_drift(
        data                = tx_level,
        tag_col             = "Device",
        timestamp_col       = "timestamp SF transmission",
        start_col           = "Time start",
        end_col             = "Time end",
        temp_col            = drift_temp_col,
        programmed_interval = programmed_interval,
        poly_degree         = drift_poly_degree,
        max_drift_filter    = drift_max_filter,
        min_obs_per_tag     = drift_min_obs
      )

      drift_model <- attr(tx_corrected, "drift_model")

      # ---- 4. Join drift_predicted back to the long table ----
      drift_lookup <- tx_corrected %>%
        dplyr::select(Device, `timestamp SF transmission`,
                      drift_predicted, drift_source)

      movebank_df <- movebank_df %>%
        left_join(drift_lookup,
                  by = c("Device", "timestamp SF transmission")) %>%
        mutate(drift_predicted = dplyr::coalesce(drift_predicted, 0))

      # ---- 5. Scale window offsets from receipt time ----
      # Each row's Time start / Time end were computed relative to the
      # receipt timestamp using the tag's (drifting) clock. A slow clock
      # (drift > 0) means more real time elapsed than the tag counted,
      # so offsets grow by (1 + drift).
      movebank_df <- movebank_df %>%
        mutate(
          .offset_end_s   = as.numeric(difftime(`timestamp SF transmission`,
                                                `Time end`, units = "secs")),
          .offset_start_s = as.numeric(difftime(`timestamp SF transmission`,
                                                `Time start`, units = "secs")),
          `Time end`   = `timestamp SF transmission` -
            as.difftime(.offset_end_s * (1 + drift_predicted), units = "secs"),
          `Time start` = `timestamp SF transmission` -
            as.difftime(.offset_start_s * (1 + drift_predicted), units = "secs")
        ) %>%
        dplyr::select(-.offset_end_s, -.offset_start_s)

      message(sprintf(
        "[pipeline] Drift correction applied. Mean drift: %+.4f%%",
        mean(drift_lookup$drift_predicted * 100, na.rm = TRUE)
      ))

      # ---- 6. Optional diagnostic plots ----
      if (drift_diagnostics) {
        message("[pipeline] Generating drift diagnostic plots ...")
        diag_plots <- plot_drift_diagnostics(
          corrected_data = tx_corrected,
          tag_col        = "Device",
          temp_col       = drift_temp_col
        )
        diag_dir <- file.path(output_dir, "drift_diagnostics")
        if (!fs::dir_exists(diag_dir)) fs::dir_create(diag_dir, recurse = TRUE)
        for (pname in names(diag_plots)) {
          ggplot2::ggsave(
            filename = file.path(diag_dir, paste0(pname, ".png")),
            plot     = diag_plots[[pname]],
            width    = 8, height = 5, dpi = 150
          )
        }
        message("[pipeline] Diagnostic plots saved to: ", diag_dir)
      }
    } else {
      message("[pipeline] No transmissions found; skipping drift correction.")
    }
  }

  # ---- base fields for derived Movebank columns ----
  movebank_base <- movebank_df %>%
    mutate(
      `tag ID` = Device,
      `Sigfox computed location radius` =
        if ("Radius (m) (Source/Status)" %in% names(.))
          as.numeric(stringr::str_extract(`Radius (m) (Source/Status)`, "^[0-9]+"))
      else NA_real_,
      `Sigfox computed location source` =
        if ("Radius (m) (Source/Status)" %in% names(.))
          as.numeric(stringr::str_match(`Radius (m) (Source/Status)`, "\\((\\d+)/")[, 2])
      else NA_real_,
      `Sigfox computed location status` =
        if ("Radius (m) (Source/Status)" %in% names(.))
          as.numeric(stringr::str_match(`Radius (m) (Source/Status)`, "/(\\d+)\\)")[, 2])
      else NA_real_,
      `Sigfox LQI`           = if ("LQI" %in% names(.)) LQI else NA_real_,
      `Sigfox link quality`  = if ("Link Quality" %in% names(.)) `Link Quality` else NA_real_,
      `Sigfox country`       = if ("Country Code" %in% names(.)) `Country Code` else NA_real_,
      `Sigfox base stations` = if ("Base Stations (ID, RSSI, Reps)" %in% names(.)) `Base Stations (ID, RSSI, Reps)` else NA_character_,
      `Sigfox payload`       = if ("Raw Data" %in% names(.)) as.character(`Raw Data`) else NA_character_,
      `max_rssi`             = if ("max_rssi" %in% names(.)) max_rssi else NA_real_,
      timestamp_str          = format(`timestamp SF transmission`, "%Y-%m-%d %H:%M:%OS3"),
      time_start_str         = if ("Time start" %in% names(.)) format(`Time start`, "%Y-%m-%d %H:%M:%OS3") else NA_character_,
      time_end_str           = if ("Time end" %in% names(.))   format(`Time end`,   "%Y-%m-%d %H:%M:%OS3") else NA_character_
    )

  # ---- locations ----
  loc_data <- movebank_base %>%
    filter(!is.na(`latitude [°]`), !is.na(`longitude [°]`)) %>%
    transmute(
      `tag ID`,
      `location lat`  = `latitude [°]`,
      `location long` = `longitude [°]`,
      timestamp       = timestamp_str,
      `sequence number` = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `max_rssi`,
      `sensor type` = "sigfox-geolocation"
    ) %>%
    dplyr::left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c(`tag ID` = "Tag.ID")
    )

  # ---- VeDBA ----
  # VeDBA values in WildCloud are SUMS that need context from the firmware config:
  #   - "windowed_sum": sum over a measurement window → divide by vedba_count
  #     to get per-burst average
  #   - "cumulative_daily": running total since tag deployment → daily VeDBA
  #     is the DIFFERENCE between consecutive days' totals, divided by
  #     vedba_count (1440 bursts/day) for per-burst average
  vedba_data <- movebank_base %>%
    filter(!is.na(`VeDBA [m/s²]`)) %>%
    transmute(
      `tag ID`,
      `VeDBA sum`       = `VeDBA [m/s²]`,
      timestamp         = timestamp_str,
      `start timestamp` = time_start_str,
      `end timestamp`   = time_end_str,
      `sequence number` = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `sensor type` = "acceleration"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID,
                                   tag_model_family, firmware_version,
                                   vedba_count, burst_duration_s, burst_rate_hz,
                                   sampling_interval_s, sampling_count, vedba_type),
      by = c("tag ID" = "Tag.ID")
    ) %>%
    mutate(
      `VeDBA avg` = ifelse(
        !is.na(vedba_count) & vedba_count > 0 & vedba_type == "windowed_sum",
        `VeDBA sum` / vedba_count,
        NA_real_
      )
    )

  # For cumulative_daily tags, VeDBA avg requires differencing consecutive
  # transmissions — flag these for downstream processing
  n_cumulative <- sum(vedba_data$vedba_type == "cumulative_daily", na.rm = TRUE)
  if (n_cumulative > 0) {
    message(sprintf(
      "[VeDBA] %d rows from cumulative-daily tags (TinyFoxBatt). VeDBA avg requires ",
      n_cumulative),
      "differencing consecutive transmissions — not computed automatically. ",
      "Use the VeDBA sum column with lag() for daily VeDBA."
    )
  }

  # Report VeDBA avg computation
  n_vedba    <- nrow(vedba_data)
  n_avg_ok   <- sum(!is.na(vedba_data$`VeDBA avg`))
  n_avg_miss <- n_vedba - n_avg_ok
  if (n_vedba > 0) {
    message(sprintf("[VeDBA] %d rows total. VeDBA avg computed for %d (%.0f%%).",
                    n_vedba, n_avg_ok, 100 * n_avg_ok / n_vedba))
    if (n_avg_miss > 0) {
      miss_models <- vedba_data %>%
        filter(is.na(`VeDBA avg`)) %>%
        distinct(tag_model_family, firmware_version)
      warning(sprintf(
        "[VeDBA] %d rows have NA VeDBA avg (unknown vedba_count). ", n_avg_miss),
        "Affected model/firmware: ",
        paste(sprintf("%s/%s", miss_models$tag_model_family, miss_models$firmware_version),
              collapse = ", "),
        ". Update sampling_config to fix.")
    }
  }

  # ---- Pressure ----
  bar_data <- movebank_base %>%
    mutate(
      .pressure_mbar = dplyr::coalesce(
        if ("pressure [mbar]" %in% names(.)) `pressure [mbar]` else NA_real_,
        if ("Min pressure of last 3 hrs (mbar)" %in% names(.)) suppressWarnings(as.numeric(`Min pressure of last 3 hrs (mbar)`)) else NA_real_
      )
    ) %>%
    filter(!is.na(.pressure_mbar)) %>%
    transmute(
      `tag ID`,
      `barometric pressure` = .pressure_mbar,
      timestamp             = timestamp_str,
      `start timestamp`     = time_start_str,
      `end timestamp`       = time_end_str,
      `sequence number`     = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `sensor type` = "barometer"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ---- Temperature (36-min bins) ----
  temp_data <- movebank_base %>%
    filter(!is.na(`temperature [°C]`)) %>%
    transmute(
      `tag ID`,
      `external temperature` = `temperature [°C]`,
      timestamp              = timestamp_str,
      `start timestamp`      = time_start_str,
      `end timestamp`        = time_end_str,
      `sequence number`      = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `sensor type` = "accessory-measurements"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ---- Min temp (range label) ----
  min_temp_data <- movebank_base %>%
    mutate(
      .min_temp_range = dplyr::coalesce(
        if ("min_temp_range [°C]" %in% names(.)) as.character(`min_temp_range [°C]`) else NA_character_,
        if ("Min temperature of last 3 hrs (°C)" %in% names(.)) as.character(`Min temperature of last 3 hrs (°C)`) else NA_character_,
        if ("Min temperature of last 3 hrs (temperature range °C)" %in% names(.)) as.character(`Min temperature of last 3 hrs (temperature range °C)`) else NA_character_
      ),
      # Treat literal "N/A", "NA", "" as true NA
      .min_temp_range = ifelse(
        is.na(.min_temp_range) | trimws(.min_temp_range) %in% c("", "N/A", "NA", "n/a"),
        NA_character_,
        .min_temp_range
      )
    ) %>%
    filter(!is.na(.min_temp_range)) %>%
    transmute(
      `tag ID`,
      `minimum temperature` = .min_temp_range,
      timestamp             = timestamp_str,
      `start timestamp`     = time_start_str,
      `end timestamp`       = time_end_str,
      `sequence number`     = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `sensor type` = "Derived"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ---- Max temp (FineScalePressure firmware only) ----
  max_temp_data <- movebank_base %>%
    mutate(
      .max_temp_range = dplyr::coalesce(
        if ("max_temp_range [°C]" %in% names(.)) as.character(`max_temp_range [°C]`) else NA_character_,
        if ("Max temperature of last 3 hrs (°C)" %in% names(.)) as.character(`Max temperature of last 3 hrs (°C)`) else NA_character_
      ),
      .max_temp_range = ifelse(
        is.na(.max_temp_range) | trimws(.max_temp_range) %in% c("", "N/A", "NA", "n/a"),
        NA_character_,
        .max_temp_range
      )
    ) %>%
    filter(!is.na(.max_temp_range)) %>%
    transmute(
      `tag ID`,
      `maximum temperature` = .max_temp_range,
      timestamp             = timestamp_str,
      `start timestamp`     = time_start_str,
      `end timestamp`       = time_end_str,
      `sequence number`     = `Sequence Number`,
      `Sigfox computed location radius`,
      `Sigfox computed location source`,
      `Sigfox computed location status`,
      `Sigfox LQI`,
      `Sigfox link quality`,
      `Sigfox country`,
      `Sigfox base stations`,
      `Sigfox payload`,
      `sensor type` = "Derived"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ---- deployment ----
  # Movebank reference data: includes ring and PIT marker IDs when available.
  # id_source tracks which cascade level produced each Animal.ID.
  # Sampling metadata from firmware config included for reproducibility.

  # ---- compute deploy.off.date for overlapping tag deployments ----
  # When the same tag ID is redeployed (e.g. tag recovered and reattached),
  # the previous deployment must end before the next one begins. We set
  # deploy.off.date to 1 second before the next deploy.on.date for that tag.
  # The final (or only) deployment of each tag gets no deploy.off.date.

  deploy_df <- animals_mb %>%
    transmute(
      tag.id               = Tag.ID,
      animal.taxon         = Species,
      animal.life.stage    = `animal.life.stage`,
      animal.sex           = `animal.sex`,
      animal.mass          = `Weight..g.`,
      animal.forearm.length = `forearm.length.mm`,
      animal.reproductive.condition = `animal.reproductive.condition`,
      deploy.on.date       = Timestamp.release,
      animal.id            = Animal.ID,
      animal.ring.id       = Ring.ID,
      animal.marker.id     = PIT.ID,
      animal.id.source     = id_source,
      tag.model            = tag_model_raw,
      tag.model.family     = tag_model_family,
      tag.firmware         = firmware_version,
      tag.mass             = `tag.mass.g`,
      tag.attachment.type  = `tag.attachment.type`,
      tag.attachment.detail = `tag.attachment.raw`,
      vedba.count          = vedba_count,
      vedba.type           = vedba_type,
      burst.duration.s     = burst_duration_s,
      burst.rate.hz        = burst_rate_hz,
      sampling.interval.s  = sampling_interval_s,
      sampling.count       = sampling_count,
      Deploy.On.Latitude,
      Deploy.On.Longitude,
      Movebank.Project
    )

  # Parse deploy.on.date as POSIXct for comparison
  deploy_df$.deploy_on_ts <- lubridate::ymd_hms(deploy_df$deploy.on.date, quiet = TRUE)
  # Fallback: try date-only
  no_ts <- is.na(deploy_df$.deploy_on_ts)
  if (any(no_ts)) {
    deploy_df$.deploy_on_ts[no_ts] <- lubridate::ymd(
      deploy_df$deploy.on.date[no_ts], quiet = TRUE
    )
  }

  # ---- remove duplicate deployments (same tag + same individual = retrap) ----
  # When the same tag is recaptured on the same individual, the second row is
  # not a real new deployment — the tag was never moved to a different animal.
  # Keep only the first (earliest) deployment per tag+animal combination.
  deploy_df <- deploy_df %>%
    arrange(tag.id, .deploy_on_ts) %>%
    group_by(tag.id, animal.id) %>%
    mutate(.deploy_rank = row_number()) %>%
    ungroup()

  retrap_dupes <- deploy_df %>% filter(.deploy_rank > 1)
  if (nrow(retrap_dupes) > 0) {
    message(sprintf(
      "[deploy] Removed %d duplicate deployment(s) (same tag + same individual retrap):",
      nrow(retrap_dupes)))
    for (i in seq_len(min(nrow(retrap_dupes), 10))) {
      r <- retrap_dupes[i, ]
      message(sprintf("      tag %s, animal %s: duplicate on %s (keeping first deployment)",
                      r$tag.id, r$animal.id, r$deploy.on.date))
    }
    if (nrow(retrap_dupes) > 10)
      message("      ... and ", nrow(retrap_dupes) - 10, " more")
  }

  deploy_df <- deploy_df %>%
    filter(.deploy_rank == 1) %>%
    dplyr::select(-.deploy_rank)

  # ---- compute deploy.off.date for remaining overlapping tag deployments ----
  # When the same tag is redeployed on a DIFFERENT individual, the previous
  # deployment must end before the next one begins. Set deploy.off.date to
  # 1 second before the next deploy.on.date for that tag.
  deploy_df <- deploy_df %>%
    arrange(tag.id, .deploy_on_ts) %>%
    group_by(tag.id) %>%
    mutate(
      .next_deploy_on = lead(.deploy_on_ts),
      deploy.off.date = ifelse(
        !is.na(.next_deploy_on),
        format(.next_deploy_on - 1, "%Y-%m-%d %H:%M:%S"),
        NA_character_
      )
    ) %>%
    ungroup()

  # Report overlapping deployments
  overlaps <- deploy_df %>%
    filter(!is.na(deploy.off.date))
  if (nrow(overlaps) > 0) {
    message(sprintf(
      "[deploy] %d tag redeployment(s) on different individuals — deploy.off.date set:",
      nrow(overlaps)))
    for (i in seq_len(min(nrow(overlaps), 10))) {
      r <- overlaps[i, ]
      message(sprintf("      tag %s: %s deployed on %s, off %s (next deploy: %s)",
                      r$tag.id, r$animal.id, r$deploy.on.date,
                      r$deploy.off.date,
                      format(r$.next_deploy_on, "%Y-%m-%d %H:%M:%S")))
    }
    if (nrow(overlaps) > 10)
      message("      ... and ", nrow(overlaps) - 10, " more")
  }

  # Clean up and reorder deploy.off.date next to deploy.on.date
  deployment_data <- deploy_df %>%
    dplyr::select(
      tag.id, animal.taxon, animal.life.stage, animal.sex, animal.mass,
      animal.forearm.length, animal.reproductive.condition,
      deploy.on.date, deploy.off.date,
      animal.id, animal.ring.id, animal.marker.id, animal.id.source,
      tag.model, tag.model.family, tag.firmware,
      tag.mass, tag.attachment.type, tag.attachment.detail,
      vedba.count, vedba.type,
      burst.duration.s, burst.rate.hz, sampling.interval.s, sampling.count,
      Deploy.On.Latitude, Deploy.On.Longitude, Movebank.Project
    )

  # ---- write ----
  write_movebank_upload_csvs(
    loc_data, vedba_data, bar_data, temp_data, min_temp_data, deployment_data,
    max_temp_data = max_temp_data,
    output_dir = output_dir
  )

  result <- list(
    movebank_df     = movebank_df,
    animals_mb      = animals_mb,
    loc_data        = loc_data,
    vedba_data      = vedba_data,
    bar_data        = bar_data,
    temp_data       = temp_data,
    min_temp_data   = min_temp_data,
    max_temp_data   = max_temp_data,
    deployment_data = deployment_data,
    sampling_config = fw_config
  )

  # Attach drift model to output if correction was applied
  if (!is.null(drift_model)) {
    attr(result, "drift_model") <- drift_model
  }

  invisible(result)
}


# =============================================================================
# Example usage
# =============================================================================
if (FALSE) {
  # Source the drift correction module
  source("./R/correct_time_drift.R")

  # ---- Without drift correction (original behavior) ----
  result <- wildcloud_to_movebank(
    wc_path               = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/wildcloud/",
    animals_path          = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss_MPIAB_captures.xlsx",
    output_dir            = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/movebank/",
    force_tag_model_family = "NanoFox",
    force_firmware_version = "30Days",
    force_vedba_count = 504,
    location_abbr         = "Swiss",  # used in fallback Animal.ID, e.g. Nnoc25_01_Swiss_9EC016
    movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Thurgau, Switzerland"
  )

  # ---- With drift correction ----
  result <- wildcloud_to_movebank(
    wc_path               = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/wildcloud/",
    animals_path          = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss_MPIAB_captures.xlsx",
    output_dir            = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/movebank/",
    force_tag_model_family = "NanoFox",
    force_firmware_version = "30Days",
    force_vedba_count = 504,
    location_abbr         = "Swiss",
    movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Thurgau, Switzerland",
    correct_drift         = TRUE,
    programmed_interval   = 3 * 3600,       # 3-hour schedule; NULL to infer per tag
    drift_temp_col        = "temperature [°C]", # or NULL to skip temperature term
    drift_diagnostics     = TRUE            # saves diagnostic plots to output_dir
  )

  # Inspect the drift model
  drift_model <- attr(result, "drift_model")
  summary(drift_model)

  # ---- Custom sampling config ----
  # If the defaults don't cover your firmware, extend or replace the config.
  # Check result$sampling_config to see the default table.
  # The function matches tag_model + software_version from the reference data.
  # Rows with software_version = "all" are used as model-level fallbacks.
  # Note: firmware aliases like "spring2025bat" -> "30Days" are applied
  # automatically before the config lookup.
  my_config <- tibble::tribble(
    ~tag_model,  ~software_version,  ~vedba_count, ~burst_duration_s, ~burst_rate_hz, ~sampling_interval_s, ~sampling_count, ~vedba_type,          ~notes,
    "NanoFox",   "30Days",            504,          1.0,               28,             120,                  18,              "windowed_sum",       "1s burst @ 28Hz, every 2min, 36min window",
    "TinyFox",   "all",               1440,         1.0,               28,             60,                   1440,            "cumulative_daily",   "1s burst @ 28Hz, every 60s"
  )
  result <- wildcloud_to_movebank(
    wc_path               = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/wildcloud/",
    animals_path          = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss_MPIAB_captures.xlsx",
    output_dir            = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/movebank/",
    movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Thurgau, Switzerland",
    location_abbr         = "Swiss",
    sampling_config       = my_config
  )
}
