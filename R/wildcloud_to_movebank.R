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


# =============================================================================
# 1) Expand NEW Wildcloud wide schema into a long table
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

  min_temp_col <- "Min temperature of last 3 hrs (temperature range °C)"
  min_pres_col <- "Min pressure of last 3 hrs (mbar)"

  has_new <- length(vedba_cols) > 0 || length(temp_cols) > 0 ||
    (min_temp_col %in% names(df)) || (min_pres_col %in% names(df))

  if (!has_new) return(df)

  df <- df %>%
    mutate(
      `timestamp SF transmission` = parse_wc_time_utc(`Time (UTC)`),
      `Sequence Number` = suppressWarnings(as.numeric(`Sequence Number`)),
      `Link Quality`    = suppressWarnings(as.numeric(`Link Quality`)),
      `Country Code`    = suppressWarnings(as.numeric(`Country Code`))
    ) %>%
    split_position_wc()

  base_cols <- intersect(
    c("Device", "Time (UTC)", "Raw Data", "Position",
      "Radius (m) (Source/Status)", "Sequence Number",
      "LQI", "Link Quality", "Operator Name", "Country Code",
      "Base Stations (ID, RSSI, Reps)", "Compression",
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

  vedba_long <- build_36min_long(vedba_cols, "VeDBA [m/s²]")
  temp_long  <- build_36min_long(temp_cols,  "temperature [°C]")

  if (!is.null(vedba_long)) vedba_long$`VeDBA [m/s²]`    <- suppressWarnings(as.numeric(vedba_long$`VeDBA [m/s²]`))
  if (!is.null(temp_long))  temp_long$`temperature [°C]`  <- suppressWarnings(as.numeric(temp_long$`temperature [°C]`))

  three_hr_base <- base %>%
    mutate(
      `Time start` = `timestamp SF transmission` - as.difftime(180, units = "mins"),
      `Time end`   = `timestamp SF transmission`
    )

  pres_long <- NULL
  if (min_pres_col %in% names(df)) {
    pres_long <- three_hr_base %>%
      mutate(`pressure [mbar]` = suppressWarnings(as.numeric(df[[min_pres_col]]))) %>%
      dplyr::select(-.row_id)
  }

  min_temp_long <- NULL
  if (min_temp_col %in% names(df)) {
    min_temp_long <- three_hr_base %>%
      mutate(`min_temp_range [°C]` = as.character(df[[min_temp_col]])) %>%
      dplyr::select(-.row_id)
  }

  # Location rows
  loc_rows <- base %>%
    mutate(
      `Time start` = `timestamp SF transmission`,
      `Time end`   = `timestamp SF transmission`,
      mb_record_type = "location"
    ) %>%
    dplyr::select(-.row_id)

  # FIX: NULL-safe bind_rows — only select -.row_id when non-NULL
  out <- dplyr::bind_rows(
    loc_rows,
    if (!is.null(vedba_long)) vedba_long %>% mutate(mb_record_type = "vedba")       %>% dplyr::select(-.row_id),
    if (!is.null(temp_long))  temp_long  %>% mutate(mb_record_type = "temperature")  %>% dplyr::select(-.row_id),
    if (!is.null(pres_long))  pres_long  %>% mutate(mb_record_type = "pressure"),
    if (!is.null(min_temp_long)) min_temp_long %>% mutate(mb_record_type = "min_temp_range")
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

    readr::write_csv(loc_proj,      file.path(project_folder, "data.csv"))
    readr::write_csv(vedba_proj,    file.path(project_folder, "dataVeDBA.csv"))
    readr::write_csv(bar_proj,      file.path(project_folder, "dataBar.csv"))
    readr::write_csv(temp_proj,     file.path(project_folder, "dataTemp.csv"))
    readr::write_csv(min_temp_proj, file.path(project_folder, "dataMinTemp.csv"))
    readr::write_csv(dep_proj,      file.path(project_folder, "deployment.csv"))

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
    clean_lat_range      = NULL,
    clean_lon_range      = NULL,
    # ---- drift correction arguments ----
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
  animals <- read.csv(animals_path, header = TRUE, sep = ",", stringsAsFactors = FALSE)

  # FIX: species fallback now uses animal.taxon from the CSV instead of no-op
  animals_mb <- animals %>%
    mutate(
      Tag.ID              = tag.id,
      Animal.ID           = animal.id,
      Species             = if (!is.null(species)) species else animal.taxon,
      `animal.life.stage` = if (!is.null(life_stage)) life_stage else animal.life.stage,
      `Weight..g.`        = animal.mass,
      Deploy.On.Latitude  = capture.latitude,
      Deploy.On.Longitude = capture.longitude,
      Timestamp.release   = deploy.on.date,
      Movebank.Project    = movebank_project_name
    )

  # ---- read data ----
  if (!is.null(movebank_csv_path)) {
    message("Reading CSV: ", movebank_csv_path)
    movebank_df <- data.table::fread(movebank_csv_path) %>% as_tibble()
    movebank_df <- wc_wide_to_mb_long(movebank_df)
  } else {
    message("Reading all WildCloud CSVs in: ", wc_path)
    csvs <- fs::dir_ls(wc_path, glob = "*.csv")
    if (length(csvs) == 0) stop("No CSV files found in wc_path: ", wc_path)

    movebank_df <- purrr::map_dfr(csvs, ~ data.table::fread(.x) %>% as_tibble())
    movebank_df <- wc_wide_to_mb_long(df = movebank_df)
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
      `Sigfox LQI`          = if ("LQI" %in% names(.)) LQI else NA_real_,
      `Sigfox link quality` = if ("Link Quality" %in% names(.)) `Link Quality` else NA_real_,
      `Sigfox country`      = if ("Country Code" %in% names(.)) `Country Code` else NA_real_,
      timestamp_str         = format(`timestamp SF transmission`, "%Y-%m-%d %H:%M:%OS3"),
      time_start_str        = if ("Time start" %in% names(.)) format(`Time start`, "%Y-%m-%d %H:%M:%OS3") else NA_character_,
      time_end_str          = if ("Time end" %in% names(.))   format(`Time end`,   "%Y-%m-%d %H:%M:%OS3") else NA_character_
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
      `sensor type` = "sigfox-geolocation"
    ) %>%
    dplyr::left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c(`tag ID` = "Tag.ID")
    )

  # ---- VeDBA ----
  vedba_data <- movebank_base %>%
    filter(!is.na(`VeDBA [m/s²]`)) %>%
    transmute(
      `tag ID`,
      VeDBA             = `VeDBA [m/s²]`,
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
      `sensor type` = "acceleration"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

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
        if ("min_temp_range [°C]" %in% names(.)) `min_temp_range [°C]` else NA_character_,
        if ("Min temperature of last 3 hrs (temperature range °C)" %in% names(.)) as.character(`Min temperature of last 3 hrs (temperature range °C)`) else NA_character_
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
      `sensor type` = "Derived"
    ) %>%
    left_join(
      animals_mb %>% dplyr::select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ---- deployment ----
  # FIX: use the cleaned Movebank-ready column names from animals_mb
  deployment_data <- animals_mb %>%
    transmute(
      tag.id               = Tag.ID,
      animal.taxon         = Species,
      animal.life.stage    = `animal.life.stage`,
      animal.mass          = `Weight..g.`,
      deploy.on.date       = Timestamp.release,
      animal.id            = Animal.ID,
      Deploy.On.Latitude,
      Deploy.On.Longitude,
      Movebank.Project
    )

  # ---- write ----
  write_movebank_upload_csvs(
    loc_data, vedba_data, bar_data, temp_data, min_temp_data, deployment_data,
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
    deployment_data = deployment_data
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
    wc_path               = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/wildcloud/",
    animals_path          = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/belgium-reference-data.csv",
    output_dir            = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/movebank/",
    movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Flanders"
  )

  # ---- With drift correction ----
  result <- wildcloud_to_movebank(
    wc_path               = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/wildcloud/",
    animals_path          = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/belgium-reference-data.csv",
    output_dir            = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/movebank/",
    movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Flanders",
    correct_drift         = TRUE,
    programmed_interval   = 3 * 3600,       # 3-hour schedule; NULL to infer per tag
    drift_temp_col        = "temperature [°C]", # or NULL to skip temperature term
    drift_diagnostics     = TRUE            # saves diagnostic plots to output_dir
  )

  # Inspect the drift model
  drift_model <- attr(result, "drift_model")
  summary(drift_model)
}
