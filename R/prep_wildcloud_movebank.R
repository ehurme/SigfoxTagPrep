prep_sigfox_movebank <- function(
    wc_path              = NULL,  # directory with WildCloud CSVs
    movebank_csv_path    = NULL,  # alternative: existing movebank-style CSV OR raw wildcloud export CSV
    animals_path,
    output_dir,
    movebank_project_name,
    location_name,
    species              = NULL,
    life_stage           = NULL,
    clean_lat_range      = NULL,  # e.g. c(30, 60) or NULL to skip
    clean_lon_range      = NULL   # e.g. c(0, 30) or NULL to skip
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(lubridate)
    library(readr)
    library(fs)
    library(data.table)
    library(janitor)
    library(purrr)
  })

  # ------------------------------------------------------------------
  # 0. Sanity checks on inputs
  # ------------------------------------------------------------------
  if (is.null(wc_path) && is.null(movebank_csv_path)) {
    stop("Provide either 'wc_path' (WildCloud directory) or 'movebank_csv_path' (existing movebank-style CSV).")
  }
  if (!is.null(wc_path) && !dir_exists(wc_path)) {
    stop("wc_path does not exist: ", wc_path)
  }
  if (!is.null(movebank_csv_path) && !file.exists(movebank_csv_path)) {
    stop("movebank_csv_path does not exist: ", movebank_csv_path)
  }
  if (!file.exists(animals_path)) {
    stop("animals_path does not exist: ", animals_path)
  }
  if (!dir_exists(output_dir)) dir_create(output_dir)

  # ------------------------------------------------------------------
  # helpers
  # ------------------------------------------------------------------
  make_animal_ids <- function(df,
                              genus_col,
                              species_col,
                              tagid_col,
                              datetime_col,
                              location_name,
                              counter_pad = 2,
                              tz = "UTC") {
    suppressPackageStartupMessages({
      library(dplyr)
      library(stringr)
      library(lubridate)
    })

    stopifnot(
      genus_col %in% names(df),
      species_col %in% names(df),
      tagid_col %in% names(df),
      datetime_col %in% names(df)
    )

    # --- species abbreviation: 1st letter genus + first 3 letters species
    species_abbrev <- function(genus, species) {
      paste0(
        str_to_title(str_sub(genus, 1, 1)),
        str_to_lower(str_sub(species, 1, 3))
      )
    }

    location_name <- location_name |>
      tolower() |>
      str_replace_all("\\s+", "_") |>
      str_replace_all("[^a-z0-9_\\-]", "")

    df$location_name <- location_name

    df %>%
      mutate(
        genus_chr   = as.character(.data[[genus_col]]),
        species_chr = as.character(.data[[species_col]]),
        tag_id_chr  = as.character(.data[[tagid_col]]),

        # parse datetime robustly
        dt = as_datetime(.data[[datetime_col]], tz = tz),

        year_yy = format(dt, "%y"),

        species_abbrev = species_abbrev(genus_chr, species_chr)
      ) %>%
      arrange(species_abbrev, dt, tag_id_chr) %>%
      group_by(species_abbrev) %>%
      mutate(
        tag_index = row_number(),
        tag_index_str = str_pad(tag_index, width = counter_pad, pad = "0"),
        Animal.ID = paste0(
          species_abbrev,
          year_yy,
          "_",
          location_name,
          "_",
          tag_index_str,
          "_",
          tag_id_chr
        )
      ) %>%
      ungroup() %>%
      select(-genus_chr, -species_chr, -tag_id_chr, -dt)
  }

  parse_wc_time_utc <- function(x) {
    # handles "14.06.2025, 17:56:24"
    x <- as.character(x)
    x <- gsub(",", "", x)
    lubridate::dmy_hms(x, tz = "UTC")
  }

  split_position <- function(df) {
    if (!"Position" %in% names(df)) return(df)
    parts <- stringr::str_split_fixed(df$Position, ",\\s*", 2)
    df$`latitude [°]`  <- suppressWarnings(as.numeric(parts[, 1]))
    df$`longitude [°]` <- suppressWarnings(as.numeric(parts[, 2]))
    df
  }

  # Expand NEW wide schema into long Movebank-style dpl
  expand_new_wide_schema <- function(df) {
    # Requires "Time (UTC)" and at least one of the new VeDBA columns
    if (!("Time (UTC)" %in% names(df))) return(df)

    vedba_cols <- names(df)[str_detect(names(df), "^VeDBA sum\\s+\\d+\\s+min ago")]
    temp_cols  <- names(df)[str_detect(names(df), "^Average temperature\\s+\\d+\\s+min ago")]

    has_new <- length(vedba_cols) > 0 || length(temp_cols) > 0 ||
      ("Min pressure of last 3 hrs (mbar)" %in% names(df)) ||
      ("Min temperature of last 3 hrs (temperature range °C)" %in% names(df))

    if (!has_new) return(df)

    # Core time
    df <- df %>%
      mutate(
        `timestamp SF transmission` = parse_wc_time_utc(`Time (UTC)`),
        `Sequence Number` = suppressWarnings(as.numeric(`Sequence Number`)),
        `Link Quality`    = suppressWarnings(as.numeric(`Link Quality`)),
        `Country Code`    = suppressWarnings(as.numeric(`Country Code`))
      )

    df <- split_position(df)

    # Build “base” (metadata) columns to repeat
    base_cols <- intersect(
      c("Device","Time (UTC)","Raw Data","Position","Radius (m) (Source/Status)","Sequence Number",
        "LQI","Link Quality","Operator Name","Country Code","Base Stations (ID, RSSI, Reps)","Compression",
        "timestamp SF transmission","latitude [°]","longitude [°]"),
      names(df)
    )

    base <- df[, base_cols, drop = FALSE]

    # ---- 5-sample streams (36 min bins): VeDBA + Avg Temp ----
    # We interpret “X min ago” as the END of a 36-min window.
    build_36min_long <- function(value_cols, value_name) {
      if (length(value_cols) == 0) return(NULL)

      long <- base %>%
        select(all_of(base_cols)) %>%
        mutate(.row_id = row_number()) %>%
        left_join(
          df %>%
            mutate(.row_id = row_number()) %>%
            select(.row_id, all_of(value_cols)),
          by = ".row_id"
        ) %>%
        pivot_longer(
          cols = all_of(value_cols),
          names_to = "metric",
          values_to = value_name
        ) %>%
        mutate(
          minutes_ago = suppressWarnings(as.numeric(str_extract(metric, "\\d+"))),
          .end = `timestamp SF transmission` - as.difftime(minutes_ago, units = "mins"),
          `Time end`   = .end,
          `Time start` = .end - as.difftime(36, units = "mins")
        ) %>%
        select(-metric, -minutes_ago, -.end, -.row_id)

      long
    }

    vedba_long <- build_36min_long(vedba_cols, "VeDBA [m/s²]")
    temp_long  <- build_36min_long(temp_cols,  "temperature [°C]")

    # Coerce numerics
    if (!is.null(vedba_long)) vedba_long$`VeDBA [m/s²]` <- suppressWarnings(as.numeric(vedba_long$`VeDBA [m/s²]`))
    if (!is.null(temp_long))  temp_long$`temperature [°C]` <- suppressWarnings(as.numeric(temp_long$`temperature [°C]`))

    # ---- 3-hour summaries: min temp (categorical) and min pressure (numeric) ----
    three_hr <- base %>%
      mutate(
        `Time start` = `timestamp SF transmission` - as.difftime(180, units = "mins"),
        `Time end`   = `timestamp SF transmission`
      )

    min_temp_col <- "Min temperature of last 3 hrs (temperature range °C)"
    min_pres_col <- "Min pressure of last 3 hrs (mbar)"

    min_temp_long <- NULL
    if (min_temp_col %in% names(df)) {
      min_temp_long <- three_hr %>%
        mutate(`min_temp [°C]` = as.character(df[[min_temp_col]])) %>%
        select(all_of(base_cols), `timestamp SF transmission`, `Time start`, `Time end`, `min_temp [°C]`)
    }

    pres_long <- NULL
    if (min_pres_col %in% names(df)) {
      pres_long <- three_hr %>%
        mutate(`pressure [mbar]` = suppressWarnings(as.numeric(df[[min_pres_col]]))) %>%
        select(all_of(base_cols), `timestamp SF transmission`, `Time start`, `Time end`, `pressure [mbar]`)
    }

    # Combine into one dpl-like data frame:
    # We keep columns that exist and allow NAs for the rest (like your wc pipeline output)
    out <- bind_rows(
      vedba_long,
      temp_long,
      min_temp_long,
      pres_long,
      # also keep “location rows” (Time start=end=transmission)
      base %>%
        mutate(`Time start` = `timestamp SF transmission`,
               `Time end`   = `timestamp SF transmission`) %>%
        select(all_of(base_cols), `timestamp SF transmission`, `Time start`, `Time end`)
    ) %>%
      distinct(Device, `timestamp SF transmission`, `Time start`, `Time end`,
               `latitude [°]`, `longitude [°]`, .keep_all = TRUE)

    out
  }

  # ------------------------------------------------------------------
  # 1. Read capture sheet and prepare animals table (robust)
  # ------------------------------------------------------------------

  animals <- data.table::fread(animals_path) %>%
    as_tibble() %>%
    janitor::clean_names()

  # ---- helpers ----

  # return first existing column among candidates; else NULL
  pick_col <- function(df, candidates) {
    candidates <- janitor::make_clean_names(candidates)
    hit <- candidates[candidates %in% names(df)][1]
    if (is.na(hit)) return(NULL)
    hit
  }

  # coalesce values from multiple possible columns; returns vector length nrow(df)
  coalesce_cols <- function(df, candidates, default = NA) {
    cols <- janitor::make_clean_names(candidates)
    cols <- cols[cols %in% names(df)]
    if (length(cols) == 0) return(rep(default, nrow(df)))
    out <- df[[cols[1]]]
    if (length(cols) > 1) {
      for (k in cols[-1]) out <- dplyr::coalesce(out, df[[k]])
    }
    out
  }

  as_chr <- function(x) {
    x <- as.character(x)
    x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
    x
  }

  as_num <- function(x) suppressWarnings(as.numeric(as.character(x)))

  # if time missing -> "12:00:00"
  fill_time_noon <- function(x) {
    x <- as_chr(x)
    x <- ifelse(is.na(x) | !nzchar(str_trim(x)), "12:00:00", str_trim(x))
    x
  }

  # accepts date-only, datetime, or separate date+time; always returns POSIXct (or NA if date missing)
  parse_dt_flex <- function(date_str, time_str = NULL, tz = "UTC") {
    date_str <- as_chr(date_str)

    # If a full datetime is already in date_str, just parse it.
    # Otherwise, combine date + time (with noon fallback)
    if (is.null(time_str)) {
      dt_str <- date_str
      dt_str <- ifelse(is.na(dt_str), NA_character_, str_trim(dt_str))
    } else {
      time_str <- fill_time_noon(time_str)
      dt_str <- ifelse(
        is.na(date_str) | !nzchar(str_trim(date_str)),
        NA_character_,
        paste0(str_trim(date_str), " ", time_str)
      )
    }

    suppressWarnings(lubridate::parse_date_time(
      dt_str,
      orders = c(
        # common mixed formats
        "Y-m-d H:M:S", "Y-m-d H:M",
        "d/m/Y H:M:S", "d/m/Y H:M",
        "d-m-Y H:M:S", "d-m-Y H:M",
        "m/d/Y H:M:S", "m/d/Y H:M",
        # date-only
        "Y-m-d", "d/m/Y", "d-m-Y", "m/d/Y"
      ),
      tz = tz
    ))
  }

  fmt_mb_time <- function(dt) {
    ifelse(is.na(dt), NA_character_, format(as_datetime(dt), "%Y-%m-%d %H:%M:%OS3"))
  }

  # ---- resolve key fields with synonyms ----

  tag_id <- as_chr(coalesce_cols(animals, c("tag_id", "tag", "tagid", "device_id", "tag_identifier")))
  # animal_id <- as_chr(coalesce_cols(animals, c("animal_id", "animalid", "individual_id", "id", "id_alternate", "ring_id", "ring_number")))
  genus <- as_chr(coalesce_cols(animals, c("genus")))
  species_raw <- as_chr(coalesce_cols(animals, c("species", "specific_epithet")))

  # Build Species string for Movebank: prefer user-provided `species` argument if not NULL,
  # else use genus+species if both present, else species column
  species_mb <- if (!is.null(species)) {
    species
  } else if (!all(is.na(genus)) && !all(is.na(species_raw))) {
    str_trim(paste(genus, species_raw))
  } else {
    species_raw
  }

  life_stage_mb <- if (!is.null(life_stage)) life_stage else as_chr(coalesce_cols(animals, c("animal_life_stage", "life_stage", "age_class", "age")))

  mass_mb <- as_num(coalesce_cols(animals, c("mass_w_tag_g", "mass_g", "weight_g", "weight_bat", "weight")))
  deploy_lat <- as_num(coalesce_cols(animals, c("release_latitude", "deploy_on_latitude", "deploy_latitude", "latitude", "latitude_ycoord")))
  deploy_lon <- as_num(coalesce_cols(animals, c("release_longitude", "deploy_on_longitude", "deploy_longitude", "longitude", "longitude_xcoord")))

  # Release / tagging timestamp: accept many possibilities
  release_dt <- parse_dt_flex(
    date_str = coalesce_cols(animals, c("date_released", "release_date", "deploy_on_date", "date_tagged", "tagging_date", "capture_date")),
    time_str = coalesce_cols(animals, c("release_time", "deploy_on_time", "tag_deployment_time", "tagging_time", "capture_time")),
    tz = "Europe/Berlin"   # keep your original behavior here
  )

  # ---- final animals_mb ----
  animals_mb <- animals %>%
    mutate(
      Tag.ID = tag_id,
      # Animal.ID = dplyr::coalesce(animal_id, tag_id),  # fallback: at least something stable
      Species = species_mb,
      `animal.life.stage` = life_stage_mb,
      `Weight..g.` = mass_mb,
      `Deploy.On.Latitude` = deploy_lat,
      `Deploy.On.Longitude` = deploy_lon,
      `Timestamp.release` = fmt_mb_time(release_dt),
      Movebank.Project = movebank_project_name
    )

  animals_mb <- animals_mb %>%
    make_animal_ids(
      genus_col     = "genus",
      species_col   = "species",
      tagid_col     = "Tag.ID",
      datetime_col  = "Timestamp.release",
      location_name = location_name,
      counter_pad   = 2,
      tz            = "UTC"
    )

  # (optional) keep only rows with Tag.ID present
  # animals_mb <- animals_mb %>% filter(!is.na(Tag.ID) & nzchar(Tag.ID))

  # ------------------------------------------------------------------
  # 2. Get movebank_df
  # ------------------------------------------------------------------
  if (!is.null(movebank_csv_path)) {
    message("Reading CSV: ", movebank_csv_path)
    movebank_df <- data.table::fread(movebank_csv_path) %>% as_tibble()
    # If it's raw wildcloud wide schema, expand it
    movebank_df <- expand_new_wide_schema(movebank_df)
  } else {
    message("Running wc_multicsv_to_dpl() on: ", wc_path)
    source("./R/wildcloud_nanofox30d_to_movebank.R")  # must provide wc_multicsv_to_dpl
    movebank_df <- wc_multicsv_to_dpl(
      wc_path,
      raw2physical      = TRUE,
      norm_multisamples = TRUE
    ) %>% as_tibble()
  }

  # ------------------------------------------------------------------
  # 3. Ensure expected columns / types (supports both schemas)
  # ------------------------------------------------------------------
  # Prefer consistent naming downstream:
  if ("timestamp_sf" %in% names(movebank_df) && !"timestamp SF transmission" %in% names(movebank_df)) {
    movebank_df <- movebank_df %>% mutate(`timestamp SF transmission` = lubridate::as_datetime(timestamp_sf))
  }
  if ("Time (UTC)" %in% names(movebank_df) && !"timestamp SF transmission" %in% names(movebank_df)) {
    movebank_df <- movebank_df %>% mutate(`timestamp SF transmission` = parse_wc_time_utc(`Time (UTC)`))
  }

  # Coerce common columns if present
  if ("Device" %in% names(movebank_df)) movebank_df$Device <- as.character(movebank_df$Device)
  if ("Sequence Number" %in% names(movebank_df)) movebank_df$`Sequence Number` <- suppressWarnings(as.numeric(movebank_df$`Sequence Number`))
  if ("Link Quality" %in% names(movebank_df)) movebank_df$`Link Quality` <- suppressWarnings(as.numeric(movebank_df$`Link Quality`))
  if ("Country Code" %in% names(movebank_df)) movebank_df$`Country Code` <- suppressWarnings(as.numeric(movebank_df$`Country Code`))
  if ("Time start" %in% names(movebank_df)) movebank_df$`Time start` <- lubridate::as_datetime(movebank_df$`Time start`)
  if ("Time end" %in% names(movebank_df)) movebank_df$`Time end` <- lubridate::as_datetime(movebank_df$`Time end`)
  if ("VeDBA [m/s²]" %in% names(movebank_df)) movebank_df$`VeDBA [m/s²]` <- suppressWarnings(as.numeric(movebank_df$`VeDBA [m/s²]`))
  if ("temperature [°C]" %in% names(movebank_df)) movebank_df$`temperature [°C]` <- suppressWarnings(as.numeric(movebank_df$`temperature [°C]`))
  if ("pressure [mbar]" %in% names(movebank_df)) movebank_df$`pressure [mbar]` <- suppressWarnings(as.numeric(movebank_df$`pressure [mbar]`))
  if ("min_temp [°C]" %in% names(movebank_df)) movebank_df$`min_temp [°C]` <- as.character(movebank_df$`min_temp [°C]`)

  # If Position exists but lat/long do not, split
  if ("Position" %in% names(movebank_df) && !("latitude [°]" %in% names(movebank_df))) {
    movebank_df <- split_position(movebank_df)
  }

  # ------------------------------------------------------------------
  # 4. Optional cleaning by latitude/longitude thresholds
  # ------------------------------------------------------------------
  if (!is.null(clean_lat_range) || !is.null(clean_lon_range)) {
    message("Cleaning data by latitude/longitude thresholds...")

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
      select(-keep_row)
  }

  # ------------------------------------------------------------------
  # 5. Base fields
  # ------------------------------------------------------------------
  movebank_base <- movebank_df %>%
    mutate(
      `tag ID` = Device,
      `Sigfox computed location radius` =
        as.numeric(str_extract(`Radius (m) (Source/Status)`, "^[0-9]+")),
      `Sigfox computed location source` =
        as.numeric(str_match(`Radius (m) (Source/Status)`, "\\((\\d+)/")[, 2]),
      `Sigfox computed location status` =
        as.numeric(str_match(`Radius (m) (Source/Status)`, "/(\\d+)\\)")[, 2]),
      `Sigfox LQI`          = LQI,
      `Sigfox link quality` = `Link Quality`,
      `Sigfox country`      = `Country Code`,
      timestamp_str         = format(`timestamp SF transmission`, "%Y-%m-%d %H:%M:%OS3"),
      time_start_str        = if ("Time start" %in% names(.)) format(`Time start`, "%Y-%m-%d %H:%M:%OS3") else NA_character_,
      time_end_str          = if ("Time end" %in% names(.))   format(`Time end`,   "%Y-%m-%d %H:%M:%OS3") else NA_character_
    )

  # ------------------------------------------------------------------
  # 6. Locations
  # ------------------------------------------------------------------
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
    left_join(
      animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ------------------------------------------------------------------
  # 7. VeDBA
  # ------------------------------------------------------------------
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
      animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ------------------------------------------------------------------
  # 8. Pressure (min of last 3 hrs)
  # ------------------------------------------------------------------
  # `Min pressure of last 3 hrs (mbar)` or `pressure [mbar]`
  if("Min pressure of last 3 hrs (mbar)" %in% names(movebank_base)){
    bar_data <- movebank_base %>%
      filter(!is.na(`Min pressure of last 3 hrs (mbar)`)) %>%
      transmute(
        `tag ID`,
        `barometric pressure` = `Min pressure of last 3 hrs (mbar)`,
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      )
  }
  if("pressure [mbar]" %in% names(movebank_base)){
    bar_data <- movebank_base %>%
      filter(!is.na(`pressure [mbar]`)) %>%
      transmute(
        `tag ID`,
        `barometric pressure` = `pressure [mbar]`,
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      )
  }


  # ------------------------------------------------------------------
  # 9. Temperature – supports BOTH:
  #    A) long column: `temperature [°C]`
  #    B) wide columns: `Average temperature {X} min ago (°C)`
  # ------------------------------------------------------------------

  # Detect wide temperature columns
  temp_wide_cols <- names(movebank_base)[
    stringr::str_detect(names(movebank_base), "^Average temperature\\s+\\d+\\s+min ago\\s*\\(°C\\)$")
  ]

  if ("temperature [°C]" %in% names(movebank_base)) {
    # --- Case A: already long ---
    temp_data <- movebank_base %>%
      filter(!is.na(`temperature [°C]`)) %>%
      transmute(
        `tag ID`,
        `external temperature` = as.numeric(`temperature [°C]`),
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      )

  } else if (length(temp_wide_cols) > 0) {
    # --- Case B: wide schema -> pivot long & compute time window ---
    # We treat “X min ago” as the END of the 36-min window (consistent with VeDBA code).
    temp_data <- movebank_base %>%
      # We need the real transmission datetime for time arithmetic:
      # If you don't have it here, fall back to parsing timestamp_str.
      mutate(
        .ts = dplyr::coalesce(
          .data$`timestamp SF transmission`,
          lubridate::as_datetime(.data$timestamp_str, tz = "UTC")
        )
      ) %>%
      select(
        `tag ID`, `Sequence Number`,
        `Radius (m) (Source/Status)`,
        `Sigfox computed location radius`,
        `Sigfox computed location source`,
        `Sigfox computed location status`,
        `Sigfox LQI`, `Sigfox link quality`, `Sigfox country`,
        .ts, all_of(temp_wide_cols)
      ) %>%
      tidyr::pivot_longer(
        cols      = all_of(temp_wide_cols),
        names_to  = "temp_metric",
        values_to = "temp_value"
      ) %>%
      mutate(
        minutes_ago = suppressWarnings(as.numeric(stringr::str_extract(temp_metric, "\\d+"))),
        temp_value  = suppressWarnings(as.numeric(temp_value)),
        `Time end`   = .ts - as.difftime(minutes_ago, units = "mins"),
        `Time start` = `Time end` - as.difftime(36, units = "mins"),
        timestamp_str  = format(.ts, "%Y-%m-%d %H:%M:%OS3"),
        time_start_str = format(`Time start`, "%Y-%m-%d %H:%M:%OS3"),
        time_end_str   = format(`Time end`, "%Y-%m-%d %H:%M:%OS3")
      ) %>%
      filter(!is.na(temp_value)) %>%
      transmute(
        `tag ID`,
        `external temperature` = temp_value,
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      ) %>% unique()

  } else {
    # No recognized temperature fields
    temp_data <- tibble::tibble()
  }

  # ------------------------------------------------------------------
  # 10. Min Temp (categorical) – derived
  # ------------------------------------------------------------------
  if("Min temperature of last 3 hrs (temperature range °C)" %in% names(movebank_base)){
    min_temp_data <- movebank_base %>%
      filter(!is.na(`Min temperature of last 3 hrs (temperature range °C)`)) %>%
      transmute(
        `tag ID`,
        `minimum temperature` = `Min temperature of last 3 hrs (temperature range °C)`,
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      )
  }
  if("min_temp [°C]" %in% names(movebank_base)){
    min_temp_data <- movebank_base %>%
      filter(!is.na(`min_temp [°C]`)) %>%
      transmute(
        `tag ID`,
        `minimum temperature` = `min_temp [°C]`,
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
        animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
        by = c("tag ID" = "Tag.ID")
      )
  }

  # ------------------------------------------------------------------
  # 11. Deployment table
  # ------------------------------------------------------------------
  deployment_cols <- c(
    "Tag.ID", "Species", "animal.life.stage", "Weight..g.",
    "Timestamp.release",
    "Animal.ID", "Deploy.On.Latitude", "Deploy.On.Longitude"
  )

  deployment_data <- animals_mb #%>%
    # select(all_of(deployment_cols), Movebank.Project)

  # ------------------------------------------------------------------
  # 12. Write per-project CSVs
  # ------------------------------------------------------------------
  projects <- unique(na.omit(animals_mb$Movebank.Project))

  for (proj in projects) {
    safe_proj_name <- str_replace_all(proj, "[^A-Za-z0-9_\\-]", "_")
    project_folder <- file.path(output_dir, "Projects", safe_proj_name)
    if (!dir_exists(project_folder)) dir_create(project_folder)

    loc_proj      <- loc_data      %>% filter(Movebank.Project == proj)
    vedba_proj    <- vedba_data    %>% filter(Movebank.Project == proj)
    bar_proj      <- bar_data      %>% filter(Movebank.Project == proj)
    temp_proj     <- temp_data     %>% filter(Movebank.Project == proj)
    min_temp_proj <- min_temp_data %>% filter(Movebank.Project == proj)
    dep_proj      <- deployment_data %>%
      filter(Movebank.Project == proj) %>%
      select(-Movebank.Project)

    write_csv(loc_proj,      file.path(project_folder, "data.csv"))
    write_csv(vedba_proj,    file.path(project_folder, "dataVeDBA.csv"))
    write_csv(bar_proj,      file.path(project_folder, "dataBar.csv"))
    write_csv(temp_proj,     file.path(project_folder, "dataTemp.csv"))
    write_csv(min_temp_proj, file.path(project_folder, "dataMinTemp.csv"))
    write_csv(dep_proj,      file.path(project_folder, "deployment.csv"))

    message("Saved Movebank CSVs for project: ", proj,
            " → folder: ", project_folder)
  }

  invisible(
    list(
      movebank_df     = movebank_df,
      animals_mb      = animals_mb,
      loc_data        = loc_data,
      vedba_data      = vedba_data,
      bar_data        = bar_data,
      temp_data       = temp_data,
      min_temp_data   = min_temp_data,
      deployment_data = deployment_data
    )
  )
}


# res <- prep_sigfox_movebank(
#   wc_path           = NULL,
#   movebank_csv_path = "../../../Dropbox/MPI/Moths/Data/movebank/movebank_fall_2025.csv",
#   animals_path      = "../../../Dropbox/MPI/Moths/Data/CaptureSheets/Deathshead2025.csv",
#   output_dir        = "../../../Dropbox/MPI/Moths/Data/movebank",
#   movebank_project_name = "ICARUS Insects. Death's-head hawkmoth. Germany and Switzerland.",
#   clean_lat_range   = NULL,
#   clean_lon_range   = NULL
# )
#
# res <- prep_sigfox_movebank(
#   wc_path           = NULL,
#   movebank_csv_path = "../../../Dropbox/MPI/Moths/Data/movebank/movebank_fall_2025.csv",
#   animals_path      = "../../../Dropbox/MPI/Moths/Data/CaptureSheets/Deathshead2025.csv",
#   output_dir        = "../../../Dropbox/MPI/Moths/Data/movebank",
#   movebank_project_name = "ICARUS Insects. Death's-head hawkmoth. Germany and Switzerland.",
#   clean_lat_range   = NULL,
#   clean_lon_range   = NULL
# )


# res <- prep_sigfox_movebank(
#   wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/wildcloud/",
#   movebank_csv_path = NULL,
#   animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/Tags_Navarre.csv",
#   output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/movebank/",
#   movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula Nyctalus lasiopterus. Navarre, Spain",
#   location_name = "navarre",
#   clean_lat_range   = c(30, 60),
#   clean_lon_range   = NULL
# )
