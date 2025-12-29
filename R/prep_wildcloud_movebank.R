prep_sigfox_movebank <- function(
    wc_path              = NULL,  # directory with WildCloud CSVs
    movebank_csv_path    = NULL,  # alternative: existing movebank-style CSV OR raw wildcloud export CSV
    animals_path,
    output_dir,
    movebank_project_name,
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
  # 1. Read capture sheet and prepare animals table
  # ------------------------------------------------------------------
  animals <- read.csv(animals_path, header = TRUE, sep = ",", stringsAsFactors = FALSE)
  # animals$species <- species
  # animals$animal.life.stage <- life_stage

  parse_tagging_time <- function(x) {
    x <- ifelse(str_detect(x, "^\\d{4}-\\d{2}-\\d{2}$"), paste0(x, " 12:00"), x)
    dt <- parse_date_time(
      x,
      orders = c("d/m/Y H:M", "d/m/Y H:M:S", "d-m-Y H:M", "d-m-Y H:M:S", "Y-m-d H:M", "Y-m-d"),
      tz = "Europe/Berlin"
    )
    format(dt, "%Y-%m-%d %H:%M:%OS3")
  }

  animals_mb <- animals %>%
    mutate(
      Tag.ID   = tag.id,
      Animal.ID = animal.id,
      Species  = species,
      `animal.life.stage` = animal.life.stage,
      `Weight..g.`        = animal.mass,
      `Deploy.On.Latitude`  = capture.latitude,
      `Deploy.On.Longitude` = capture.longitude,
      `Timestamp.release`   = deploy.on.date,
      Movebank.Project      = movebank_project_name
    )

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
      raw2physical      = FALSE,
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
  if ("Min pressure of last 3 hrs (mbar)" %in% names(movebank_df)) movebank_df$`Min pressure of last 3 hrs (mbar)` <-
    suppressWarnings(as.numeric(movebank_df$`Min pressure of last 3 hrs (mbar)`))
  if ("Min temperature of last 3 hrs (temperature range °C)" %in% names(movebank_df)) movebank_df$`Min temperature of last 3 hrs (temperature range °C)` <-
    as.character(movebank_df$`Min temperature of last 3 hrs (temperature range °C)`)

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

  # ------------------------------------------------------------------
  # 9. Temperature (5 samples)
  # ------------------------------------------------------------------
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
      animals_mb %>% select(Tag.ID, Movebank.Project, Animal.ID),
      by = c("tag ID" = "Tag.ID")
    )

  # ------------------------------------------------------------------
  # 10. Min Temp (categorical) – derived
  # ------------------------------------------------------------------
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

  # ------------------------------------------------------------------
  # 11. Deployment table
  # ------------------------------------------------------------------
  deployment_cols <- c(
    "Tag.ID", "Species", "animal.life.stage", "Weight..g.",
    "Timestamp.release",
    "Animal.ID", "Deploy.On.Latitude", "Deploy.On.Longitude"
  )

  deployment_data <- animals_mb %>%
    select(all_of(deployment_cols), Movebank.Project)

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


res <- prep_sigfox_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/belgium-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus leisleri Flanders",
  clean_lat_range   = c(30, 60),
  clean_lon_range   = NULL
)
