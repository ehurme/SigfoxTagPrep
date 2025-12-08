prep_sigfox_movebank <- function(
    wc_path              = NULL,  # directory with WildCloud CSVs
    movebank_csv_path    = NULL,  # alternative: existing movebank-style CSV
    animals_path,
    output_dir,
    movebank_project_name,
    species              = "Acherontia atropos",
    life_stage           = "Adult",
    clean_lat_range      = NULL,  # e.g. c(30, 60) or NULL to skip
    clean_lon_range      = NULL   # e.g. c(0, 30) or NULL to skip
) {
  # --- Libraries ----
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(lubridate)
  library(readr)
  library(fs)
  library(data.table)
  source("./R/wildcloud_nanofox30d_to_movebank.R")
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
  # 1. Read capture sheet and prepare animals table
  # ------------------------------------------------------------------
  animals <- read.csv(animals_path,
                      header = TRUE, sep = ",", stringsAsFactors = FALSE)

  animals$species <- species
  animals$animal.life.stage <- life_stage

  # Helper: parse tagging / release times to Berlin time
  parse_tagging_time <- function(x) {
    x <- ifelse(str_detect(x, "^\\d{4}-\\d{2}-\\d{2}$"),
                paste0(x, " 12:00"),
                x)
    dt <- parse_date_time(
      x,
      orders = c("d/m/Y H:M", "d/m/Y H:M:S",
                 "d-m-Y H:M", "d-m-Y H:M:S",
                 "Y-m-d H:M", "Y-m-d"),
      tz = "Europe/Berlin"
    )
    format(dt, "%Y-%m-%d %H:%M:%OS3")
  }

  animals_mb <- animals %>%
    mutate(
      Tag.ID   = tag_id,
      Animal.ID = animal_id,
      Species  = species,
      `animal.life.stage` = animal.life.stage,
      `Weight..g.`        = mass_w_tag_g,      # or mass_g if you prefer
      `Deploy.On.Latitude`  = release_latitude,
      `Deploy.On.Longitude` = release_longitude,
      `Timestamp.release`   = parse_tagging_time(date_released),
      Movebank.Project      = movebank_project_name
    )

  # ------------------------------------------------------------------
  # 2. Get movebank_df: either from WC pipeline or existing CSV
  # ------------------------------------------------------------------
  if (!is.null(movebank_csv_path)) {
    message("Reading existing movebank-style CSV: ", movebank_csv_path)
    movebank_df <- data.table::fread(movebank_csv_path) %>%
      as_tibble()
  } else {
    if (!exists("wc_multicsv_to_dpl")) {
      stop("Function 'wc_multicsv_to_dpl' not found. Source your wildcloud_nanofox30d_to_movebank.R first.")
    }
    message("Running wc_multicsv_to_dpl() on: ", wc_path)
    movebank_df <- wc_multicsv_to_dpl(
      wc_path,
      raw2physical      = TRUE,
      norm_multisamples = TRUE
    ) %>%
      as_tibble()
  }

  # ------------------------------------------------------------------
  # 3. Ensure key columns have correct types, using your actual names
  # ------------------------------------------------------------------
  # We don't change ANY names; we just coerce where present.
  if ("Device" %in% names(movebank_df)) {
    movebank_df$Device <- as.character(movebank_df$Device)
  }
  if ("timestamp SF transmission" %in% names(movebank_df)) {
    movebank_df$`timestamp SF transmission` <-
      lubridate::as_datetime(movebank_df$`timestamp SF transmission`)
  }
  if ("Raw Data" %in% names(movebank_df)) {
    movebank_df$`Raw Data` <- as.character(movebank_df$`Raw Data`)
  }
  if ("Radius (m) (Source/Status)" %in% names(movebank_df)) {
    movebank_df$`Radius (m) (Source/Status)` <-
      as.character(movebank_df$`Radius (m) (Source/Status)`)
  }
  if ("Sequence Number" %in% names(movebank_df)) {
    movebank_df$`Sequence Number` <- as.numeric(movebank_df$`Sequence Number`)
  }
  if ("LQI" %in% names(movebank_df)) {
    movebank_df$LQI <- as.character(movebank_df$LQI)
  }
  if ("Link Quality" %in% names(movebank_df)) {
    movebank_df$`Link Quality` <- as.numeric(movebank_df$`Link Quality`)
  }
  if ("Operator Name" %in% names(movebank_df)) {
    movebank_df$`Operator Name` <- as.character(movebank_df$`Operator Name`)
  }
  if ("Country Code" %in% names(movebank_df)) {
    movebank_df$`Country Code` <- as.numeric(movebank_df$`Country Code`)
  }
  if ("Base Stations (ID, RSSI, Reps)" %in% names(movebank_df)) {
    movebank_df$`Base Stations (ID, RSSI, Reps)` <-
      as.character(movebank_df$`Base Stations (ID, RSSI, Reps)`)
  }
  if ("Compression" %in% names(movebank_df)) {
    movebank_df$Compression <- as.character(movebank_df$Compression)
  }
  if ("Time start" %in% names(movebank_df)) {
    movebank_df$`Time start` <- lubridate::as_datetime(movebank_df$`Time start`)
  }
  if ("Time end" %in% names(movebank_df)) {
    movebank_df$`Time end` <- lubridate::as_datetime(movebank_df$`Time end`)
  }
  if ("VeDBA [LSB]" %in% names(movebank_df)) {
    movebank_df$`VeDBA [LSB]` <- as.numeric(movebank_df$`VeDBA [LSB]`)
  }
  if ("VeDBA [m/s²]" %in% names(movebank_df)) {
    movebank_df$`VeDBA [m/s²]` <- as.numeric(movebank_df$`VeDBA [m/s²]`)
  }
  if ("temperature [°C]" %in% names(movebank_df)) {
    movebank_df$`temperature [°C]` <- as.numeric(movebank_df$`temperature [°C]`)
  }
  if ("min_temp [°C]" %in% names(movebank_df)) {
    # categorical, keep as character
    movebank_df$`min_temp [°C]` <- as.character(movebank_df$`min_temp [°C]`)
  }
  if ("Min. Temp(°C)" %in% names(movebank_df) && !"min_temp [°C]" %in% names(movebank_df)) {
    # If only original categorical field is present, copy it into min_temp [°C]
    movebank_df$`min_temp [°C]` <- as.character(movebank_df$`Min. Temp(°C)`)
  }
  if ("pressure [mbar]" %in% names(movebank_df)) {
    movebank_df$`pressure [mbar]` <- as.numeric(movebank_df$`pressure [mbar]`)
  }
  if ("latitude [°]" %in% names(movebank_df)) {
    movebank_df$`latitude [°]` <- as.numeric(movebank_df$`latitude [°]`)
  }
  if ("longitude [°]" %in% names(movebank_df)) {
    movebank_df$`longitude [°]` <- as.numeric(movebank_df$`longitude [°]`)
  }

  # ------------------------------------------------------------------
  # 4. Optional cleaning by latitude/longitude thresholds
  # ------------------------------------------------------------------
  if (!is.null(clean_lat_range) || !is.null(clean_lon_range)) {
    message("Cleaning data by latitude/longitude thresholds...")

    if (!is.null(clean_lat_range)) {
      lat_min <- min(clean_lat_range, na.rm = TRUE)
      lat_max <- max(clean_lat_range, na.rm = TRUE)
    } else {
      lat_min <- -Inf
      lat_max <-  Inf
    }
    if (!is.null(clean_lon_range)) {
      lon_min <- min(clean_lon_range, na.rm = TRUE)
      lon_max <- max(clean_lon_range, na.rm = TRUE)
    } else {
      lon_min <- -Inf
      lon_max <-  Inf
    }

    movebank_df <- movebank_df %>%
      mutate(
        keep_row = ifelse(
          !is.na(`latitude [°]`) & !is.na(`longitude [°]`),
          `latitude [°]` >= lat_min & `latitude [°]` <= lat_max &
            `longitude [°]` >= lon_min & `longitude [°]` <= lon_max,
          TRUE  # keep rows without coordinates (e.g. some sensor-only rows)
        )
      ) %>%
      filter(keep_row) %>%
      select(-keep_row)
  }

  # ------------------------------------------------------------------
  # 5. Base fields from movebank_df (uses your exact column names)
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
      time_start_str        = format(`Time start`, "%Y-%m-%d %H:%M:%OS3"),
      time_end_str          = format(`Time end`,   "%Y-%m-%d %H:%M:%OS3")
    )

  # ------------------------------------------------------------------
  # 6. Locations (sigfox-geolocation)
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
  # 7. VeDBA (acceleration) – uses VeDBA [m/s²]
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
  # 8. Barometric pressure
  # ------------------------------------------------------------------
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

  # ------------------------------------------------------------------
  # 9. Temperature (Avg. Temp)
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
  # 10. Min Temp (categorical) – derived sensor
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
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/Freinat/Fall2025_nanofox",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/Freinat/Fall2025_nanofox/Capture_Data_Autumn25_MPI_Sheet.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/Freinat/Fall2025_nanofox/movebank",
  movebank_project_name = "Nyctalus leisleri Autumn 2025",
  clean_lat_range   = c(30, 60),
  clean_lon_range   = NULL
)
