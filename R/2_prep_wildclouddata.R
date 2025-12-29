## --- Libraries ----
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(readr)
library(fs)
library(data.table)

## ------------------------------------------------------------------
## 0. Read inputs
## ------------------------------------------------------------------

# movebank_df from your WC → dpl pipeline
# %>% %>% %>% %>% %>% %>% %>% movebank_df <- wc_multicsv_to_dpl(path, raw2physical = TRUE, norm_multisamples = TRUE)
movebank_df <- movebank_clean

output_dir <- "../../../Dropbox/MPI/Moths/Data/movebank"

# capture sheet
animals <- read.csv("../../../Dropbox/MPI/Moths/Data/CaptureSheets/Deathshead2025.csv",
                    header = TRUE, sep = ",", stringsAsFactors = FALSE)

# Basic species / life stage
animals$species <- "Acherontia atropos"
animals$animal.life.stage <- "Adult"
# animals$date_released <- dmy_hms(animals$date_released)

## ------------------------------------------------------------------
## 1. Prepare animals table for Movebank (Tag.ID, Animal.ID, deployments)
## ------------------------------------------------------------------

# Set project name (change if you like)
movebank_project_name <- "ICARUS Insects. Death's-head hawkmoth. Germany and Switzerland."
# movebank_project_name <- "Death's-head hawkmoth migration SigFox MPIAB"

# Helper: parse tagging times in messy formats to Berlin time
parse_tagging_time <- function(x) {
  # If date only, assume noon
  x <- ifelse(str_detect(x, "^\\d{4}-\\d{2}-\\d{2}$"),
              paste0(x, " 12:00"),
              x)
  # Try several common formats
  dt <- parse_date_time(
    x,
    orders = c("d/m/Y H:M", "d/m/Y H:M:S", "d-m-Y H:M", "Y-m-d H:M", "Y-m-d"),
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
    `Weight.g.`        = mass_w_tag_g,      # or mass_g if you prefer
    `Deploy.On.Latitude`  = release_latitude,
    `Deploy.On.Longitude` = release_longitude,
    `Timestamp.release` =
      parse_tagging_time(date_released),
    Movebank.Project = movebank_project_name
  )

## ------------------------------------------------------------------
## 2. Prepare base fields from movebank_df (Sigfox, time, tag ID, etc.)
## ------------------------------------------------------------------

# Make sure names are what we expect:
# names(movebank_df)
movebank_df$`min_temp [°C]`[which(!is.na(movebank_df$`min_temp [°C]`))]

movebank_base <- movebank_df %>%
  mutate(
    `tag ID` = Device,
    # Sigfox radius/source/status parsed from "Radius (m) (Source/Status)"
    `Sigfox computed location radius` =
      as.numeric(str_extract(`Radius (m) (Source/Status)`, "^[0-9]+")),
    `Sigfox computed location source` =
      as.numeric(str_match(`Radius (m) (Source/Status)`, "\\((\\d+)/")[, 2]),
    `Sigfox computed location status` =
      as.numeric(str_match(`Radius (m) (Source/Status)`, "/(\\d+)\\)")[, 2]),
    `Sigfox LQI`         = LQI,
    `Sigfox link quality`= `Link Quality`,
    `Sigfox country`     = `Country Code`,
    # Standardised time strings for Movebank
    timestamp_str        = format(timestamp_sf, "%Y-%m-%d %H:%M:%OS3"),
    time_start_str       = format(`Time start`, "%Y-%m-%d %H:%M:%OS3"),
    time_end_str         = format(`Time end`,   "%Y-%m-%d %H:%M:%OS3")
  )

## ------------------------------------------------------------------
## 3. Locations (sigfox-geolocation)
## ------------------------------------------------------------------

loc_data <- movebank_base %>%
  filter(!is.na(`latitude [°]`), !is.na(`longitude [°]`)) %>%
  mutate(.keep = "none",
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

# table(paste(loc_data$`tag ID`, loc_data$timestamp))[1:10]

## ------------------------------------------------------------------
## 4. VeDBA (acceleration) – 5 samples per burst
## ------------------------------------------------------------------

vedba_data <- movebank_base %>%
  filter(!is.na(`VeDBA [m/s2]`)) %>%  # use physical units
  mutate(.keep = "none",
    `tag ID`,
    VeDBA = `VeDBA [m/s2]`,
    timestamp       = timestamp_str,
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

# table(paste(vedba_data$`tag ID`, vedba_data$timestamp))[1:10]

## ------------------------------------------------------------------
## 5. Pressure (barometer) – 1 sample per ~3 h
## ------------------------------------------------------------------

bar_data <- movebank_base %>%
  filter(!is.na(`pressure [mbar]`)) %>%
  mutate(.keep = "none",
    `tag ID`,
    `barometric pressure` = `pressure [mbar]`,
    timestamp       = timestamp_str,
    `start timestamp` = time_start_str,
    `end timestamp`   = time_end_str,
    `sequence number` = `Sequence Number`,
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

table(paste(bar_data$`tag ID`, bar_data$timestamp))[1:10]
## ------------------------------------------------------------------
## 6. Temperature – 5 samples per burst
## ------------------------------------------------------------------

temp_data <- movebank_base %>%
  filter(!is.na(`temperature [°C]`)) %>%
  mutate(.keep = "none",
    `tag ID`,
    `external temperature` = `temperature [°C]`,
    timestamp       = timestamp_str,
    `start timestamp` = time_start_str,
    `end timestamp`   = time_end_str,
    `sequence number` = `Sequence Number`,
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
table(paste(temp_data$`tag ID`, temp_data$timestamp))[1:10]

min_temp_data <- movebank_base %>%
  filter(!is.na(`min_temp [°C]`)) %>%
  mutate(.keep = "none",
    `tag ID`,
    `minimum temperature` = `temperature [°C]`,
    timestamp       = timestamp_str,
    `start timestamp` = time_start_str,
    `end timestamp`   = time_end_str,
    `sequence number` = `Sequence Number`,
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

table(paste(min_temp_data$`tag ID`, min_temp_data$timestamp))[1:10]

## ------------------------------------------------------------------
## 7. Deployment table for Movebank
## ------------------------------------------------------------------

deployment_cols <- c(
  "Tag.ID", "Species", "animal.life.stage", "Weight..g.",
  "Timestamp.release",
  "Animal.ID", "Deploy.On.Latitude", "Deploy.On.Longitude"
)

deployment_data <- animals_mb %>%
  select(all_of(deployment_cols), Movebank.Project)

## ------------------------------------------------------------------
## 8. Write per-project CSVs (Projects/<ProjectName>/...)
## ------------------------------------------------------------------

# if (!dir_exists("Projects")) dir_create("Projects")

projects <- unique(na.omit(animals_mb$Movebank.Project))

for (proj in projects) {
  safe_proj_name <- str_replace_all(proj, "[^A-Za-z0-9_\\-]", "_")
  project_folder <- file.path(output_dir, "Projects", safe_proj_name)
  if (!dir_exists(project_folder)) dir_create(project_folder)

  # Subset for this project
  loc_proj   <- loc_data   %>% filter(Movebank.Project == proj)
  vedba_proj <- vedba_data %>% filter(Movebank.Project == proj)
  bar_proj   <- bar_data   %>% filter(Movebank.Project == proj)
  temp_proj  <- temp_data  %>% filter(Movebank.Project == proj)
  min_temp_proj  <- min_temp_data  %>% filter(Movebank.Project == proj)
  dep_proj   <- deployment_data %>% filter(Movebank.Project == proj) %>%
    select(-Movebank.Project)

  write_csv(loc_proj,   file.path(project_folder, "data.csv"))
  write_csv(vedba_proj, file.path(project_folder, "dataVeDBA.csv"))
  write_csv(bar_proj,   file.path(project_folder, "dataBar.csv"))
  write_csv(temp_proj,  file.path(project_folder, "dataTemp.csv"))
  write_csv(min_temp_proj,  file.path(project_folder, "dataMinTemp.csv"))
  write_csv(dep_proj,   file.path(project_folder, "deployment.csv"))

  message("Saved Movebank CSVs for project: ", proj,
          " → folder: ", project_folder)
}
