#' Download Sigfox Tracking Data
#'
#' This function downloads tracking data for individual tags from a specified source.
#' It retries the download multiple times in case of connection failure.
#'
#' @param tag_ID Character; Biologger ID.
#' @param ID Character; PIT-tag identifier.
#' @param ring Character; Ring identifier.
#' @param attachment_type Character; Type of attachment, e.g., 'glue' or 'collar'.
#' @param capture_weight Numeric; Weight of the bat at capture in grams.
#' @param capture_time POSIXct; DateTime of capture.
#' @param FA_length Numeric; Forearm length in mm.
#' @param tag_weight Numeric; Weight of the tag in grams.
#' @param sex Character; Sex of the bat.
#' @param age Character; Age of the bat.
#' @param repro_status Character; Reproductive status of the bat.
#' @param species Character; Species of the bat.
#' @param release_time POSIXct; DateTime of release.
#' @param capture_latitude Numeric; Latitude of capture location.
#' @param capture_longitude Numeric; Longitude of capture location.
#' @param roost Character; Roost identifier.
#' @param firmware Character; Firmware version of the tag
#' @param download_attempts Numeric; Number of times to retry downloads of the sigfox data
#' @return A data frame of the deployment data including tracking and bat information.
#' @examples
#' \dontrun{
#'   downloaded_data <- sigfox_download(ID = "12345", tag_ID = "ABC123", ...)
#' }
#' @export
#' @importFrom rvest read_html html_nodes html_table
#' @importFrom dplyr %>% filter select mutate
#' @importFrom data.table data.table rbindlist
#' @importFrom lubridate dmy_hms
sigfox_download <- function(tag_ID = NA,
                            ID = NA,
                            ring = NA,
                            attachment_type = NA,
                            capture_weight = NA,
                            capture_time = NA,
                            FA_length = NA,
                            tag_weight = NA,
                            sex = NA,
                            age = NA,
                            repro_status = NA,
                            species = NA,
                            release_time = NA,
                            capture_latitude = NA,
                            capture_longitude = NA,
                            deploy_on_latitude = NA,
                            deploy_on_longitude = NA,
                            roost = NA,
                            firmware = NA,
                            download_attempts = 5) {

  # Ensure required packages are installed and loaded
  pacman::p_load(tidyverse, data.table, lubridate, rvest, stringr, pacman, update = FALSE)

  # Define the length of your main variable, for example, using tag_ID
  num_rows <- length(tag_ID)

  # Ensure that all variables have the same length, filling with NA if necessary
  roost <- if(is.null(roost)) rep(NA, num_rows) else roost
  firmware <- if(is.null(firmware)) rep(NA, num_rows) else firmware
  ID <- if(length(ID) == 0) rep(NA, num_rows) else ID
  ring <- if(length(ring) == 0) rep(NA, num_rows) else ring
  attachment_type <- if(length(attachment_type) == 0) rep(NA, num_rows) else attachment_type
  capture_weight <- if(length(capture_weight) == 0) rep(NA, num_rows) else capture_weight
  capture_time <- if(length(capture_time) == 0) rep(NA, num_rows) else capture_time
  FA_length <- if(length(FA_length) == 0) rep(NA, num_rows) else FA_length
  tag_weight <- if(length(tag_weight) == 0) rep(NA, num_rows) else tag_weight
  sex <- if(length(sex) == 0) rep(NA, num_rows) else sex
  age <- if(length(age) == 0) rep(NA, num_rows) else age
  repro_status <- if(length(repro_status) == 0) rep(NA, num_rows) else repro_status
  species <- if(length(species) == 0) rep(NA, num_rows) else species
  release_time <- if(length(release_time) == 0) rep(NA, num_rows) else release_time
  capture_latitude <- if(length(capture_latitude) == 0) rep(NA, num_rows) else capture_latitude
  capture_longitude <- if(length(capture_longitude) == 0) rep(NA, num_rows) else capture_longitude
  deploy_on_latitude <- if(length(deploy_on_latitude) == 0) rep(NA, num_rows) else deploy_on_latitude
  deploy_on_longitude <- if(length(deploy_on_longitude) == 0) rep(NA, num_rows) else deploy_on_longitude

  # Prepare capture data
  capture_data <- data.frame(tag_ID, ID, ring, attachment_type, capture_weight,
                             capture_time, FA_length, tag_weight, sex, age,
                             repro_status, species, release_time,
                             capture_latitude, capture_longitude,
                             deploy_on_latitude, deploy_on_longitude,
                             roost, firmware, stringsAsFactors = FALSE)
  capture_data <- capture_data[which(nchar(capture_data$tag_ID) > 1),]

  capture_data$tag_ID <- stringr::str_remove(capture_data$tag_ID, "^0+")
  tags <- na.omit(unique(capture_data$tag_ID[nchar(capture_data$tag_ID) == 7]))

  df <- data.table()

  for(i in seq_along(tags)) {
    message(paste0("tag ", tags[i], ": ", i, " out of ", length(tags)))
    url <- paste0("https://mpiab.4lima.de/batt.php?id=", tags[i])

    d <- retry_download(url, max_attempts = download_attempts)

    if (!is.null(d) && length(d) >= 2) {
      d[[2]]$tag_ID <- tags[i]
      df <- rbind(df, d[[2]])
      #TODO extract activation messages
    } else {
      message(paste("Failed to retrieve data for bat", tags[i], "after", download_attempts, "attempts."))
    }
  }
  # return(df)
  df$`24h Min. Pressure (mbar)` <- as.numeric(df$`24h Min. Pressure (mbar)`)

  processed_data <- process_data(df, capture_data)
  # processed_data %>% group_by(Device) %>% reframe(first(timestamp), last(timestamp), n())
  return(processed_data)
}

#' Retry downloading HTML data
#'
#' @param url The URL to download from.
#' @param max_attempts Maximum number of attempts to download.
#' @return Downloaded HTML tables or NULL if unsuccessful.
retry_download <- function(url, max_attempts = 5) {
  attempt <- 1
  d <- NULL
  while(is.null(d) && attempt <= max_attempts) {
    try({
      #TODO
      # D <- url %>%
      #   read_html()
      # D %>% html_nodes("body") %>% html_text()
      # grep between last activation and (Paris)

      d <- url %>%
        read_html() %>%
        html_nodes("table") %>%
        html_table(fill = TRUE)
    }, silent = TRUE)

    if(is.null(d)) {
      Sys.sleep(2)
      attempt <- attempt + 1
    }
  }
  return(d)
}

#' Process downloaded data
#'
#' @param df Data frame of downloaded data.
#' @param capture_data Data frame of capture information.
#' @return A data frame with combined and processed information.
process_data <- function(df, capture_data) {
  # Example processing steps
  # This should be replaced with actual data processing logic
  df$timestamp <- lubridate::dmy_hms(df$`Time (Paris)`, tz = "Europe/Berlin")
  df$latitude <- sapply(strsplit(as.character(df$Position), ","), `[`, 1) %>% as.numeric
  df$longitude <- sapply(strsplit(as.character(df$Position), ","), `[`, 2) %>% as.numeric

  df <- df %>%
    left_join(capture_data, by = "tag_ID")
  # Creating initial rows based on capture data
  initial_rows <- capture_data %>%
    transmute(
      tag_ID = tag_ID,
      Device = tag_ID,
      `Time (Paris)` = NA_character_,
      `Raw Data` = NA_character_,
      Position = NA_character_,
      `Radius (m) (Source/Status)` = NA_character_,
      `Total VeDBA` = NA_integer_,
      `24h Min. Temperature (??C)` = NA_real_,
      `24h Max. Temperature (??C)` = NA_real_,
      `24h Active (%)` = NA_real_,
      `24h Min. Pressure (mbar)` = NA_real_,
      `Seq. Number` = NA_integer_,
      LQI = NA_character_,
      `Link Quality` = NA_integer_,
      Operator = NA_character_,
      `Country Code` = NA_integer_,
      `Base Stations (ID, RSSI, Reps)` = NA_character_,
      timestamp = as.POSIXct(release_time),
      latitude = deploy_on_latitude,
      longitude = deploy_on_longitude,
      tag_ID, ID, ring, attachment_type, capture_weight,
      capture_time, FA_length, tag_weight, sex, age,
      repro_status, species, release_time,
      capture_latitude, capture_longitude,
      deploy_on_latitude, deploy_on_longitude,
      roost, firmware)

  # Combine initial rows with the main data frame
  joined_df <- bind_rows(initial_rows, df) %>%
    arrange(tag_ID, timestamp)  # Ensure the data is sorted

  # # remove undeployed locations
  # min_time_by_tag <- joined_df %>%
  #   group_by(tag_ID) %>%
  #   summarise(initial_timestamp = min(timestamp, na.rm = TRUE)) %>%
  #   ungroup()

  # Join the minimum timestamp back to the main df and filter
  if(any(!is.na(joined_df$release_time))){
    joined_df <- joined_df %>% group_by(Device) %>%
      #left_join(min_time_by_tag, by = "tag_ID") %>%
      filter(timestamp >= release_time) # %>%
      #select(-initial_timestamp)  # Remove the extra column after filtering
  }
  return(joined_df)
}

