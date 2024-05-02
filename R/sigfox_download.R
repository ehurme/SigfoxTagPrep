#' Download Sigfox Tracking Data
#'
#' This function downloads tracking data for individual bats from a specified source.
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
sigfox_download <- function(tag_ID = NA, ID = NA, ring = NA,
                            attachment_type = NA,
                            capture_weight = NA,
                            capture_time = NA,
                            FA_length = NA,
                            tag_weight = NA,
                            sex = NA, age = NA, repro_status = NA,
                            species = NA, release_time = NA,
                            capture_latitude = NA,
                            capture_longitude = NA,
                            roost = NA,
                            download_attempts = 5) {

  # Ensure required packages are installed and loaded
  pacman::p_load(tidyverse, data.table, lubridate, rvest, stringr, pacman, update = FALSE)

  # Prepare capture data
  capture_data <- data.frame(tag_ID, ID, ring, attachment_type, capture_weight,
                             capture_time, FA_length, tag_weight, sex, age,
                             repro_status, species, release_time, capture_latitude,
                             capture_longitude, roost, stringsAsFactors = FALSE)
  capture_data <- capture_data[which(nchar(capture_data$tag_ID) > 1),]

  capture_data$tag_ID <- stringr::str_remove(capture_data$tag_ID, "^0+")
  bats <- na.omit(unique(capture_data$tag_ID[nchar(capture_data$tag_ID) == 7]))

  df <- data.table()

  for(i in seq_along(bats)) {
    message(paste0("bat ", bats[i], ": ", i, " out of ", length(bats)))
    url <- paste0("https://mpiab.4lima.de/batt.php?id=", bats[i])

    d <- retry_download(url, max_attempts = download_attempts)

    if (!is.null(d) && length(d) >= 2) {
      d[[2]]$tag_ID <- bats[i]
      df <- rbind(df, d[[2]])
      #TODO extract activation messages
    } else {
      message(paste("Failed to retrieve data for bat", bats[i], "after 5 attempts."))
    }
  }
  return(df)
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
      latitude = capture_latitude,
      longitude = capture_longitude,
      tag_ID, ID, ring, attachment_type, capture_weight,
      capture_time, FA_length, tag_weight, sex, age,
      repro_status, species, release_time, capture_latitude,
      capture_longitude, roost)

  # Combine initial rows with the main data frame
  df <- bind_rows(initial_rows, df) %>%
    arrange(tag_ID, timestamp)  # Ensure the data is sorted

  # remove undeployed locations
  min_time_by_tag <- df %>%
    group_by(tag_ID) %>%
    summarise(initial_timestamp = min(timestamp, na.rm = TRUE)) %>%
    ungroup()
  # Join the minimum timestamp back to the main df and filter
  df <- df %>%
    left_join(min_time_by_tag, by = "tag_ID") %>%
    filter(timestamp >= initial_timestamp) %>%
    select(-initial_timestamp)  # Remove the extra column after filtering

  return(df)
}

