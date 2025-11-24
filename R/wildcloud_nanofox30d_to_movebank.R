# prep nanofox data for movebank
wc_plut_INF_30Days_v1_2 <- function() {
  list(
    key_prop = list(
      time = list(
        # Original WildCloud time column and a new internal name
        timestamp_id  = "Time (UTC)",
        timestamp_new = "timestamp_sf"
      ),
      split_cols = list(
        # Split Position -> latitude/longitude
        location = list(
          delimiter = ", ",
          colname1  = "latitude [°]",
          colname2  = "longitude [°]",
          type      = "numeric"
        )
      )
    ),
    # Sensor parameters ---------------------------------------------
    VeDBA = list(
      time_offset  = 36,         # minutes between VeDBA samples
      col_name_id  = "VeDBA ",   # matches "VeDBA 5", "VeDBA 4", ...
      col_name_ext = "",
      raw_dim      = "",         # IMPORTANT: no " [LSB]" – matches headers
      physical_dim = " [m/s2]",
      raw2physical = "acc"
    ),
    temperature = list(
      time_offset  = 0,
      col_name_id  = "Avg. Temp ",
      col_name_ext = " (°C)",
      raw_dim      = " [°C]",
      physical_dim = " [°C]",
      raw2physical = FALSE
    ),
    min_temp = list(
      time_offset  = 0,
      col_name_id  = "Min. Temp",
      col_name_ext = " (°C)",
      raw_dim      = " [°C]",
      physical_dim = " [°C]",
      raw2physical = FALSE
    ),
    pressure = list(
      time_offset  = 180,
      col_name_id  = "Pressure",
      col_name_ext = " (mbar)",
      raw_dim      = " [mbar]",
      physical_dim = " [mbar]",
      raw2physical = FALSE
    ),
    # “location” acts like a parameter whose value is the Position column
    location = list(
      time_offset  = 0,
      col_name_id  = "Position",
      col_name_ext = "",
      raw_dim      = "",
      physical_dim = "",
      raw2physical = FALSE
    )
  )
}

wc_fw_details <- function(product = "INF", firmware = "30Days") {
  if (identical(product, "INF") && identical(firmware, "30Days")) {
    list(
      g_range   = 16,
      bit_width = 12,
      n_samples = 28,
      n_bursts  = 18,
      scaling_f = 2600,
      offset_f  = 0
    )
  } else {
    stop("Firmware details not defined for product = ", product,
         ", firmware = ", firmware)
  }
}

wc_lsb2acc <- function(x,
                       mode      = "auto",
                       product   = "INF",
                       firmware  = "30Days",
                       normation = TRUE,
                       g_range   = 16,
                       bit_width = 12,
                       n_samples = 28,
                       n_bursts  = 18,
                       scaling_f = 2600) {

  if (identical(mode, "auto")) {
    fw <- wc_fw_details(product, firmware)
    g_range   <- fw$g_range
    bit_width <- fw$bit_width
    n_samples <- fw$n_samples
    n_bursts  <- fw$n_bursts
    scaling_f <- fw$scaling_f
    offset_f  <- fw$offset_f
  } else {
    offset_f <- 0
  }

  x <- as.numeric(x)
  norm_f <- if (isTRUE(normation)) n_bursts * n_samples else 1

  (g_range * x / (2^bit_width) - offset_f * g_range) * 9.81 * scaling_f / norm_f
}

wc_df_to_dpl <- function(raw_df,
                         plut             = wc_plut_INF_30Days_v1_2(),
                         product          = "INF",
                         firmware         = "30Days",
                         raw2physical     = FALSE,
                         norm_multisamples = TRUE,
                         tz               = "UTC") {

  stopifnot(is.data.frame(raw_df))

  # --- 1) Time parsing -------------------------------------------
  time_colname <- plut$key_prop$time$timestamp_id
  col_SF_time  <- plut$key_prop$time$timestamp_new

  if (!time_colname %in% names(raw_df)) {
    stop("Time column '", time_colname, "' not found in raw_df.")
  }

  # Parse "30.09.2025, 12:47:34" -> POSIXct
  raw_df[[time_colname]] <- raw_df[[time_colname]] |>
    as.character() |>
    gsub(",", "", x = _) |>
    lubridate::dmy_hms(tz = tz)

  # --- 2) Determine parameter vs non-parameter columns -----------
  param_keys <- setdiff(names(plut), "key_prop")

  param_ids <- vapply(param_keys, function(k) plut[[k]]$col_name_id, character(1))
  pattern   <- paste0("^(", paste(param_ids, collapse = "|"), ")")

  param_cols     <- grep(pattern, names(raw_df), value = TRUE)
  non_param_cols <- setdiff(names(raw_df), param_cols)

  # Template Sigfox-level df (one row per transmission)
  SF_df <- raw_df[, non_param_cols, drop = FALSE]

  # Ensure Sigfox timestamp uses the new name
  names(SF_df)[names(SF_df) == time_colname] <- col_SF_time

  # Start with empty data-point-level df with same non-parameter columns
  proc_df <- SF_df[0, , drop = FALSE]

  # --- 3) Expand each parameter to data-point rows ----------------
  for (param_name in param_keys) {

    pinfo        <- plut[[param_name]]
    col_id       <- pinfo$col_name_id
    col_ext      <- pinfo$col_name_ext
    time_offset  <- pinfo$time_offset
    raw_dim      <- pinfo$raw_dim
    physical_dim <- pinfo$physical_dim

    # All columns belonging to this parameter
    cols_for_param <- grep(paste0("^", col_id), names(raw_df), value = TRUE)
    param_count    <- length(cols_for_param)

    if (param_count == 0L) {
      next  # nothing to do for this parameter
    }

    # For j = 1..param_count, create data-point rows
    for (j in seq_len(param_count)) {

      tmp <- SF_df

      # Time start/end, matching Python logic:
      # start = SF_time - offset*(param_count - j + 1)
      # end   = SF_time - offset*(param_count - j)
      if (time_offset != 0) {
        offset_minutes_start <- time_offset * (param_count - j + 1)
        offset_minutes_end   <- time_offset * (param_count - j)

        tmp[["Time start"]] <- tmp[[col_SF_time]] -
          as.difftime(offset_minutes_start, units = "mins")
        tmp[["Time end"]] <- tmp[[col_SF_time]] -
          as.difftime(offset_minutes_end, units = "mins")
      } else {
        tmp[["Time start"]] <- tmp[[col_SF_time]]
        tmp[["Time end"]]   <- tmp[[col_SF_time]]
      }

      # Determine source column name (same logic as Python try/except)
      cand1 <- paste0(col_id, j, col_ext)
      cand2 <- paste0(col_id, col_ext)
      cand3 <- col_id

      candidates <- c(cand1, cand2, cand3)
      src_col    <- candidates[candidates %in% names(raw_df)][1]

      raw_col_name <- paste0(param_name, raw_dim)

      # Decide if this parameter should be numeric.
      # VeDBA, temperatures, and pressure should all be numeric.
      is_numeric_param <- !identical(pinfo$raw2physical, FALSE) ||
        grepl("temp|pressure|vedba", param_name, ignore.case = TRUE)

      if (!is.na(src_col)) {
        values <- raw_df[[src_col]]
        if (is_numeric_param) {
          # force numeric for consistency; non-numeric become NA
          values <- suppressWarnings(as.numeric(values))
        }
        tmp[[raw_col_name]] <- values
      } else {
        tmp[[raw_col_name]] <- if (is_numeric_param) NA_real_ else NA
      }


      # Optional: convert to physical units (VeDBA -> m/s²)
      if (!identical(pinfo$raw2physical, FALSE) && isTRUE(raw2physical)) {
        if (identical(pinfo$raw2physical, "acc")) {
          phys_col_name <- paste0(param_name, physical_dim)
          tmp[[phys_col_name]] <-
            wc_lsb2acc(tmp[[raw_col_name]],
                       mode      = "auto",
                       product   = product,
                       firmware  = firmware,
                       normation = norm_multisamples)
        }
      }

      proc_df <- dplyr::bind_rows(proc_df, tmp)
    }
  }

  # --- 4) Split composite columns (Position -> lat/long) ---------
  param_split <- plut$key_prop$split_cols

  for (split_name in names(param_split)) {
    sinfo <- param_split[[split_name]]

    if (!split_name %in% names(proc_df))
      next

    parts <- stringr::str_split_fixed(
      string = proc_df[[split_name]],
      pattern = sinfo$delimiter,
      n = 2
    )

    col1 <- sinfo$colname1
    col2 <- sinfo$colname2

    proc_df[[col1]] <- suppressWarnings(as.numeric(parts[, 1]))
    proc_df[[col2]] <- suppressWarnings(as.numeric(parts[, 2]))

    proc_df[[split_name]] <- NULL
  }

  # --- 5) Remove duplicate rows ---------------------------------------
  proc_df <- proc_df |>
    dplyr::distinct(
      Device,
      `Time start`,
      `Time end`,
      `latitude [°]`,
      `longitude [°]`,
      .keep_all = TRUE
    )

  proc_df
}

wc_read_dir <- function(path, delim = ";") {
  files <- list.files(path, pattern = "\\.csv$", full.names = TRUE)
  if (length(files) == 0L) {
    stop("No .csv files found in ", path)
  }

  # Read *all* columns as character so types are consistent across files
  purrr::map_dfr(
    files,
    ~ readr::read_delim(
      .x,
      delim         = delim,
      col_types     = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE
    )
  )
}

wc_multicsv_to_dpl <- function(path,
                               product           = "INF",
                               firmware          = "30Days",
                               raw2physical      = FALSE,
                               norm_multisamples = TRUE,
                               tz                = "UTC") {

  raw_df <- wc_read_dir(path)

  plut <- wc_plut_INF_30Days_v1_2()

  wc_df_to_dpl(
    raw_df           = raw_df,
    plut             = plut,
    product          = product,
    firmware         = firmware,
    raw2physical     = raw2physical,
    norm_multisamples = norm_multisamples,
    tz               = tz
  )
}

library(tidyverse)
library(lubridate)
library(stringr)
library(purrr)

source("wc2movebank.R")  # file containing the functions above

path <- "C:/Users/Edward/Dropbox/MPI/Moths/Data/wildcloud"

movebank_df <- wc_multicsv_to_dpl(path,
                                  raw2physical      = TRUE,
                                  norm_multisamples = TRUE)

movebank_df <- wc_multicsv_to_dpl(
  path,
  raw2physical      = TRUE,
  norm_multisamples = TRUE
) |>
  dplyr::mutate(
    Device           = as.character(Device),
    timestamp_sf     = lubridate::as_datetime(timestamp_sf),
    `Raw Data`       = as.character(`Raw Data`),
    `Radius (m) (Source/Status)` = as.character(`Radius (m) (Source/Status)`),
    `Sequence Number` = as.numeric(`Sequence Number`),
    LQI              = as.character(LQI),
    `Link Quality`   = as.numeric(`Link Quality`),
    `Operator Name`  = as.character(`Operator Name`),
    `Country Code`   = as.numeric(`Country Code`),
    `Base Stations (ID, RSSI, Reps)` =
      as.character(`Base Stations (ID, RSSI, Reps)`),
    Compression      = as.character(Compression),
    `Time start`     = lubridate::as_datetime(`Time start`),
    `Time end`       = lubridate::as_datetime(`Time end`),
    VeDBA            = as.numeric(VeDBA),
    `VeDBA [m/s2]`   = as.numeric(`VeDBA [m/s2]`),
    `temperature [°C]` = as.numeric(`temperature [°C]`),
    `min_temp [°C]`    = as.numeric(`min_temp [°C]`),
    `pressure [mbar]`  = as.numeric(`pressure [mbar]`),
    `latitude [°]`     = as.numeric(`latitude [°]`),
    `longitude [°]`    = as.numeric(`longitude [°]`)
  )


dplyr::glimpse(movebank_df)
plot(movebank_df$`longitude [°]`, movebank_df$`latitude [°]`, col = factor(movebank_df$Device))
movebank_df$Device %>% table()

tmp <- movebank_df[which(movebank_df$`latitude [°]` < 0),]
plot(tmp$`longitude [°]`, tmp$`latitude [°]`)
glimpse(tmp)
tmp$Device %>% unique() %>% paste

# remove tags not in the study
not_my_tags <- tmp$Device %>% unique

movebank_clean <- movebank_df[-which(movebank_df$Device %in% not_my_tags),]
plot(movebank_clean$`longitude [°]`, movebank_clean$`latitude [°]`, col = factor(movebank_clean$Device))
ggplot()+
  geom_sf(data = rnaturalearth::countries110)+
  geom_path(data = movebank_clean,
            aes(`longitude [°]`, `latitude [°]`, col = Device))+
  ylim(c(30,60))+
  xlim(c(0,30))+
  theme(legend.position ="none")

write.csv(movebank_clean, file = "../../../Dropbox/MPI/Moths/Data/movebank/movebank_fall_2025.csv")

