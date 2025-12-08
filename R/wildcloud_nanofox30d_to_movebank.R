wc_plut_INF_30Days_v1_2 <- function() {
  list(
    key_prop = list(
      time = list(
        # Original WildCloud time column and a new internal name
        timestamp_id  = "Time (UTC)",
        # THIS will be the final timestamp column name in movebank_df
        timestamp_new = "timestamp SF transmission"
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

    # -------- Sensor parameters (as they appear in the CSV) --------

    # VeDBA 5 (raw); VeDBA 4 (raw); ...; VeDBA 1 (raw)
    VeDBA = list(
      time_offset  = 36,          # minutes between VeDBA samples
      col_name_id  = "VeDBA ",    # prefix (matches "VeDBA 5 (raw)")
      col_name_ext = "(raw)",     # suffix, not heavily used
      raw_dim      = " [LSB]",    # raw col name: "VeDBA [LSB]"
      physical_dim = " [m/s²]",   # physical col: "VeDBA [m/s²]"
      raw2physical = "acc"
    ),

    # Avg. Temp 5 (°C); ...; Avg. Temp 1 (°C)
    temperature = list(
      time_offset  = 36,
      col_name_id  = "Avg. Temp ",  # prefix
      col_name_ext = "(°C)",        # header is "Avg. Temp 5 (°C)" etc.
      raw_dim      = " [°C]",       # "temperature [°C]"
      physical_dim = " [°C]",
      raw2physical = FALSE
    ),

    # Min. Temp(°C) – categorical string, not numeric
    min_temp = list(
      time_offset  = 180,
      col_name_id  = "Min. Temp",   # matches "Min. Temp(°C)"
      col_name_ext = "(°C)",        # no space in header
      raw_dim      = " [°C]",       # "min_temp [°C]" (character)
      physical_dim = " [°C]",
      raw2physical = FALSE
    ),

    # Pressure (mbar)
    pressure = list(
      time_offset  = 180,
      col_name_id  = "Pressure ",   # matches "Pressure (mbar)"
      col_name_ext = "(mbar)",
      raw_dim      = " [mbar]",     # "pressure [mbar]"
      physical_dim = " [mbar]",
      raw2physical = FALSE
    ),

    # Location "parameter" (Position string)
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

  # returns acceleration in m/s² scaled as in the original Python
  (g_range * x / (2^bit_width) - offset_f * g_range) * 9.81 * scaling_f / norm_f
}

wc_df_to_dpl <- function(raw_df,
                         plut              = wc_plut_INF_30Days_v1_2(),
                         product           = "INF",
                         firmware          = "30Days",
                         raw2physical      = FALSE,
                         norm_multisamples = TRUE,
                         tz                = "UTC") {

  stopifnot(is.data.frame(raw_df))

  # --- 0) Remove exact duplicates at Sigfox level ------------------
  raw_df <- raw_df |>
    dplyr::distinct(
      Device,
      `Time (UTC)`,
      `Raw Data`,
      `Position`,
      .keep_all = TRUE
    )

  # --- 1) Time parsing ---------------------------------------------
  time_colname <- plut$key_prop$time$timestamp_id
  col_SF_time  <- plut$key_prop$time$timestamp_new   # "timestamp SF transmission"

  if (!time_colname %in% names(raw_df)) {
    stop("Time column '", time_colname, "' not found in raw_df.")
  }

  # Parse "30.09.2025, 12:47:34" -> POSIXct
  raw_df[[time_colname]] <- raw_df[[time_colname]] |>
    as.character() |>
    gsub(",", "", x = _) |>
    lubridate::dmy_hms(tz = tz)

  # --- 2) Determine parameter vs non-parameter columns -------------
  param_keys <- setdiff(names(plut), "key_prop")

  param_ids <- vapply(param_keys, function(k) plut[[k]]$col_name_id, character(1))
  pattern   <- paste0("^(", paste(param_ids, collapse = "|"), ")")

  param_cols     <- grep(pattern, names(raw_df), value = TRUE)
  non_param_cols <- setdiff(names(raw_df), param_cols)

  # Template Sigfox-level df (one row per transmission)
  SF_df <- raw_df[, non_param_cols, drop = FALSE]

  # Ensure Sigfox timestamp uses the new name (timestamp SF transmission)
  names(SF_df)[names(SF_df) == time_colname] <- col_SF_time

  # Start with empty data-point-level df with same non-parameter columns
  proc_df <- SF_df[0, , drop = FALSE]

  # --- 3) Expand each parameter to data-point rows -----------------
  for (param_name in param_keys) {

    pinfo        <- plut[[param_name]]
    col_id       <- pinfo$col_name_id
    time_offset  <- pinfo$time_offset
    raw_dim      <- pinfo$raw_dim
    physical_dim <- pinfo$physical_dim

    # All columns belonging to this parameter (any header style)
    cols_for_param <- grep(paste0("^", col_id), names(raw_df), value = TRUE)

    if (length(cols_for_param) == 0L) {
      next  # nothing to do for this parameter
    }

    # Extract index numbers for multi-dimensional parameters
    idx <- stringr::str_extract(cols_for_param, "\\d+")
    idx <- unique(idx[!is.na(idx)])

    if (length(idx) > 0L) {
      # e.g. VeDBA 5 (raw) .. VeDBA 1 (raw); Avg. Temp 5..1
      dims <- idx
    } else {
      # Single-column parameters (Min. Temp(°C), Pressure (mbar), Position)
      dims <- ""
    }

    param_count <- length(dims)

    # Decide which parameters are numeric
    numeric_params <- c("Vedba", "VeDBA", "temperature", "pressure")
    is_numeric_param <- !identical(pinfo$raw2physical, FALSE) ||
      tolower(param_name) %in% tolower(numeric_params)

    # Loop over each dimension j
    for (j in seq_len(param_count)) {

      tmp <- SF_df

      # Time start/end
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

      # Choose the source column for this dimension
      dim_tag <- dims[j]

      if (dim_tag != "") {
        # e.g. pick "Avg. Temp 5 (°C)" for dim_tag == "5"
        src_candidates <- cols_for_param[
          stringr::str_detect(cols_for_param, paste0("\\b", dim_tag, "\\b"))
        ]
        if (length(src_candidates) == 0L) {
          src_candidates <- cols_for_param  # fallback if header is odd
        }
      } else {
        # no index: just use the first column we found
        src_candidates <- cols_for_param
      }

      src_col <- src_candidates[1]

      raw_col_name <- paste0(param_name, raw_dim)  # e.g. "VeDBA [LSB]"

      if (!is.na(src_col) && nzchar(src_col)) {
        values <- raw_df[[src_col]]
        if (is_numeric_param) {
          values <- suppressWarnings(as.numeric(values))
        }
        tmp[[raw_col_name]] <- values
      } else {
        tmp[[raw_col_name]] <- if (is_numeric_param) NA_real_ else NA
      }

      # Optional: VeDBA raw -> VeDBA [m/s²]
      if (!identical(pinfo$raw2physical, FALSE) && isTRUE(raw2physical)) {
        if (identical(pinfo$raw2physical, "acc")) {
          phys_col_name <- paste0(param_name, physical_dim)  # "VeDBA [m/s²]"
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

  # --- 4) Split composite columns (Position -> lat/long) ----------
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

  # NOTE: we leave duplicate removal commented out here – you can
  # control that upstream or in the wrapper if needed.
  # proc_df <- proc_df |>
  #   dplyr::distinct(
  #     Device,
  #     `Time start`,
  #     `Time end`,
  #     `latitude [°]`,
  #     `longitude [°]`,
  #     .keep_all = TRUE
  #   )

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
    raw_df            = raw_df,
    plut              = plut,
    product           = product,
    firmware          = firmware,
    raw2physical      = raw2physical,
    norm_multisamples = norm_multisamples,
    tz                = tz
  )
}

# movebank_df <- wc_multicsv_to_dpl(
#   path,
#   raw2physical      = TRUE,
#   norm_multisamples = TRUE
# )
#
# head(movebank_df)
