# ------------------------------------------------------------
# Detect single-base-station Sigfox detections and station locations
# ------------------------------------------------------------
detect_sigfox_single_base_stations <- function(
    study_id,
    tz = "UTC",
    verbose = TRUE,
    out_csv = NULL,
    out_rds = NULL
) {
  suppressPackageStartupMessages({
    require(move2)
    require(dplyr)
    require(purrr)
    require(sf)
    require(jsonlite)
    require(lubridate)
    require(stringr)
    require(tibble)
  })

  .msg <- function(...) if (isTRUE(verbose)) message(...)

  # ---- helper: pick a likely track-id column, else create one
  .ensure_track_id <- function(x, sid) {
    # x is an sf/data.frame already
    candidates <- c(
      "individual_local_identifier",
      "tag_local_identifier",
      "animal_id",
      "tag_id",
      "individual_id",
      "deployment_id"
    )
    found <- candidates[candidates %in% names(x)]
    if (length(found) > 0) {
      idcol <- found[1]
      # replace NA/empty with a unique per-row fallback
      id <- as.character(x[[idcol]])
      bad <- is.na(id) | !nzchar(id)
      if (any(bad)) {
        id[bad] <- paste0("study_", sid, "_row_", which(bad))
      }
      x$.track_id <- id
      x$.track_id_source <- idcol
    } else {
      # no obvious ID col exists
      x$.track_id <- paste0("study_", sid, "_row_", seq_len(nrow(x)))
      x$.track_id_source <- "generated"
    }
    x
  }

  # ---- download one study, then immediately coerce to plain sf
  dl_one_sf <- function(sid) {
    .msg("Downloading study_id: ", sid)
    b <- move2::movebank_download_study(study_id = sid)

    # IMPORTANT: coerce away from move2 class BEFORE row-binding
    # move2 objects are sf-like; st_as_sf() drops move2 reconstruction issues
    b_sf <- sf::st_as_sf(b)

    b_sf$.study_id <- sid
    b_sf <- .ensure_track_id(b_sf, sid)

    b_sf
  }

  if (length(study_id) < 1) stop("Provide at least one study_id.")
  b_list <- lapply(study_id, dl_one_sf)

  # safe bind as plain sf (no move2 reconstruction)
  b_all <- do.call(rbind, b_list)

  # --- from here down, keep your existing logic, using b_all as sf ---
  # (I’m including the key bits needed to proceed robustly.)

  # Find a timestamp column
  time_candidates <- c("timestamp", "timestamp_utc", "time", "event_timestamp")
  time_col <- time_candidates[time_candidates %in% names(b_all)][1]
  if (is.na(time_col)) stop("Could not find a timestamp column (timestamp/timestamp_utc/time).")

  # normalize time
  b_all[[time_col]] <- suppressWarnings(lubridate::as_datetime(b_all[[time_col]], tz = tz))
  b_all <- b_all %>% mutate(.original_row = row_number())

  # keep only rows with non-empty geometry
  has_geom <- !sf::st_is_empty(sf::st_geometry(b_all)) & !is.na(sf::st_geometry(b_all))
  b_loc <- b_all[has_geom, , drop = FALSE]
  if (nrow(b_loc) == 0) stop("No non-empty geometry rows found after download.")

  # ------- keep your existing parsing helpers (same as before) -------
  fix_json_string <- function(s) {
    if (is.na(s) || !nzchar(s) || s == "[]") return(NA_character_)
    s <- gsub("([{, ])([a-zA-Z0-9_]+):", "\\1\"\\2\":", s)
    s <- gsub("\"ID\":([^,}\\]]+)", "\"ID\":\"\\1\"", s)
    s
  }

  parse_antennas <- function(x, original_row) {
    if (is.na(x) || !nzchar(x) || x == "[]") {
      return(tibble(ID = character(), N = integer(), RSSI = numeric(), original_row = integer()))
    }
    x_fixed <- fix_json_string(x)
    if (is.na(x_fixed)) {
      return(tibble(ID = character(), N = integer(), RSSI = numeric(), original_row = integer()))
    }
    df <- tryCatch(fromJSON(x_fixed, simplifyDataFrame = TRUE), error = function(e) NULL)
    if (is.null(df) || nrow(as.data.frame(df)) == 0) {
      return(tibble(ID = character(), N = integer(), RSSI = numeric(), original_row = integer()))
    }
    df <- as_tibble(df)
    if (!"ID" %in% names(df)) df$ID <- NA_character_
    if (!"N"  %in% names(df)) df$N  <- NA_integer_
    if (!"RSSI" %in% names(df)) df$RSSI <- NA_real_

    df %>%
      transmute(
        ID = as.character(.data$ID),
        N = suppressWarnings(as.integer(.data$N)),
        RSSI = suppressWarnings(as.numeric(.data$RSSI)),
        original_row = original_row,
        source_field = "antennas"
      )
  }

  parse_base_stations_id_rssi_reps <- function(x, original_row) {
    if (is.na(x) || !nzchar(x)) {
      return(tibble(ID = character(), N = integer(), RSSI = numeric(), original_row = integer()))
    }
    vec <- unlist(strsplit(x, ",", fixed = TRUE))
    if (length(vec) < 3) {
      return(tibble(ID = character(), N = integer(), RSSI = numeric(), original_row = integer()))
    }
    N <- suppressWarnings(as.integer(vec[length(vec)]))
    core <- vec[-length(vec)]
    if (length(core) %% 2 != 0) core <- core[seq_len(length(core) - 1)]
    ids   <- core[c(TRUE, FALSE)]
    rssis <- suppressWarnings(as.numeric(core[c(FALSE, TRUE)]))

    tibble(
      ID = as.character(ids),
      N = N,
      RSSI = rssis,
      original_row = original_row,
      source_field = "base_stations_id_rssi_reps"
    )
  }

  if (!("base_stations_id_rssi_reps" %in% names(b_loc)) && !("antennas" %in% names(b_loc))) {
    stop("Neither 'base_stations_id_rssi_reps' nor 'antennas' found in the data.")
  }

  .msg("Parsing station fields...")
  station_tables <- list()

  if ("base_stations_id_rssi_reps" %in% names(b_loc)) {
    station_tables$bs <- purrr::map2_dfr(
      b_loc$base_stations_id_rssi_reps,
      b_loc$.original_row,
      parse_base_stations_id_rssi_reps
    )
  }
  if ("antennas" %in% names(b_loc)) {
    station_tables$ant <- purrr::map2_dfr(
      b_loc$antennas,
      b_loc$.original_row,
      parse_antennas
    )
  }

  stations_long <- dplyr::bind_rows(station_tables) %>%
    filter(!is.na(ID), nzchar(ID))

  singles_idx <- stations_long %>%
    distinct(original_row, ID) %>%
    count(original_row, name = "n_stations") %>%
    filter(n_stations == 1L) %>%
    pull(original_row)

  single_row_station <- stations_long %>%
    filter(original_row %in% singles_idx) %>%
    distinct(original_row, ID) %>%
    rename(station_id = ID)

  singles_points <- b_loc %>%
    filter(.original_row %in% singles_idx) %>%
    left_join(single_row_station, by = "original_row")

  if (is.na(st_crs(singles_points))) st_crs(singles_points) <- 4326

  stations_summary <- singles_points %>%
    group_by(station_id) %>%
    summarise(
      n_points = n(),
      last_timestamp = max(.data[[time_col]], na.rm = TRUE),
      lon_med = median(st_coordinates(st_cast(geometry, "POINT"))[, "X"], na.rm = TRUE),
      lat_med = median(st_coordinates(st_cast(geometry, "POINT"))[, "Y"], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    st_as_sf(coords = c("lon_med", "lat_med"), crs = st_crs(singles_points), remove = FALSE)

  if (!is.null(out_csv)) {
    write.csv(
      stations_summary %>% st_drop_geometry() %>% arrange(desc(n_points)),
      out_csv,
      row.names = FALSE
    )
    .msg("Wrote CSV: ", out_csv)
  }

  out <- list(
    stations_summary = stations_summary,
    singles_points = singles_points,
    stations_long = stations_long,
    b_all = b_all
  )

  if (!is.null(out_rds)) {
    saveRDS(out, out_rds)
    .msg("Saved RDS: ", out_rds)
  }

  out
}

res <- detect_sigfox_single_base_stations(study_id = c(4731778376, 4731733529, 3115801413))

# Any missing IDs left?
table(is.na(res$b_all$.track_id))

# Top stations by number of points
res$stations_summary %>% sf::st_drop_geometry() %>% head()


res <- detect_sigfox_single_base_stations(
  study_id = c(
    4731778376, 4731733529, 3115801413, 3098965953, 3099062915,
    3097213426, 3099169068, 3115915732, 3127820115, 4584654529
  ),
  out_csv = "../../../Dropbox/MPI/Noctule/Data/base_stations_single.csv",
  out_rds = "../../../Dropbox/MPI/Noctule/Data/base_stations_single.rds"
)

# station points (sf)
res$stations_summary

# all location points that were single-station detections
res$singles_points
