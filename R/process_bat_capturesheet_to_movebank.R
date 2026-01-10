process_bat_capturesheet_to_movebank <- function(
    capture_csv,
    out_csv = NULL,
    tz = "UTC",
    encoding = "latin1",

    location_name,
    tag_counter_pad = 2,

    tag_count_order = c("deploy_time_then_tag", "tag_only"),
    deploy_time_source = c("tag_deployment_time", "capture_time"),
    append_firmware_to_tag_model = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(stringr)
    library(readr)
    library(lubridate)
  })

  tag_count_order    <- match.arg(tag_count_order)
  deploy_time_source <- match.arg(deploy_time_source)

  # ---------------- helpers ----------------

  as_chr <- function(x) {
    x <- as.character(x)
    x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
    x
  }

  as_num <- function(x) suppressWarnings(as.numeric(as.character(x)))

  # normalize time strings: if missing -> "12:00:00"
  fill_time_noon <- function(x) {
    x <- as_chr(x)
    x <- ifelse(is.na(x) | !nzchar(str_trim(x)), "12:00:00", str_trim(x))
    x
  }

  # parse date+time; if date missing -> NA (can't invent date)
  parse_dt <- function(date_str, time_str, tz) {
    date_str <- as_chr(date_str)
    time_str <- fill_time_noon(time_str)

    dt_str <- ifelse(
      is.na(date_str) | !nzchar(str_trim(date_str)),
      NA_character_,
      paste0(str_trim(date_str), " ", time_str)
    )

    suppressWarnings(lubridate::parse_date_time(
      dt_str,
      orders = c(
        "Y-m-d H:M:S", "Y-m-d H:M",
        "d/m/Y H:M:S", "d/m/Y H:M",
        "d-m-Y H:M:S", "d-m-Y H:M"
      ),
      tz = tz
    ))
  }

  fmt_mb_time <- function(dt) {
    ifelse(is.na(dt), NA_character_, format(dt, "%Y-%m-%d %H:%M:%OS3"))
  }

  year_from_dt <- function(deploy_dt, capture_dt, tz) {
    dt <- ifelse(!is.na(deploy_dt), deploy_dt, capture_dt)
    dt <- as.POSIXct(dt, origin = "1970-01-01", tz = tz)
    format(dt, "%y")
  }

  # sanitize ID tokens for use in Movebank animal-id strings
  clean_token <- function(x) {
    x <- as_chr(x)
    x <- str_trim(x)
    x <- ifelse(is.na(x), NA_character_, x)
    # keep letters/digits/_/- only
    x <- ifelse(is.na(x), NA_character_, str_replace_all(x, "[^A-Za-z0-9_\\-]", ""))
    # collapse multiple underscores
    x <- ifelse(is.na(x), NA_character_, str_replace_all(x, "_+", "_"))
    x
  }

  loc_id <- location_name |>
    tolower() |>
    str_replace_all("\\s+", "_") |>
    str_replace_all("[^a-z0-9_\\-]", "")

  # ---------------- read ----------------
  # Force tag IDs and common ID columns to character (avoid 1.21E7 type issues)
  cap <- readr::read_csv(
    capture_csv,
    show_col_types = FALSE,
    locale = readr::locale(encoding = encoding),
    col_types = readr::cols(
      .default        = readr::col_guess(),
      `tag ID`        = readr::col_character(),
      `ring number`   = readr::col_character(),
      `ID_alternate`  = readr::col_character(),
      `animal id`     = readr::col_character()
    )
  )

  get_col <- function(df, name) if (name %in% names(df)) df[[name]] else rep(NA, nrow(df))

  # pick an animal-id source column if present (priority order)
  pick_animal_id <- function(df) {
    candidates <- c(
      "animal id",
      "animal_id",
      "Animal ID",
      "Animal.ID",
      "ID_alternate",
      "id_alternate",
      "ID alternate",
      "ID"
    )
    present <- candidates[candidates %in% names(df)]
    if (length(present) == 0) return(rep(NA_character_, nrow(df)))
    as_chr(df[[present[1]]])
  }

  # ---------------- build all required columns inside one mutate ----------------
  cap <- cap %>%
    mutate(
      # raw strings
      capture_date    = as_chr(get_col(., "capture date")),
      capture_time    = fill_time_noon(get_col(., "capture time")),
      tag_deploy_time = fill_time_noon(get_col(., "tag deployment time")),

      # parsed datetimes
      capture_dt    = parse_dt(capture_date, capture_time, tz),
      deploy_dt_raw = parse_dt(capture_date, tag_deploy_time, tz),

      # choose deploy_dt source INSIDE mutate so it cannot disappear
      deploy_dt = dplyr::case_when(
        deploy_time_source == "capture_time" ~ capture_dt,
        TRUE ~ deploy_dt_raw
      ),
      # hard fallback
      deploy_dt = dplyr::coalesce(deploy_dt, capture_dt),

      # id + taxonomy
      tag_id  = clean_token(get_col(., "tag ID")),
      animal_id_sheet = clean_token(pick_animal_id(.)),  # <-- NEW
      genus   = as_chr(get_col(., "genus")),
      species = as_chr(get_col(., "species")),

      # abbreviation = 1st letter genus + first 3 of species
      species_abbrev = paste0(
        str_to_title(str_sub(genus, 1, 1)),
        str_to_lower(str_sub(species, 1, 3))
      ),

      animal_taxon = dplyr::case_when(
        !is.na(genus) & !is.na(species) ~ paste(genus, species),
        TRUE ~ NA_character_
      ),

      # derived year
      year_yy = year_from_dt(deploy_dt, capture_dt, tz),

      # other fields
      animal_sex  = as_chr(get_col(., "sex")),
      life_stage  = as_chr(get_col(., "age")),
      repro_cond  = as_chr(get_col(., "reproductive state")),

      animal_mass = as_num(get_col(., "weight bat")),
      cap_lat     = as_num(get_col(., "latitude_ycoord")),
      cap_lon     = as_num(get_col(., "longitude_xcoord")),

      attach_type = as_chr(get_col(., "tag attachment")),
      tag_mass    = as_num(get_col(., "tag weight")),
      tag_brand   = as_chr(get_col(., "tag brand")),
      tag_model   = as_chr(get_col(., "tag model")),
      firmware    = as_chr(get_col(., "firmware version")),

      tag_comment   = as_chr(get_col(., "tag comment")),
      comment       = as_chr(get_col(., "comment")),
      deploy_person = as_chr(get_col(., "project contact")),
      study_site    = as_chr(get_col(., "location")),
      roost         = as_chr(get_col(., "roost")),

      tag_model_final = dplyr::if_else(
        append_firmware_to_tag_model & !is.na(tag_model) & !is.na(firmware),
        paste(tag_model, firmware),
        tag_model
      ),

      animal_comments = dplyr::case_when(
        !is.na(comment) & !is.na(tag_comment) ~ paste(comment, "| tag:", tag_comment),
        !is.na(comment) ~ comment,
        !is.na(tag_comment) ~ tag_comment,
        TRUE ~ NA_character_
      ),

      deployment_comments = dplyr::if_else(!is.na(roost), paste0("roost=", roost), NA_character_)
    )

  # ------------------------------------------------------------------
  # Deduplicate by tag_id, keep earliest record
  # ------------------------------------------------------------------
  cap <- cap %>%
    mutate(.dedup_time = dplyr::coalesce(.data$deploy_dt, .data$capture_dt)) %>%
    group_by(.data$tag_id) %>%
    arrange(.data$.dedup_time, .by_group = TRUE) %>%
    slice(1) %>%
    ungroup() %>%
    dplyr::select(-.data$.dedup_time)

  # ---------------- tag counter (deterministic) ----------------
  tag_key <- cap %>%
    filter(!is.na(.data$tag_id)) %>%
    distinct(.data$tag_id, .data$deploy_dt, .keep_all = FALSE)

  tag_key <- if (tag_count_order == "deploy_time_then_tag") {
    arrange(tag_key, .data$deploy_dt, .data$tag_id)
  } else {
    arrange(tag_key, .data$tag_id)
  }

  tag_key <- tag_key %>%
    distinct(.data$tag_id, .keep_all = TRUE) %>%
    mutate(tag_index = row_number())

  cap <- cap %>%
    left_join(tag_key, by = c("tag_id", "deploy_dt")) %>%
    mutate(
      tag_index_str = str_pad(.data$tag_index, width = tag_counter_pad, pad = "0"),

      # NEW animal-id rule:
      # If an animal id exists in the capture sheet, use it:
      #   {species_abbrev}{yy}_{loc}_{animal_id_sheet}_{tag_id}
      # Else fallback to your counter version:
      #   {species_abbrev}{yy}_{loc}_{tag_index}_{tag_id}
      `animal-id` = dplyr::if_else(
        !is.na(.data$animal_id_sheet) & nzchar(.data$animal_id_sheet),
        paste0(.data$species_abbrev, .data$year_yy, "_", loc_id, "_", .data$animal_id_sheet, "_", .data$tag_id),
        paste0(.data$species_abbrev, .data$year_yy, "_", loc_id, "_", .data$tag_index_str, "_", .data$tag_id)
      )
    )

  # ---------------- Movebank reference-data output ----------------
  mb <- cap %>%
    transmute(
      `tag-id` = .data$tag_id,
      `animal-id` = .data$`animal-id`,
      `animal-taxon` = .data$animal_taxon,
      `deploy-on-date` = fmt_mb_time(.data$deploy_dt),
      `deploy-off-date` = NA_character_,

      `animal-comments` = .data$animal_comments,
      `animal-life-stage` = .data$life_stage,  # FIX: was .data$age
      `animal-mass` = .data$animal_mass,
      `animal-reproductive-condition` = .data$repro_cond,
      `animal-ring-id` = clean_token(get_col(cap, "ring number")),
      `animal-sex` = .data$animal_sex,

      `attachment-type` = .data$attach_type,
      `capture-latitude` = .data$cap_lat,
      `capture-longitude` = .data$cap_lon,
      `capture-timestamp` = fmt_mb_time(.data$capture_dt),

      `deploy-off-latitude` = NA_real_,
      `deploy-off-longitude` = NA_real_,

      `deploy-on-latitude` = .data$cap_lat,
      `deploy-on-longitude` = .data$cap_lon,

      `deploy-on-measurements` = NA_character_,
      `deploy-on-person` = .data$deploy_person,
      `deployment-comments` = .data$deployment_comments,
      `deployment-end-type` = NA_character_,
      `habitat-according-to` = NA_character_,
      `manipulation-type` = NA_character_,

      `study-site` = .data$study_site,

      `tag-comments` = .data$tag_comment,
      `tag-manufacturer-name` = .data$tag_brand,
      `tag-mass` = .data$tag_mass,
      `tag-model` = .data$tag_model_final,
      `tag-readout-method` = NA_character_,
      `tag-serial-no` = NA_character_
    )

  if (!is.null(out_csv)) {
    readr::write_csv(mb, out_csv, na = "")
  }

  mb
}


# mb_ref <- process_bat_capturesheet_to_movebank(
#   capture_csv = "../../../Dropbox/MPI/Noctule/Data/Freinat/Fall2025_nanofox/Capture_Data_Autumn25_MPI_Sheet.csv",
#   out_csv     = "../../../Dropbox/MPI/Noctule/Data/Freinat/Fall2025_nanofox/freinat25f-reference-data.csv",
#   tz          = "CET",
#   location_name  = "freinat",
#   deploy_time_source = "tag_deployment_time",
#   tag_count_order = "deploy_time_then_tag"
# )
# mb_ref$`animal-id`
# head(mb_ref)
# t <- mb_ref$`tag-id` %>% table()
# which(t > 1)
