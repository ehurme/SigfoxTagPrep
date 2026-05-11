#' Download and process bat tracking data from Movebank
#'
#' A high-level pipeline that downloads one or more Movebank studies containing
#' NanoFox, TinyFox, or uWasp tag data, computes movement metrics, converts
#' pressure to altitude, and produces three outputs per study: the full
#' multi-sensor object, a location-only object with speed/distance/bearing, and
#' a daily thinned dataset.
#'
#' @param study_id Integer or character vector of Movebank study IDs.
#' @param tag_type Character or named list. When tag type cannot be inferred
#'   automatically from tag metadata, supply it here: a single string for all
#'   studies, or a named list (e.g. \code{list("12345" = "nanofox")}) for
#'   specific studies. Allowed values: \code{"uWasp"}, \code{"nanofox"},
#'   \code{"tinyfox"}.
#' @param sensor_external_ids Character vector of Movebank sensor external IDs
#'   to download. Default covers acceleration, accessory measurements,
#'   barometer, sigfox-geolocation, and derived sensors.
#' @param sensor_labels Character vector (same length as \code{sensor_external_ids})
#'   of human-readable labels used as \code{sensor_type} values in the output.
#' @param merge_studies Logical; if \code{TRUE} (default), stack all downloaded
#'   studies into single merged objects. If \code{FALSE}, return a list per study.
#' @param track_combine Character; strategy for merging track data across studies
#'   in \code{\link[move2]{mt_stack}}. Default \code{"merge"}.
#' @param compute_vedba_sum Logical; compute summed VeDBA per location row via
#'   \code{\link{add_vedba_temp_to_locations}}. Default \code{TRUE}.
#' @param vedba_col Column name of VeDBA values in sensor rows. Default
#'   \code{"vedba"}.
#' @param vedba_sum_name Column name for the resulting sum. Default
#'   \code{"vedba_sum"}.
#' @param run_elevation Logical; retrieve ground elevation via \pkg{elevatr}.
#'   Default \code{TRUE}.
#' @param run_daily_metrics Logical; compute daily solar-noon thinned dataset.
#'   Default \code{TRUE}.
#' @param daily_method Character; method for selecting the daily representative
#'   fix: \code{"solar_noon"} (default), \code{"daytime_only"}, or
#'   \code{"noon_roost"}.
#' @param compute_cum_dist Logical; compute cumulative distance per track.
#'   Default \code{TRUE}.
#' @param verbose Logical; print progress messages. Default \code{TRUE}.
#' @param script_mt_add_start,script_add_min_pressure,script_mt_previous,
#'   script_calc_displacement,script_pressure_to_altitude,script_daily,
#'   script_daily_sensor Paths to helper R scripts. Defaults assume the
#'   \pkg{SigfoxTagPrep} repo is one directory above the working directory.
#' @param tz Time zone string. Default \code{"UTC"}.
#' @return A list with elements:
#' \describe{
#'   \item{\code{sensors_table}}{Full Movebank sensor type table.}
#'   \item{\code{sensors_selected}}{Subset of sensors actually requested.}
#'   \item{\code{studies}}{Per-study list (each with \code{full}, \code{location},
#'     \code{daily}).}
#'   \item{\code{full}}{Merged full multi-sensor move2 object (or list).}
#'   \item{\code{location}}{Merged location-only move2 object (or list).}
#'   \item{\code{daily}}{Merged daily move2 object (or list).}
#' }
#' @examples
#' \dontrun{
#'   out <- import_nanofox_movebank(study_id = 123456789)
#'   bats_loc   <- out$location
#'   bats_daily <- out$daily
#' }
import_nanofox_movebank <- function(
  study_id,
  tag_type = NULL,
  sensor_external_ids = c(
    "acceleration",
    "accessory-measurements",
    "barometer",
    "sigfox-geolocation",
    "derived"
  ),
  sensor_labels = c(
    "VeDBA",
    "avg.temp",
    "min.baro.pressure",
    "location",
    "min.temp"
  ),
  merge_studies = TRUE,
  track_combine = "merge",
  compute_vedba_sum = TRUE,
  vedba_col = "vedba",
  vedba_sum_name = "vedba_sum",
  run_elevation = TRUE,
  run_daily_metrics = TRUE,
  daily_method = c("solar_noon", "daytime_only", "noon_roost"),
  compute_cum_dist = TRUE,
  verbose = TRUE,
  script_mt_add_start = "../SigfoxTagPrep/R/mt_add_start.R",
  script_add_min_pressure = "../SigfoxTagPrep/R/add_min_pressure_to_locations.R",
  script_mt_previous = "../SigfoxTagPrep/R/mt_previous.R",
  script_calc_displacement = "../SigfoxTagPrep/R/calc_displacement.R",
  script_pressure_to_altitude = "../SigfoxTagPrep/R/pressure_to_altitude_m.R",
  script_daily = "../SigfoxTagPrep/R/mt_thin_daily_solar_noon.R",
  script_daily_sensor = "../SigfoxTagPrep/R/mt_add_daily_sensor_metrics.R",
  tz = "UTC"
) {
  suppressPackageStartupMessages({
    require("tidyverse", quietly = TRUE)
    require("dplyr", quietly = TRUE)
    require("tibble", quietly = TRUE)
    require("purrr", quietly = TRUE)
    require("tidyr", quietly = TRUE)
    require("stringr", quietly = TRUE)
    require("lubridate", quietly = TRUE)
    require("move2", quietly = TRUE)
    require("sf", quietly = TRUE)
    require("units", quietly = TRUE)
    require(assertthat, quietly = TRUE)
    if (isTRUE(run_elevation)) {
      require("elevatr", quietly = TRUE)
    }
    if (isTRUE(run_daily_metrics)) require("suncalc", quietly = TRUE)
  })

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  .msg <- function(...) if (isTRUE(verbose)) message(...)
  `%||%` <- function(a, b) if (!is.null(a)) a else b

  .safe_try <- function(expr, what = "step") {
    tryCatch(expr, error = function(e) {
      .msg("\u26a0\ufe0f  ", what, " failed: ", conditionMessage(e))
      NULL
    })
  }

  .norm <- function(x) gsub("\\s+", " ", tolower(trimws(as.character(x))))

  .stop_if_missing <- function(path) {
    if (!file.exists(path)) {
      stop("Missing required script: ", path)
    }
    invisible(TRUE)
  }

  .source_local <- function(path) {
    .stop_if_missing(path)
    source(path)
    invisible(TRUE)
  }

  # Choose the most specific identifier available for movement metrics.
  # Prefer deployment_id, then tag_local_identifier, then individual_local_identifier,
  # and finally the internal move2 track id.
  .metric_group_col <- function(x) {
    for (nm in c(
      "deployment_id",
      "tag_local_identifier",
      "individual_local_identifier"
    )) {
      if (nm %in% names(x) && !all(is.na(x[[nm]]))) return(nm)
    }
    NULL
  }

  # ---------------------------------------------------------------------------
  # .fill_from_td() — fill an event column from track data by direct lookup
  #
  # Safer than mt_as_event_attribute() for species/taxon because it uses
  # mt_track_id() directly and tolerates factor/int64 type mismatches.
  # Non-destructive: only fills NA positions; never overwrites existing values.
  # td_col   : column name to look for in track data (searched in priority order)
  # event_col: column to create/fill on the event data
  # ---------------------------------------------------------------------------
  .fill_from_td <- function(x, td_col, event_col) {
    td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
    if (is.null(td)) {
      return(x)
    }
    taxon_src <- intersect(
      c(
        td_col,
        "taxon_canonical_name",
        "species",
        "individual_taxon_canonical_name"
      ),
      names(td)
    )[1]
    if (is.na(taxon_src)) {
      return(x)
    }
    id_col <- move2::mt_track_id_column(x)
    if (!id_col %in% names(td)) {
      return(x)
    }
    lkp <- stats::setNames(
      as.character(td[[taxon_src]]),
      as.character(td[[id_col]])
    )
    vals <- unname(lkp[as.character(move2::mt_track_id(x))])
    if (!event_col %in% names(x) || all(is.na(x[[event_col]]))) {
      x[[event_col]] <- vals
    } else {
      miss <- is.na(x[[event_col]])
      x[[event_col]][miss] <- vals[miss]
    }
    x
  }

  # Enforce strict time ordering per track (required by mt_speed / mt_distance).
  .dedupe_timestamps <- function(x) {
    # The move2 track id column (e.g. individual_local_identifier) determines
    # which tracks must be contiguous — mt_is_track_id_cleaved requires it.
    # The metric grouping column (e.g. deployment_id) is finer-grained and used
    # for deduplication, but the final sort order must put the move2 track id first
    # so the cleave invariant is satisfied.
    move2_track_col <- move2::mt_track_id_column(x)
    grp_col <- .metric_group_col(x)
    if (is.null(grp_col)) {
      grp_col <- move2_track_col
    }

    # Some studies do not include comments; prefer non-start rows only if present.
    ord_comments <- if ("comments" %in% names(x)) {
      (is.na(x$comments) | x$comments != "start")
    } else {
      rep(TRUE, nrow(x))
    }
    ord_event <- if ("event_id" %in% names(x)) {
      (!is.na(x$event_id))
    } else {
      rep(FALSE, nrow(x))
    }
    x <- x %>% dplyr::mutate(`..dedupe_order` = ord_comments + 0.1 * ord_event)

    out <- x %>%
      dplyr::group_by(.data[[grp_col]], .data$timestamp) %>%
      dplyr::slice_max(
        order_by = .data$`..dedupe_order`,
        n = 1,
        with_ties = FALSE
      ) %>%
      dplyr::ungroup() %>%
      # Sort by move2 track id FIRST (satisfies cleave), then dep_col, then time
      dplyr::arrange(
        .data[[move2_track_col]],
        .data[[grp_col]],
        .data$timestamp
      ) %>%
      dplyr::select(-dplyr::any_of(c("..dedupe_order", "..metric_group_id")))
    out
  }

  # ---------------------------------------------------------------------------
  # .add_start()
  #
  # Inserts one synthetic "start" row per deployment at deploy_on_timestamp,
  # placed at deploy_on_location (the capture/release location).
  #
  # Unlike the sourced mt_add_start(), this implementation:
  #   - Creates one row per DEPLOYMENT (keyed by deployment_id), not per
  #     individual, so multi-deployment individuals get one start row each.
  #   - Fills all event columns (tag_type, sensor_type, individual_local_identifier,
  #     deployment_id, tag_local_identifier, species, sex, etc.) from the first
  #     real location row of that deployment so no columns are NA.
  #   - Sets comments = "start" for downstream deduplication.
  #   - Sets sensor_type = "location" so the row is included in b_loc and
  #     calc_displacement() has a capture-location anchor for each deployment.
  #
  # If deploy_on_timestamp or deploy_on_location are missing from track data,
  # the deployment is skipped (no start row inserted for it).
  # ---------------------------------------------------------------------------
  .add_start <- function(x) {
    require(sf)
    require(dplyr)

    td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
    if (is.null(td)) {
      .msg("  .add_start: no track data found; skipping.")
      return(x)
    }

    # Identify required columns in track data
    has_deploy_ts <- "deploy_on_timestamp" %in% names(td)
    has_deploy_loc <- "deploy_on_location" %in% names(td)
    has_dep_id <- "deployment_id" %in% names(td)

    if (!has_deploy_ts) {
      .msg("  .add_start: deploy_on_timestamp not in track data; skipping.")
      return(x)
    }

    # Track id column used by this move2 object
    track_col <- move2::mt_track_id_column(x)

    # Build a lookup: track_id → first real location row index
    # Used to copy event-column values so the start row has no NAs.
    loc_mask <- if ("sensor_type" %in% names(x)) {
      x$sensor_type == "location" & !is.na(x$sensor_type)
    } else {
      rep(TRUE, nrow(x))
    }
    track_ids_vec <- as.character(move2::mt_track_id(x))

    # For each track, get the index of its first location row
    first_loc_idx <- tapply(
      seq_len(nrow(x))[loc_mask],
      track_ids_vec[loc_mask],
      min
    )

    # td track id column: match the move2 track id so lookups are consistent
    td_id_col <- if (track_col %in% names(td)) track_col else names(td)[1]

    n_inserted <- 0L
    start_rows <- vector("list", nrow(td))

    for (i in seq_len(nrow(td))) {
      track_key <- as.character(td[[td_id_col]][i])

      # deploy_on_timestamp — skip if missing or unparseable
      dep_ts_raw <- td$deploy_on_timestamp[i]
      # May be POSIXct, character (from stringification), or NA
      dep_ts <- tryCatch(
        {
          if (inherits(dep_ts_raw, c("POSIXct", "POSIXlt"))) {
            dep_ts_raw
          } else if (
            is.character(dep_ts_raw) &&
              !is.na(dep_ts_raw) &&
              nchar(trimws(dep_ts_raw)) > 0
          ) {
            lubridate::ymd_hms(dep_ts_raw, tz = "UTC", quiet = TRUE)
          } else {
            NA_real_
          }
        },
        error = function(e) NA_real_
      )
      if (length(dep_ts) == 0 || all(is.na(dep_ts))) {
        next
      }

      # Geometry: prefer deploy_on_location from track data.
      # After .fix_track_data_lists(), geometry columns become character WKT strings.
      # Handle sfc, sfg, character WKT, and numeric/NA gracefully.
      ref_idx <- first_loc_idx[track_key]
      geom <- NULL

      if (has_deploy_loc) {
        loc_raw <- td$deploy_on_location[i]
        geom <- tryCatch(
          {
            if (inherits(loc_raw, "sfc")) {
              sf::st_sfc(loc_raw, crs = sf::st_crs(x))
            } else if (inherits(loc_raw, "sfg")) {
              sf::st_sfc(list(loc_raw), crs = sf::st_crs(x))
            } else if (
              is.character(loc_raw) &&
                !is.na(loc_raw) &&
                nchar(trimws(loc_raw)) > 0 &&
                grepl(
                  "POINT|LINESTRING|POLYGON|GEOMETRY",
                  loc_raw,
                  ignore.case = TRUE
                )
            ) {
              sf::st_as_sfc(loc_raw, crs = sf::st_crs(x))
            } else {
              NULL
            }
          },
          error = function(e) NULL
        )
      }

      # Fall back to first real location fix of the deployment
      if (is.null(geom) || length(geom) == 0) {
        geom <- if (!is.null(ref_idx) && !is.na(ref_idx)) {
          sf::st_geometry(x)[ref_idx]
        } else {
          sf::st_sfc(sf::st_point(), crs = sf::st_crs(x))
        }
      }

      # Build the start row from the reference location row (copies all event columns)
      base_idx <- if (!is.null(ref_idx) && !is.na(ref_idx)) {
        ref_idx
      } else {
        any_idx <- which(track_ids_vec == track_key)[1]
        if (is.na(any_idx)) {
          next
        }
        any_idx
      }
      start_row <- sf::st_as_sf(x[base_idx, , drop = FALSE])

      # Override the deployment-specific fields
      start_row$timestamp <- dep_ts
      if ("comments" %in% names(start_row)) {
        start_row$comments <- "start"
      } else {
        start_row$comments <- "start"
      }
      tryCatch(
        sf::st_geometry(start_row) <- sf::st_sfc(
          geom[[1]],
          crs = sf::st_crs(x)
        ),
        error = function(e) NULL # geometry assignment failed — keep original
      )

      # Ensure deployment_id is on the start row (may have been promoted from track data)
      if (has_dep_id && "deployment_id" %in% names(start_row)) {
        start_row$deployment_id <- as.character(td$deployment_id[i])
      }

      start_rows[[i]] <- start_row
      n_inserted <- n_inserted + 1L
    }

    start_rows <- Filter(Negate(is.null), start_rows)
    if (length(start_rows) == 0) {
      .msg(
        "  .add_start: no valid deploy_on_timestamp found; no start rows inserted."
      )
      return(x)
    }

    new_rows <- tryCatch(
      dplyr::bind_rows(start_rows),
      error = function(e) {
        .msg(
          "  .add_start: bind_rows failed (",
          conditionMessage(e),
          "); skipping."
        )
        NULL
      }
    )
    if (is.null(new_rows)) {
      return(x)
    }

    # Combine original + start rows without calling mt_as_move2().
    # mt_as_move2 would rebuild track data from event rows and error if any
    # start-row deployment_id isn't in the minimal rebuilt track data.
    # Instead: strip move2 class, bind as plain sf, re-sort, re-attach class + track data.
    orig_td_start <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
    x_plain <- sf::st_as_sf(x)
    class(x_plain) <- class(x_plain)[!class(x_plain) %in% "move2"]

    combined <- dplyr::bind_rows(new_rows, x_plain) %>%
      dplyr::arrange(.data[[track_col]], .data$timestamp)

    class(combined) <- c("move2", class(combined))
    attr(combined, "track_id_column") <- track_col
    attr(combined, "time_column") <- "timestamp"
    if (!is.null(orig_td_start)) {
      combined <- tryCatch(
        move2::mt_set_track_data(combined, orig_td_start),
        error = function(e) combined
      )
    }

    .msg("  .add_start: inserted ", n_inserted, " deployment start rows.")
    combined
  }

  add_prev_latlon <- function(x, lat_name = "lat_prev", lon_name = "lon_prev") {
    if (!inherits(x, "move2")) {
      stop("x must be a move2 object")
    }
    coords <- sf::st_coordinates(x)
    track_id <- move2::mt_track_id(x)
    df <- tibble::tibble(
      track_id = track_id,
      lon = coords[, "X"],
      lat = coords[, "Y"]
    ) %>%
      group_by(track_id) %>%
      mutate(!!lon_name := dplyr::lag(lon), !!lat_name := dplyr::lag(lat)) %>%
      ungroup()
    x[[lon_name]] <- df[[lon_name]]
    x[[lat_name]] <- df[[lat_name]]
    x
  }

  .wanted_sensor_ids <- function(study_info, sensor_selected) {
    study_names <- trimws(unlist(strsplit(
      as.character(study_info$sensor_type_ids),
      "\\s*,\\s*"
    )))
    matched <- sensor_selected %>%
      mutate(name_n = .norm(.data$name)) %>%
      dplyr::filter(.data$name_n %in% .norm(study_names))
    wanted_ids <- pull(matched, .data$id)

    # TinyFox studies advertise only "sigfox-geolocation"; all tinyfox_* columns
    # are delivered as extra columns on location rows, not separate sensor rows.
    unmatched <- dplyr::anti_join(sensor_selected, matched, by = "id")
    if (nrow(unmatched) > 0) {
      .msg(
        "  Note: ",
        nrow(unmatched),
        " requested sensor(s) not in study sensor_type_ids ",
        "(likely stored as flat columns): ",
        paste(unmatched$external_id, collapse = ", ")
      )
    }

    list(wanted_ids = wanted_ids, study_sensor_names = study_names)
  }

  .add_event_attrs <- function(x) {
    # Promote track-level metadata to event columns so they survive filtering,
    # merging, and subsetting (e.g. .make_location_metrics filters to location rows).
    # These are the columns most commonly needed for analysis:
    #   species           — biological identity (via .fill_from_td)
    #   sex               — individual attribute
    #   model             — tag hardware model string
    #   tag_firmware      — firmware version on the tag
    #   tag_local_identifier — which physical tag was on the animal
    #   deployment_id     — deployment-level grouping key
    # NOT promoted: tag_type (removed from outputs), taxon_canonical_name (redundant).
    #
    # All factor/ordered columns are coerced to character to prevent level-set
    # conflicts when stacking objects from different studies.

    # ---- Coerce track data int64/factor/ordered → character before joins ----
    td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
    if (!is.null(td)) {
      changed <- FALSE
      for (nm in names(td)) {
        v <- td[[nm]]
        if (inherits(v, "integer64") || is.factor(v) || is.ordered(v)) {
          td[[nm]] <- as.character(v)
          changed <- TRUE
        }
      }
      if (changed) {
        x <- tryCatch(move2::mt_set_track_data(x, td), error = function(e) x)
      }
    }

    # ---- Promote attributes from track data to event columns ----
    for (attr in c(
      "sex",
      "model",
      "tag_firmware",
      "attachment_comments",
      "tag_local_identifier",
      "deployment_id"
    )) {
      x <- .safe_try(
        x %>% mt_as_event_attribute(attr, .keep = TRUE),
        paste0("mt_as_event_attribute(", attr, ")")
      ) %||%
        x
    }

    # ---- species: direct track-data lookup (robust to type mismatches) ----
    x <- .fill_from_td(x, "taxon_canonical_name", "species")
    x$species <- as.character(x$species)

    # ---- Coerce all factor/ordered event columns → character ----
    geom_col <- if (inherits(x, "sf")) attr(x, "sf_column") else character(0)
    for (nm in setdiff(names(x), geom_col)) {
      v <- x[[nm]]
      if (is.factor(v) || is.ordered(v)) {
        x[[nm]] <- as.character(v)
      }
    }

    x
  }

  .set_tag_type <- function(x, study_id_current = NULL) {
    allowed <- c("uWasp", "nanofox", "tinyfox")

    .classify_model <- function(s) {
      s <- as.character(s)
      dplyr::case_when(
        grepl("uWasp|SigfoxGH", s, ignore.case = TRUE) ~ "uWasp",
        grepl("Nano|NanoFox", s, ignore.case = TRUE) ~ "nanofox",
        grepl("Tiny|TinyFox|TinyFoxBat", s, ignore.case = TRUE) ~ "tinyfox",
        TRUE ~ NA_character_
      )
    }

    # 1) Prefer existing event-level tag_type if it is already valid.
    if ("tag_type" %in% names(x) && any(!is.na(x$tag_type))) {
      x$tag_type <- as.character(x$tag_type)
      bad <- !is.na(x$tag_type) & !x$tag_type %in% allowed
      if (any(bad)) {
        x$tag_type[bad] <- NA_character_
      }
      if (any(!is.na(x$tag_type))) {
        .msg(
          "  tag_type: using existing event/data column. Distribution: ",
          paste(
            names(table(x$tag_type, useNA = "no")),
            table(x$tag_type, useNA = "no"),
            sep = "=",
            collapse = ", "
          )
        )
        return(x)
      }
    }

    # 2) Prefer explicit per-study override only when tag_type is not inferable.
    override <- NULL
    if (!is.null(tag_type)) {
      if (length(tag_type) == 1 && !is.na(tag_type)) {
        override <- as.character(tag_type)
      } else if (!is.null(names(tag_type)) && !is.null(study_id_current)) {
        sid <- as.character(study_id_current)
        if (sid %in% names(tag_type)) override <- as.character(tag_type[[sid]])
      }
      if (!is.null(override) && !is.na(override) && !override %in% allowed) {
        stop(
          "Invalid tag_type override for study ",
          study_id_current,
          ": ",
          override,
          ". Allowed values are: ",
          paste(allowed, collapse = ", ")
        )
      }
    }

    # 3) Infer from format_type if available.
    inferred <- NULL
    if ("format_type" %in% names(x) && any(!is.na(x[["format_type"]]))) {
      inferred <- .classify_model(x[["format_type"]])
      if (any(!is.na(inferred))) {
        x$tag_type <- inferred
        .msg(
          "  tag_type: resolved from format_type. Distribution: ",
          paste(
            names(table(x$tag_type, useNA = "no")),
            table(x$tag_type, useNA = "no"),
            sep = "=",
            collapse = ", "
          )
        )
        return(x)
      }
    }

    # 4) Infer from event-level or track-level model metadata.
    model_vec <- NULL
    for (col in c("model", "tag_model")) {
      if (col %in% names(x) && any(!is.na(x[[col]]))) {
        model_vec <- as.character(x[[col]])
        .msg("  tag_type: resolved from event column '", col, "'")
        break
      }
    }
    if (is.null(model_vec)) {
      td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
      if (!is.null(td)) {
        td_model_cols <- intersect(c("tag_model", "model"), names(td))
        if (length(td_model_cols) > 0) {
          td_model_col <- td_model_cols[1]
          track_id_vec <- as.character(move2::mt_track_id(x))
          id_col <- names(td)[1]
          td_lookup <- stats::setNames(
            as.character(td[[td_model_col]]),
            as.character(td[[id_col]])
          )
          model_vec <- td_lookup[track_id_vec]
          .msg(
            "  tag_type: resolved from track data column '",
            td_model_col,
            "'"
          )
        }
      }
    }
    if (!is.null(model_vec)) {
      inferred <- .classify_model(model_vec)
      if (any(!is.na(inferred))) {
        x$tag_type <- inferred
        .msg(
          "  tag_type distribution: ",
          paste(
            names(table(x$tag_type, useNA = "no")),
            table(x$tag_type, useNA = "no"),
            sep = "=",
            collapse = ", "
          )
        )
        return(x)
      }
    }

    # 5) Use explicit override only for studies where metadata are insufficient.
    if (!is.null(override) && !is.na(override)) {
      x$tag_type <- rep(override, nrow(x))
      .msg("  tag_type set explicitly for unresolved study: ", override)
      return(x)
    }

    stop(
      "tag_type could not be inferred for study ",
      as.character(study_id_current),
      ". Supply tag_type as a named vector for only the unresolved studies. ",
      "Allowed values are: ",
      paste(allowed, collapse = ", "),
      "."
    )
  }

  .add_lonlat <- function(x) {
    coords <- sf::st_coordinates(x)
    x$lon <- coords[, 1]
    x$lat <- coords[, 2]
    x
  }

  .calc_dist_bearing <- function(lon1, lat1, lon2, lat2) {
    if (require("geosphere", quietly = TRUE)) {
      list(
        dist_m = geosphere::distHaversine(cbind(lon1, lat1), cbind(lon2, lat2)),
        bearing_deg = (geosphere::bearing(
          cbind(lon1, lat1),
          cbind(lon2, lat2)
        ) +
          360) %%
          360
      )
    } else {
      p1m <- sf::st_transform(
        sf::st_as_sf(
          data.frame(lon = lon1, lat = lat1),
          coords = c("lon", "lat"),
          crs = 4326
        ),
        3857
      )
      p2m <- sf::st_transform(
        sf::st_as_sf(
          data.frame(lon = lon2, lat = lat2),
          coords = c("lon", "lat"),
          crs = 4326
        ),
        3857
      )
      list(
        dist_m = as.numeric(sf::st_distance(p1m, p2m, by_element = TRUE)),
        bearing_deg = (atan2(lon2 - lon1, lat2 - lat1) * 180 / pi + 360) %% 360
      )
    }
  }

  .label_sensor_type <- function(x, sensor_selected) {
    x %>%
      left_join(
        sensor_selected %>% dplyr::select(id, sensor_type, is_location_sensor),
        by = c("sensor_type_id" = "id")
      )
  }

  .fix_track_data_lists <- function(x) {
    # When individual_local_identifier is used as the track ID, Movebank bundles
    # multiple deployments per individual into list-columns in the track data.
    # This is EXPECTED move2 behaviour (per move2 team).
    #
    # Strategy (exactly as recommended by move2 team):
    #   1. For list-columns where every entry is identical → scalar (vec_c head).
    #   2. For list-columns with differing entries → comma-separated string.
    #   3. Leave any remaining list-columns intact — downstream code must tolerate
    #      them.  mt_as_event_attribute() correctly handles list-valued track data.

    if (!any(sapply(mt_track_data(x), rlang::is_bare_list))) {
      return(x)
    }

    .msg("  Track data has list-columns (multiple deployments per individual).")

    # Step 1: collapse columns where all entries are the same value
    x <- x |>
      move2::mutate_track_data(dplyr::across(
        dplyr::where(
          ~ rlang::is_bare_list(.x) &&
            all(purrr::map_lgl(.x, function(y) length(unique(y)) == 1L))
        ),
        ~ do.call(vctrs::vec_c, purrr::map(.x, utils::head, 1L))
      ))

    # Step 2: stringify columns where entries differ across deployments
    if (any(sapply(mt_track_data(x), rlang::is_bare_list))) {
      x <- x |>
        move2::mutate_track_data(dplyr::across(
          dplyr::where(
            ~ rlang::is_bare_list(.x) &&
              any(purrr::map_lgl(.x, function(y) length(unique(y)) != 1L))
          ),
          ~ unlist(purrr::map(.x, paste, collapse = ","))
        ))
    }

    remaining <- names(mt_track_data(x))[sapply(
      mt_track_data(x),
      rlang::is_bare_list
    )]
    if (length(remaining) > 0) {
      .msg(
        "  Track data: ",
        length(remaining),
        " list-column(s) retained (entries differ and cannot be stringified): ",
        paste(remaining, collapse = ", ")
      )
    } else {
      .msg("  Track data list-columns resolved.")
    }

    x
  }

  # ---------------------------------------------------------------------------
  # .add_altitude_from_pressure_col()
  #
  # Converts any pressure column to altitude_m using the ISA hypsometric formula:
  #   altitude_m = 44330 * (1 - (P / 1013.25) ^ (1/5.255))
  #
  # Handles both tag types with a single unified approach:
  #
  #   TinyFox (flat schema): tinyfox_pressure_min_last_24h [mbar] is a column
  #     on every location row. Convert it directly — no grouping, no joining.
  #     Each row carries its own pressure value.
  #
  #   NanoFox (separate-row schema): min_3h_pressure [mbar] is joined onto
  #     location rows by add_min_pressure_to_locations() before this function.
  #     Convert it directly in the same way.
  #
  # Priority when both columns exist (mixed-tag study):
  #   NanoFox min_3h_pressure > TinyFox tinyfox_pressure_min_last_24h
  #
  # Writes altitude_m [m] to x. Never overwrites an existing non-NA value.
  # ---------------------------------------------------------------------------
  .add_altitude_from_pressure_col <- function(
    x,
    P0 = 1013.25,
    p_min = 396,
    p_max = 1100
  ) {
    # p_min / p_max: valid pressure range in mbar.
    #   Values outside this range are sensor errors and are set to NA before
    #   conversion so they never produce altitude_m values.
    #   Default range 500–1100 mbar corresponds to ~5600m–sea level altitude.
    #   The known error value 395 mbar falls well below p_min and is caught here.
    pressure_to_alt <- function(P) {
      44330 * (1 - (as.numeric(P) / P0)^(1 / 5.255))
    }

    .mask_pressure <- function(col_name) {
      if (!col_name %in% names(x)) {
        return(invisible(NULL))
      }
      raw <- as.numeric(x[[col_name]])
      bad <- !is.na(raw) & (raw < p_min | raw > p_max)
      if (any(bad, na.rm = TRUE)) {
        x[[col_name]][bad] <<- NA_real_
        .msg(
          "  pressure: ",
          sum(bad, na.rm = TRUE),
          " error value(s) in ",
          col_name,
          " set to NA (outside ",
          p_min,
          "–",
          p_max,
          " mbar)."
        )
      }
    }
    for (pc in c(
      "barometric_pressure",
      "min_3h_pressure",
      "tinyfox_pressure_min_last_24h"
    )) {
      .mask_pressure(pc)
    }

    # Initialise altitude_m if absent
    if (!"altitude_m" %in% names(x)) {
      x$altitude_m <- NA_real_
    }
    existing <- !is.na(as.numeric(x$altitude_m))

    # barometric_pressure (NanoFox multi-sensor schema, joined from sensor rows)
    if ("barometric_pressure" %in% names(x)) {
      fill <- !existing & !is.na(as.numeric(x$barometric_pressure))
      if (any(fill, na.rm = TRUE)) {
        x$altitude_m[fill] <- pressure_to_alt(x$barometric_pressure[fill])
        existing <- existing | fill
        .msg(
          "  altitude_m: ",
          sum(fill, na.rm = TRUE),
          " rows filled from barometric_pressure."
        )
      }
    }

    # min_3h_pressure (NanoFox legacy column name)
    if ("min_3h_pressure" %in% names(x)) {
      fill <- !existing & !is.na(as.numeric(x$min_3h_pressure))
      if (any(fill, na.rm = TRUE)) {
        x$altitude_m[fill] <- pressure_to_alt(x$min_3h_pressure[fill])
        existing <- existing | fill
        .msg(
          "  altitude_m: ",
          sum(fill, na.rm = TRUE),
          " rows filled from min_3h_pressure."
        )
      }
    }

    # tinyfox_pressure_min_last_24h (TinyFox flat column on location rows)
    if ("tinyfox_pressure_min_last_24h" %in% names(x)) {
      fill <- !existing & !is.na(as.numeric(x$tinyfox_pressure_min_last_24h))
      if (any(fill, na.rm = TRUE)) {
        x$altitude_m[fill] <- pressure_to_alt(x$tinyfox_pressure_min_last_24h[
          fill
        ])
        existing <- existing | fill
        .msg(
          "  altitude_m: ",
          sum(fill, na.rm = TRUE),
          " rows filled from tinyfox_pressure_min_last_24h."
        )
      }
    }

    # Attach units
    x$altitude_m <- units::set_units(as.numeric(x$altitude_m), "m")
    x
  }

  # .expand_tinyfox_rows()
  #
  # TinyFox downloads store all sensor data as extra columns on location rows.
  # This function unpacks them into separate rows — one per sensor measurement
  # per transmission — so the full object `b` has the same tidy structure as a
  # NanoFox multi-sensor download.
  #
  # Columns processed:
  #
  #   tinyfox_vedba_NNh_ago  [m/s^2]
  #     One row per 1-hour bin.  The suffix NN gives the number of hours before
  #     transmission that the window ENDS.
  #       time_end   = timestamp - (NN - 1) hours
  #       time_start = timestamp - NN hours
  #     sensor_type = "VeDBA"
  #
  #   tinyfox_total_vedba  [m/s^2]
  #     One row covering the full 22-hour activity window.
  #       time_start = timestamp - 22 hours,  time_end = timestamp
  #     sensor_type = "VeDBA"
  #
  #   tinyfox_pressure_min_last_24h  [mbar]
  #     One row, 24-hour window.
  #     sensor_type = "min.baro.pressure"
  #
  #   tinyfox_temperature_min_last_24h / tinyfox_temperature_max_last_24h  [°C]
  #     One row each, 24-hour window.
  #     sensor_type = "avg.temp"
  #
  #   tinyfox_activity_percent_last_24h  [%]
  #     One row, 24-hour window.
  #     sensor_type = "activity"
  #
  # All new rows inherit:
  #   - geometry from their source location row
  #   - individual_local_identifier, tag_type, and all track-level fields
  #   - timestamp set to time_end (Movebank convention)
  #   - all tinyfox_* columns set to NA (those belong only to the location row)
  #
  # The original location rows also receive time_start = time_end = timestamp.
  #
  # Returns a move2/sf object with the original rows plus the new sensor rows,
  # sorted by individual and timestamp.
  # ---------------------------------------------------------------------------
  .expand_tinyfox_rows <- function(x) {
    # Only run if the characteristic column exists
    if (
      !"tinyfox_pressure_min_last_24h" %in% names(x) &&
        !any(grepl("^tinyfox_vedba_\\d+h_ago$", names(x)))
    ) {
      .msg("  .expand_tinyfox_rows: no TinyFox sensor columns found; skipping.")
      return(x)
    }

    require(sf)
    require(dplyr)
    require(tidyr)
    require(lubridate)

    # Strip units from all columns before building sub-tibbles.
    # Mixed-study downloads may have units-classed columns (e.g. vedba [standard_free_fall])
    # alongside sub-tibbles that build the same column as plain double via as.numeric().
    # bind_rows errors on <units> vs <double> clash; strip upfront so every column
    # in both x and all sub-tibbles is plain numeric.
    x <- .strip_units_cols(x)

    # Identify column groups
    vedba_h_cols <- sort(names(x)[grepl("^tinyfox_vedba_\\d+h_ago$", names(x))])
    total_v_col <- if ("tinyfox_total_vedba" %in% names(x)) {
      "tinyfox_total_vedba"
    } else {
      NULL
    }
    pres_col <- if ("tinyfox_pressure_min_last_24h" %in% names(x)) {
      "tinyfox_pressure_min_last_24h"
    } else {
      NULL
    }
    tmin_col <- if ("tinyfox_temperature_min_last_24h" %in% names(x)) {
      "tinyfox_temperature_min_last_24h"
    } else {
      NULL
    }
    tmax_col <- if ("tinyfox_temperature_max_last_24h" %in% names(x)) {
      "tinyfox_temperature_max_last_24h"
    } else {
      NULL
    }
    act_col <- if ("tinyfox_activity_percent_last_24h" %in% names(x)) {
      "tinyfox_activity_percent_last_24h"
    } else {
      NULL
    }

    # All tinyfox_* columns that will be NA'd on non-location rows
    all_tf_cols <- c(
      vedba_h_cols,
      total_v_col,
      pres_col,
      tmin_col,
      tmax_col,
      act_col,
      "tinyfox_tag_activation"
    ) %>%
      intersect(names(x))

    # Helper: build a derived-sensor tibble from one scalar column
    # Each source row → one output row with its own time window
    .make_scalar_rows <- function(
      src_col,
      value_col_name,
      sensor_lbl,
      window_hours_start,
      window_hours_end = 0
    ) {
      if (is.null(src_col) || !src_col %in% names(x)) {
        return(NULL)
      }
      vals <- as.numeric(x[[src_col]])
      keep <- !is.na(vals)
      if (!any(keep)) {
        return(NULL)
      }

      sf::st_as_sf(tibble::tibble(
        .src_row = seq_len(nrow(x))[keep],
        individual_local_identifier = x$individual_local_identifier[keep],
        timestamp = x$timestamp[keep] -
          lubridate::dhours(window_hours_end),
        time_start = x$timestamp[keep] -
          lubridate::dhours(window_hours_start),
        time_end = x$timestamp[keep] -
          lubridate::dhours(window_hours_end),
        sensor_type = sensor_lbl,
        !!value_col_name := vals[keep],
        geometry = sf::st_geometry(x)[keep]
      ))
    }

    # ---- 1. Hourly VeDBA bins ----
    # tinyfox_vedba_NNh_ago → N hours before tx is when the window ends.
    # Window: [tx - NN h, tx - (NN-1) h]
    vedba_h_rows <- NULL
    if (length(vedba_h_cols) > 0) {
      vedba_h_rows <- lapply(vedba_h_cols, function(col) {
        nn <- as.integer(sub("tinyfox_vedba_(\\d+)h_ago", "\\1", col))
        vals <- as.numeric(x[[col]])
        keep <- !is.na(vals)
        if (!any(keep)) {
          return(NULL)
        }

        sf::st_as_sf(tibble::tibble(
          .src_row = seq_len(nrow(x))[keep],
          individual_local_identifier = x$individual_local_identifier[keep],
          time_end = x$timestamp[keep] - lubridate::dhours(nn - 1L),
          time_start = x$timestamp[keep] - lubridate::dhours(nn),
          timestamp = x$timestamp[keep] - lubridate::dhours(nn - 1L),
          sensor_type = "VeDBA",
          vedba = vals[keep],
          geometry = sf::st_geometry(x)[keep]
        ))
      })
      vedba_h_rows <- dplyr::bind_rows(Filter(Negate(is.null), vedba_h_rows))
    }

    # ---- 2. Total VeDBA (full 22-hour window) ----
    total_v_rows <- .make_scalar_rows(
      total_v_col,
      "vedba",
      "VeDBA",
      window_hours_start = 22,
      window_hours_end = 0
    )

    # ---- 3. Pressure ----
    pres_rows <- .make_scalar_rows(
      pres_col,
      "barometric_pressure",
      "min.baro.pressure",
      window_hours_start = 24,
      window_hours_end = 0
    )

    # ---- 4. Temperature (min and max as separate rows) ----
    tmin_rows <- .make_scalar_rows(
      tmin_col,
      "external_temperature_min",
      "avg.temp",
      window_hours_start = 24,
      window_hours_end = 0
    )
    tmax_rows <- .make_scalar_rows(
      tmax_col,
      "external_temperature_max",
      "avg.temp",
      window_hours_start = 24,
      window_hours_end = 0
    )

    # ---- 5. Activity ----
    act_rows <- .make_scalar_rows(
      act_col,
      "activity_percent",
      "activity",
      window_hours_start = 24,
      window_hours_end = 0
    )

    # ---- 6. Add time_start / time_end to original location rows ----
    x$time_start <- x$timestamp
    x$time_end <- x$timestamp
    # NA out tinyfox sensor columns on location rows is intentional —
    # they are now represented as dedicated rows.
    # (Keep them on location rows for backward compatibility; mark with a note.)

    # ---- 7. Collect all new sensor rows ----
    new_rows_list <- Filter(
      Negate(is.null),
      list(
        vedba_h_rows,
        total_v_rows,
        pres_rows,
        tmin_rows,
        tmax_rows,
        act_rows
      )
    )

    if (length(new_rows_list) == 0) {
      .msg("  .expand_tinyfox_rows: no non-NA sensor values to expand.")
      return(x)
    }

    # Helper: create an NA vector that matches the class/units of a source column.
    # Handles units, factors, and plain vectors safely without vctrs::vec_cast
    # (which errors on geometry and other complex column types).
    .typed_na <- function(src, n) {
      if (inherits(src, "units")) {
        return(units::set_units(
          rep(NA_real_, n),
          units::deparse_unit(src),
          mode = "standard"
        ))
      }
      if (is.factor(src)) {
        return(factor(rep(NA_character_, n), levels = levels(src)))
      }
      if (is.integer(src)) {
        return(rep(NA_integer_, n))
      }
      if (is.numeric(src)) {
        return(rep(NA_real_, n))
      }
      if (is.logical(src)) {
        return(rep(NA, n))
      }
      if (is.character(src)) {
        return(rep(NA_character_, n))
      }
      if (inherits(src, "POSIXct")) {
        return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
      }
      if (inherits(src, "sfc")) {
        # geometry — copy a null/empty geometry column
        return(sf::st_sfc(
          lapply(seq_len(n), function(...) sf::st_point()),
          crs = sf::st_crs(src)
        ))
      }
      rep(NA, n) # fallback
    }

    # Align all sub-tibbles in new_rows_list to the same column schema as x
    # BEFORE binding, so the internal bind_rows sees consistent types.
    x_cols <- names(x)
    x_sf <- sf::st_drop_geometry(.strip_units_cols(x)) # drop geometry + units for type reference

    new_rows_list <- lapply(new_rows_list, function(sub) {
      n <- nrow(sub)
      # Add columns present in x but missing from this sub-tibble
      for (col in setdiff(x_cols, c(names(sub), "geometry"))) {
        src <- x_sf[[col]]
        if (is.null(src)) {
          next
        }
        sub[[col]] <- .typed_na(src, n)
      }
      # Remove any extra columns not in x (value columns like "vedba", "barometric_pressure")
      # are kept as they will become NAs in the other sub-tibbles
      sub
    })

    # Now bind — all sub-tibbles share the same base columns with matching types
    new_rows <- dplyr::bind_rows(new_rows_list)

    # Zero out tinyfox_* columns on new rows (those values live on location rows)
    for (col in intersect(all_tf_cols, names(new_rows))) {
      new_rows[[col]] <- .typed_na(x[[col]], nrow(new_rows))
    }

    # Final column alignment: add any remaining x-columns still missing
    for (col in setdiff(names(x), names(new_rows))) {
      src <- x[[col]]
      if (inherits(src, "sfc")) {
        next
      } # geometry handled by st_as_sf below
      new_rows[[col]] <- .typed_na(src, nrow(new_rows))
    }
    # Reorder to match x (geometry column may differ in position — that's fine)
    shared_cols <- intersect(names(x), names(new_rows))
    new_rows <- new_rows[, shared_cols, drop = FALSE]

    # ---- 8. Stack original + new rows and re-cast to move2 ----
    # Use only columns present in both to avoid geometry/type conflicts.
    x_out <- sf::st_as_sf(x)[, shared_cols, drop = FALSE]
    new_rows_sf <- sf::st_as_sf(new_rows)
    # Ensure new_rows_sf has the geometry column from their source rows
    if (!"geometry" %in% names(new_rows_sf)) {
      sf::st_geometry(new_rows_sf) <- sf::st_geometry(x)[
        new_rows$.src_row %% nrow(x) + 1L
      ]
    }

    combined <- dplyr::bind_rows(x_out, new_rows_sf) %>%
      dplyr::arrange(individual_local_identifier, timestamp)

    # Re-establish move2 class using the same track id as the input object.
    # Do NOT hardcode individual_local_identifier — the input is keyed to deployment_id.
    orig_track_col_expand <- move2::mt_track_id_column(x)
    orig_td_expand <- tryCatch(move2::mt_track_data(x), error = function(e) {
      NULL
    })
    class(combined) <- c("move2", class(combined))
    attr(combined, "track_id_column") <- orig_track_col_expand
    attr(combined, "time_column") <- "timestamp"
    if (!is.null(orig_td_expand)) {
      combined <- tryCatch(
        move2::mt_set_track_data(combined, orig_td_expand),
        error = function(e) combined
      )
    }

    n_new <- nrow(combined) - nrow(x)
    .msg(
      "  .expand_tinyfox_rows: added ",
      n_new,
      " sensor rows. ",
      "Total rows: ",
      nrow(combined),
      " | sensor_type distribution:"
    )
    if (isTRUE(verbose)) {
      print(table(combined$sensor_type, useNA = "ifany"))
    }

    combined
  }

  # ---------------------------------------------------------------------------
  # .add_delta_altitude()
  #
  # Computes altitude change (m) between consecutive location fixes per track.
  #   Positive = ascending, negative = descending.
  #   Requires `altitude_m` (numeric, metres) from add_altitude_from_pressure().
  #   Result column `delta_altitude_m` carries explicit units = "m" via the
  #   units package, so downstream division by dt gives a unit-safe climb rate.
  # ---------------------------------------------------------------------------
  .add_delta_altitude <- function(x) {
    if (!"altitude_m" %in% names(x)) {
      warning(".add_delta_altitude: 'altitude_m' column not found; skipping.")
      return(x)
    }

    # Group by the move2 track id (which should already be deployment_id when
    # called from .make_location_metrics after re-keying).
    group_vec <- as.character(move2::mt_track_id(x))

    delta <- tibble::tibble(
      track_id = group_vec,
      altitude_m = as.numeric(x$altitude_m)
    ) %>%
      dplyr::group_by(track_id) %>%
      dplyr::mutate(delta_altitude_m = altitude_m - dplyr::lag(altitude_m)) %>%
      dplyr::ungroup()

    x$delta_altitude_m <- units::set_units(delta$delta_altitude_m, "m")

    .msg(
      "  delta_altitude_m computed (",
      sum(!is.na(delta$delta_altitude_m)),
      " non-NA values)."
    )
    x
  }

  # ---------------------------------------------------------------------------
  # .make_location_metrics()
  #
  # Computes per-fix movement metrics on a location-only move2 object.
  #
  # Speed        : km/h  — "km/h" is the correct SI-derived units symbol.
  #               "km/hr" is not recognised by the units package and will error.
  # Distance      : km
  # dt            : seconds (numeric; used as-is for climb-rate arithmetic)
  # cum_dist_km   : cumulative distance from first fix per track (km, plain numeric)
  #                 NA at the first fix; controlled by compute_cum_dist parameter.
  # delta_altitude_m: metres with explicit units (via .add_delta_altitude)
  # ---------------------------------------------------------------------------
  .make_location_metrics <- function(x) {
    b_loc <- x %>%
      dplyr::filter(
        .data$sensor_type == "location",
        !sf::st_is_empty(.data$geometry)
      )

    b_loc <- .dedupe_timestamps(b_loc)
    b_loc$year <- lubridate::year(b_loc$timestamp)

    # ---- Deployment-level grouping vector for metric computation ----
    # mt_speed(), mt_distance(), etc. group by the move2 track id.  We need them
    # to respect deployment boundaries (not individual boundaries) so that metrics
    # don't bridge across tag changes.
    #
    # We do NOT re-key the move2 object (which rebuilds track data from scratch and
    # discards all metadata).  Instead we use deployment_id (or the finest available
    # grouping column) as a plain character vector for the ave()-based calculations
    # that we control, while keeping the move2 track id as-is for move2 functions.
    #
    # For move2 functions (mt_speed, mt_distance, mt_azimuth, mt_time_lags,
    # calc_displacement) we temporarily re-key by SWAPPING the track id attribute
    # on the move2 object — NOT via mt_as_move2() which drops track data.
    dep_col <- .metric_group_col(b_loc) # deployment_id > tag_local_id > individual_local_id
    track_col <- move2::mt_track_id_column(b_loc)

    if (
      !is.null(dep_col) &&
        dep_col != track_col &&
        dep_col %in% names(b_loc) &&
        !all(is.na(b_loc[[dep_col]]))
    ) {
      b_loc[[dep_col]] <- as.character(b_loc[[dep_col]])

      # Swap the track id attribute directly — preserves all track data
      attr(b_loc, "track_id_column") <- dep_col

      .msg(
        "  Metrics grouped by: ",
        dep_col,
        " (",
        dplyr::n_distinct(b_loc[[dep_col]], na.rm = TRUE),
        " deployments)"
      )
    }

    if (!move2::mt_is_time_ordered(b_loc)) {
      stop(
        "Location data still not strictly time-ordered within track after dedupe."
      )
    }

    # Ensure the deployment grouping column is a plain character vector so sourced
    # scripts that do filter(deployment_id %in% tracks) work without type mismatch
    dep_col_now <- move2::mt_track_id_column(b_loc)
    if (dep_col_now %in% names(b_loc)) {
      b_loc[[dep_col_now]] <- as.character(b_loc[[dep_col_now]])
    }

    b_loc <- b_loc %>%
      mutate(
        azimuth = mt_azimuth(.),
        speed = mt_speed(., units = "km/h"), # km/h: valid SI symbol
        distance = mt_distance(., units = "km"),
        dt = mt_time_lags(., "secs")
      )

    .source_local(script_mt_previous)
    b_loc <- b_loc %>%
      mt_add_prev_metrics(
        dist_units = "km",
        speed_units = "km/h",
        time_units = "secs"
      )

    .source_local(script_calc_displacement)
    b_loc <- calc_displacement(b_loc)

    # ---- Cumulative distance — grouped by deployment_id ----
    if (isTRUE(compute_cum_dist) && "distance" %in% names(b_loc)) {
      # Group by the finest deployment key (dep_col), not individual
      track_id_vec <- as.character(b_loc[[move2::mt_track_id_column(b_loc)]])
      dist_numeric <- as.numeric(b_loc$distance)
      cum_dist <- ave(
        ifelse(is.na(dist_numeric), 0, dist_numeric),
        track_id_vec,
        FUN = cumsum
      )
      first_fix <- !duplicated(track_id_vec)
      cum_dist[first_fix] <- NA_real_
      b_loc$cum_dist_km <- cum_dist
    }

    # ---- Altitude from pressure ----
    b_loc <- .add_altitude_from_pressure_col(b_loc)

    # ---- Ground elevation from AWS DEM (elevatr) ----
    if (isTRUE(run_elevation) && requireNamespace("elevatr", quietly = TRUE)) {
      .safe_try({
        idx <- which(!sf::st_is_empty(b_loc$geometry) & !is.na(b_loc$lon))
        if (length(idx) > 0) {
          elev_pts <- data.frame(x = b_loc$lon[idx], y = b_loc$lat[idx])
          elev_vals <- elevatr::get_elev_point(
            locations = elev_pts, src = "aws", prj = 4326
          )$elevation
          b_loc$elevation <- NA_real_
          b_loc$elevation[idx] <- elev_vals
          .msg("  elevation: ", sum(!is.na(elev_vals)), " values fetched from AWS DEM.")
        }
      }, "get_elev_point")
    }

    # ---- Delta altitude — grouped by deployment_id ----
    b_loc <- .safe_try(.add_delta_altitude(b_loc), "add_delta_altitude") %||%
      b_loc

    # ---- Restore original track id attribute ----
    # Swap back by setting the attribute directly — no mt_as_move2() call,
    # so track data is preserved exactly as it was.
    if (
      move2::mt_track_id_column(b_loc) != track_col &&
        track_col %in% names(b_loc)
    ) {
      attr(b_loc, "track_id_column") <- track_col
    }

    b_loc
  }

  # ---------------------------------------------------------------------------
  # .add_location_sensor_metrics()
  # Join per-transmission VeDBA sum, mean temperature, minimum pressure, and
  # maximum altitude onto location-only rows from the full multi-sensor object.
  #
  # NanoFox: all sensor rows (VeDBA, avg.temp, min.baro.pressure) share the
  #   location row timestamp, so a group_by(individual, timestamp) join works.
  # TinyFox: pressure and temperature expanded rows share the location timestamp
  #   (window_hours_end = 0). VeDBA hourly bins do NOT all share it — the 1h-ago
  #   bin also has timestamp = tx_timestamp, so summing VeDBA rows would
  #   double-count. Patch vedba_sum and avg_temp from flat columns instead.
  #
  # All aggregation runs on sf::st_drop_geometry() to avoid MULTIPOINT geometry.
  # New columns are assigned directly to the move2 object (no geometry touched).
  # ---------------------------------------------------------------------------
  .add_location_sensor_metrics <- function(b_full, b_loc) {
    if (!"sensor_type" %in% names(b_full)) return(b_loc)
    join_cols <- c("individual_local_identifier", "timestamp")
    if (!all(join_cols %in% names(b_full))) return(b_loc)

    df <- sf::st_drop_geometry(b_full)

    has_vedba <- "vedba" %in% names(df)
    has_temp  <- "external_temperature" %in% names(df)
    has_pres  <- "barometric_pressure" %in% names(df)
    has_alt   <- "altitude_m" %in% names(df)

    agg <- df %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(join_cols))) %>%
      dplyr::summarise(
        vedba_sum = if (has_vedba) {
          v <- as.numeric(vedba[sensor_type == "VeDBA"])
          v <- v[!is.na(v)]
          if (length(v) == 0L) NA_real_ else sum(v)
        } else NA_real_,
        avg_temp = if (has_temp) {
          t <- as.numeric(external_temperature[sensor_type == "avg.temp"])
          t <- t[!is.na(t)]
          if (length(t) == 0L) NA_real_ else mean(t)
        } else NA_real_,
        min_pressure = if (has_pres) {
          p <- as.numeric(barometric_pressure[sensor_type == "min.baro.pressure"])
          p <- p[!is.na(p)]
          if (length(p) == 0L) NA_real_
          else {
            pv <- min(p)
            dplyr::if_else(is.infinite(pv), NA_real_, pv)
          }
        } else NA_real_,
        max_altitude_m = if (has_alt) {
          a <- as.numeric(altitude_m)
          a <- a[!is.na(a)]
          if (length(a) == 0L) NA_real_
          else {
            av <- max(a)
            dplyr::if_else(is.infinite(av), NA_real_, av)
          }
        } else NA_real_,
        .groups = "drop"
      )

    # Safe join: extract attribute table only, use .row_id, assign columns
    # directly to the move2 object — geometry is never touched.
    key <- sf::st_drop_geometry(b_loc) %>%
      tibble::as_tibble() %>%
      dplyr::select(dplyr::all_of(join_cols)) %>%
      dplyr::mutate(.row_id = dplyr::row_number())

    joined <- dplyr::left_join(key, agg, by = join_cols) %>%
      dplyr::arrange(.row_id)

    new_cols <- c("vedba_sum", "avg_temp", "min_pressure", "max_altitude_m")
    for (nm in new_cols) {
      if (nm %in% names(b_loc)) b_loc[[nm]] <- NULL
    }
    for (nm in new_cols) {
      b_loc[[nm]] <- joined[[nm]]
    }

    # ---- TinyFox flat-column patches ----
    # vedba_sum: summing VeDBA sensor rows double-counts because the 1h-ago bin
    # and the total-VeDBA row both have timestamp = tx_timestamp.  Use the flat
    # column tinyfox_total_vedba (retained on location rows after expansion).
    if ("tinyfox_total_vedba" %in% names(b_loc) && "tag_type" %in% names(b_loc)) {
      is_tiny <- !is.na(b_loc$tag_type) &
        tolower(as.character(b_loc$tag_type)) == "tinyfox"
      if (any(is_tiny)) {
        b_loc$vedba_sum[is_tiny] <- as.numeric(b_loc$tinyfox_total_vedba[is_tiny])
      }
    }
    # avg_temp: expanded TinyFox temp rows use external_temperature_min/max, not
    # external_temperature — so the sensor-row mean is NA.  Patch from flat columns.
    has_tmin <- "tinyfox_temperature_min_last_24h" %in% names(b_loc)
    has_tmax <- "tinyfox_temperature_max_last_24h" %in% names(b_loc)
    if ((has_tmin || has_tmax) && "tag_type" %in% names(b_loc)) {
      is_tiny <- !is.na(b_loc$tag_type) &
        tolower(as.character(b_loc$tag_type)) == "tinyfox"
      if (any(is_tiny)) {
        tmin <- if (has_tmin) as.numeric(b_loc$tinyfox_temperature_min_last_24h[is_tiny]) else rep(NA_real_, sum(is_tiny))
        tmax <- if (has_tmax) as.numeric(b_loc$tinyfox_temperature_max_last_24h[is_tiny]) else rep(NA_real_, sum(is_tiny))
        avg_vals <- rowMeans(cbind(tmin, tmax), na.rm = TRUE)
        avg_vals[is.nan(avg_vals)] <- NA_real_
        b_loc$avg_temp[is_tiny] <- avg_vals
      }
    }

    .msg(
      "  location sensor metrics: vedba_sum=", sum(!is.na(b_loc$vedba_sum)),
      " avg_temp=", sum(!is.na(b_loc$avg_temp)),
      " min_pressure=", sum(!is.na(b_loc$min_pressure)),
      " max_altitude_m=", sum(!is.na(b_loc$max_altitude_m)),
      " rows"
    )
    b_loc
  }

  # ---------------------------------------------------------------------------
  # .detect_tag_fell_off_loc()
  # Per-deployment tag-fell-off detection on the location-only object.
  # Routes to detect_tag_fell_off() with method- and column-appropriate
  # arguments depending on the tag_type of each deployment track.
  #
  # NanoFox: vedba_sum > 0 (or vedba_threshold) → active
  # TinyFox: vedba_sum > tinyfox_vedba_threshold OR activity_percent > 0 → active
  # uWasp:   distance > dist_threshold → active (noisy; marked as best-effort)
  #
  # The first post-active location row is kept as tag_fell_off = FALSE because
  # the tag physically moved to that coordinate before detaching.
  # ---------------------------------------------------------------------------
  .detect_tag_fell_off_loc <- function(
      b_loc,
      nanofox_vedba_threshold  = 0,
      tinyfox_vedba_threshold  = 280800 * 3.9 / 1000 / (60 * 24),
      tinyfox_activity_threshold = 0,
      uwasp_dist_threshold     = 0.5,   # km — above Sigfox jitter radius
      min_inactive_run         = 3L
  ) {
    if (!"tag_type" %in% names(b_loc)) return(b_loc)

    track_col <- move2::mt_track_id_column(b_loc)
    b_loc$tag_fell_off <- FALSE

    tracks <- unique(as.character(b_loc[[track_col]]))

    for (trk in tracks) {
      idx <- which(as.character(b_loc[[track_col]]) == trk)
      if (length(idx) <= 1L) next

      tt <- tolower(as.character(
        na.omit(b_loc$tag_type[idx])[1]
      ))
      if (length(tt) == 0L || is.na(tt)) next

      sub <- b_loc[idx, , drop = FALSE]

      if (tt == "nanofox") {
        if (!"vedba_sum" %in% names(sub)) next
        sub <- detect_tag_fell_off(
          sub,
          method           = "nanofox",
          tag_col          = track_col,
          vedba_col        = "vedba_sum",
          vedba_threshold  = nanofox_vedba_threshold,
          min_inactive_run = min_inactive_run
        )

      } else if (tt == "tinyfox") {
        sub <- detect_tag_fell_off(
          sub,
          method             = "tinyfox",
          tag_col            = track_col,
          vedba_col          = "vedba_sum",
          activity_col       = "tinyfox_activity_percent_last_24h",
          vedba_threshold    = tinyfox_vedba_threshold,
          activity_threshold = tinyfox_activity_threshold,
          min_inactive_run   = min_inactive_run
        )

      } else if (tt == "uwasp") {
        if (!"distance" %in% names(sub)) next
        sub <- detect_tag_fell_off(
          sub,
          method           = "uwasp",
          tag_col          = track_col,
          dist_col         = "distance",
          dist_threshold   = uwasp_dist_threshold,
          min_inactive_run = min_inactive_run
        )

      } else {
        next
      }

      b_loc$tag_fell_off[idx] <- sub$tag_fell_off
    }

    n_fell <- sum(b_loc$tag_fell_off, na.rm = TRUE)
    if (n_fell > 0) {
      .msg("  tag_fell_off: ", n_fell, " / ", nrow(b_loc), " location rows flagged.")
    }
    b_loc
  }

  # ---------------------------------------------------------------------------
  # .propagate_fell_off()
  # Propagate tag_fell_off flags from b_loc onto b (full) and b_daily2.
  #
  # b_loc drives the detection because it has vedba_sum from sensor rows joined
  # onto location rows.  Once we know the per-deployment cutoff timestamp
  # (earliest row where tag_fell_off == TRUE in b_loc), we stamp all rows in
  # the target object that occur at or after that cutoff as tag_fell_off = TRUE.
  #
  # This timestamp-cutoff approach works correctly for all row types in b:
  #   - NanoFox sensor rows share the location row timestamp → always matched
  #   - TinyFox hourly VeDBA bins have offset timestamps (T - N h) and are
  #     flagged when their timestamp >= cutoff, i.e. they are from a post-
  #     detachment transmission window
  # ---------------------------------------------------------------------------
  .propagate_fell_off <- function(b_loc, target) {
    if (is.null(target) || nrow(target) == 0L) return(target)
    target$tag_fell_off <- FALSE
    if (!"tag_fell_off" %in% names(b_loc)) return(target)
    if (!any(b_loc$tag_fell_off, na.rm = TRUE)) return(target)

    loc_tc <- move2::mt_track_id_column(b_loc)
    tgt_tc <- move2::mt_track_id_column(target)

    # Pick the best common join column: prefer deployment_id, else track id
    join_col <- if (
      "deployment_id" %in% names(b_loc) && "deployment_id" %in% names(target)
    ) {
      "deployment_id"
    } else if (loc_tc %in% names(target)) {
      loc_tc
    } else if (tgt_tc %in% names(b_loc)) {
      tgt_tc
    } else {
      return(target)
    }

    # Earliest fell-off timestamp per deployment
    cutoffs <- sf::st_drop_geometry(b_loc) %>%
      tibble::as_tibble() %>%
      dplyr::filter(isTRUE(.data$tag_fell_off) | .data$tag_fell_off == TRUE) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(join_col))) %>%
      dplyr::summarise(cutoff_ts = min(.data$timestamp, na.rm = TRUE), .groups = "drop")

    if (nrow(cutoffs) == 0L) return(target)

    df_t <- sf::st_drop_geometry(target) %>%
      tibble::as_tibble() %>%
      dplyr::select(dplyr::all_of(c(join_col, "timestamp"))) %>%
      dplyr::mutate(.row_id = dplyr::row_number()) %>%
      dplyr::left_join(cutoffs, by = join_col)

    flag_rows <- which(!is.na(df_t$cutoff_ts) & df_t$timestamp >= df_t$cutoff_ts)
    if (length(flag_rows) > 0L) target$tag_fell_off[flag_rows] <- TRUE

    n_fell  <- sum(target$tag_fell_off, na.rm = TRUE)
    n_total <- nrow(target)
    if (n_fell > 0L) {
      .msg("  tag_fell_off propagated: ", n_fell, " / ", n_total, " rows flagged.")
    }
    target
  }

  # Produce a daily (solar-noon) location dataset + bat-night sensor summaries.
  .source_local(script_daily)
  .source_local(script_pressure_to_altitude)
  .source_local(script_daily_sensor)

  # ---------------------------------------------------------------------------
  # .add_night_day_id()
  # ---------------------------------------------------------------------------
  .add_night_day_id <- function(x) {
    if (!require("suncalc", quietly = TRUE)) {
      warning(".add_night_day_id: suncalc not available; skipping.")
      return(x)
    }
    if (!all(c("lon", "lat") %in% names(x))) {
      coords <- sf::st_coordinates(x)
      x$lon <- coords[, 1]
      x$lat <- coords[, 2]
    }
    df <- tibble::tibble(
      .row_idx = seq_len(nrow(x)),
      individual = x$individual_local_identifier,
      timestamp = x$timestamp,
      lon = x$lon,
      lat = x$lat
    ) %>%
      dplyr::filter(
        !is.na(.data$lon),
        !is.na(.data$lat),
        !is.na(.data$timestamp)
      )

    if (nrow(df) == 0) {
      x$night_day_id <- NA_character_
      return(x)
    }

    sun_pos <- suncalc::getSunlightPosition(
      data = data.frame(date = df$timestamp, lat = df$lat, lon = df$lon),
      keep = "altitude"
    )
    df$night_day <- ifelse(sun_pos$altitude < 0, "night", "day")
    df <- df %>%
      dplyr::group_by(.data$individual) %>%
      dplyr::mutate(
        first_date = as.Date(min(.data$timestamp, na.rm = TRUE)),
        period = as.integer(as.Date(.data$timestamp) - .data$first_date)
      ) %>%
      dplyr::ungroup()
    df$night_day_id <- paste(df$night_day, df$period, sep = "_")

    out_vec <- rep(NA_character_, nrow(x))
    out_vec[df$.row_idx] <- df$night_day_id
    x$night_day_id <- out_vec

    .msg(
      "  night_day_id assigned. Unique: ",
      dplyr::n_distinct(df$night_day_id),
      " | night: ",
      sum(df$night_day == "night"),
      " | day: ",
      sum(df$night_day == "day")
    )
    x
  }

  # ---------------------------------------------------------------------------
  # .select_daily_daytime_only()
  # ---------------------------------------------------------------------------
  .select_daily_daytime_only <- function(x) {
    if (!inherits(x, c("move2", "sf"))) {
      stop("x must be a move2/sf object")
    }
    hr <- lubridate::hour(x$timestamp)
    flying_window <- hr >= 21 | hr < 5
    .msg(
      "  [daytime_only] Removing ",
      sum(flying_window, na.rm = TRUE),
      " fixes in 21:00-05:00 UTC flight window."
    )
    x_day <- x[!flying_window, ]
    if (nrow(x_day) == 0) {
      warning("[daytime_only] No fixes remain.")
      return(NULL)
    }
    if (!exists("mt_thin_daily_solar_noon", mode = "function")) {
      stop("mt_thin_daily_solar_noon() not found.")
    }
    mt_thin_daily_solar_noon(x_day, tz = tz)
  }

  # ---------------------------------------------------------------------------
  # .select_daily_noon_roost()
  # ---------------------------------------------------------------------------
  .select_daily_noon_roost <- function(x) {
    if (!"night_day_id" %in% names(x)) {
      stop("[noon_roost] 'night_day_id' not found.")
    }
    rep_daily <- x %>%
      dplyr::filter(!stringr::str_detect(.data$night_day_id, "_0$")) %>%
      dplyr::mutate(
        night_day_split = stringr::str_split_fixed(.data$night_day_id, "_", 2),
        night_day = night_day_split[, 1],
        period_id = night_day_split[, 2]
      ) %>%
      dplyr::select(-night_day_split)
    .msg(
      "  [noon_roost] Period-0 removed. Remaining: ",
      nrow(rep_daily),
      " of ",
      nrow(x)
    )

    grp_col <- .metric_group_col(rep_daily)
    if (is.null(grp_col)) {
      rep_daily$`..metric_group_id` <- as.character(move2::mt_track_id(
        rep_daily
      ))
      grp_col <- "..metric_group_id"
    }

    rep_daily <- rep_daily %>%
      dplyr::mutate(
        noon_date = dplyr::case_when(
          .data$night_day == "night" & lubridate::hour(.data$timestamp) >= 12 ~
            as.Date(.data$timestamp) + 1,
          TRUE ~ as.Date(.data$timestamp)
        ),
        noon_time = as.POSIXct(
          paste0(.data$noon_date, " 12:00:00"),
          tz = "UTC"
        ),
        dist_to_noon = abs(as.numeric(difftime(
          .data$timestamp,
          .data$noon_time,
          units = "secs"
        )))
      ) %>%
      dplyr::group_by(.data[[grp_col]], .data$period_id) %>%
      dplyr::arrange(
        .data$night_day == "night",
        .data$dist_to_noon,
        .by_group = TRUE
      ) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    .msg(
      "  [noon_roost] Selected ",
      nrow(rep_daily),
      " daily points across ",
      dplyr::n_distinct(rep_daily[[grp_col]]),
      " groups (",
      grp_col,
      ")."
    )
    rep_daily %>%
      dplyr::select(
        -dplyr::any_of(c(
          "night_day",
          "period_id",
          "noon_date",
          "noon_time",
          "dist_to_noon"
        ))
      )
  }

  # ---------------------------------------------------------------------------
  # .make_daily()
  # ---------------------------------------------------------------------------
  .make_daily <- function(x) {
    method <- match.arg(
      daily_method,
      c("solar_noon", "daytime_only", "noon_roost")
    )
    b_daily <- switch(
      method,
      "solar_noon" = {
        if (!exists("mt_thin_daily_solar_noon", mode = "function")) {
          stop("mt_thin_daily_solar_noon() not found.")
        }
        .msg("Daily method: solar_noon")
        .safe_try(
          mt_thin_daily_solar_noon(x, tz = tz),
          "mt_thin_daily_solar_noon"
        )
      },
      "daytime_only" = {
        .msg("Daily method: daytime_only (excludes 21:00-05:00 UTC)")
        .safe_try(.select_daily_daytime_only(x), "select_daily_daytime_only")
      },
      "noon_roost" = {
        .msg("Daily method: noon_roost")
        .safe_try(.select_daily_noon_roost(x), "select_daily_noon_roost")
      }
    )
    if (is.null(b_daily) || nrow(b_daily) == 0) {
      warning(".make_daily(): no rows returned for method '", method, "'.")
      return(b_daily)
    }
    if (exists("mt_add_daily_sensor_metrics", mode = "function")) {
      b_daily <- .safe_try(
        mt_add_daily_sensor_metrics(
          b_all = x,
          b_daily = b_daily,
          tz = tz,
          day_anchor_hour = 12
        ),
        "mt_add_daily_sensor_metrics"
      ) %||%
        b_daily
    }

    # ---- diff_date: days elapsed between consecutive daily fixes per deployment ----
    # For a perfect daily dataset every value is 1.
    # Values > 1 indicate gaps (missed days); NA at the first fix of each deployment.
    # Grouped by deployment_id when available (finest grouping), so gaps don't
    # bleed across deployments for multi-deployment individuals.
    b_daily$date <- as.Date(b_daily$timestamp)
    grp_vec <- as.character(b_daily[[move2::mt_track_id_column(b_daily)]])
    b_daily$diff_date <- ave(
      as.numeric(b_daily$date),
      grp_vec,
      FUN = function(d) c(NA_real_, diff(d))
    )

    .msg(
      "  diff_date: ",
      sum(b_daily$diff_date == 1, na.rm = TRUE),
      " consecutive day pairs, ",
      sum(b_daily$diff_date > 1, na.rm = TRUE),
      " gaps (>1 day)."
    )

    b_daily
  }

  # ---------------------------------------------------------------------------
  # Input checks
  # ---------------------------------------------------------------------------
  if (length(study_id) < 1) {
    stop("Provide at least one study_id.")
  }
  if (length(sensor_external_ids) != length(sensor_labels)) {
    stop("sensor_external_ids and sensor_labels must have the same length.")
  }

  # ---------------------------------------------------------------------------
  # Sensor lookup
  # ---------------------------------------------------------------------------
  sensors_tbl <- move2::movebank_retrieve(entity_type = "tag_type") %>%
    as_tibble()
  sensor_selected <- sensors_tbl %>%
    dplyr::filter(.data$external_id %in% sensor_external_ids) %>%
    mutate(
      sensor_type = sensor_labels[match(.data$external_id, sensor_external_ids)]
    )
  if (nrow(sensor_selected) == 0) {
    stop(
      "None of the requested sensor_external_ids found in movebank_retrieve(tag_type)."
    )
  }
  if (verbose) {
    .msg("Selected sensors:")
    print(
      sensor_selected %>%
        dplyr::select(id, name, external_id, sensor_type, is_location_sensor),
      n = 50
    )
  }

  # ---------------------------------------------------------------------------
  # Per-study download + processing
  # ---------------------------------------------------------------------------
  # ---------------------------------------------------------------------------
  # Column utilities (used by download_one_study, merge_stack, and output loop)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Column utilities — defined before download_one_study so all downstream
  # code (expand_tinyfox, output loop, merge_stack) can call them freely.
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Column utilities — defined before download_one_study so all downstream
  # code (expand_tinyfox, output loop, merge_stack) can call them freely.
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Merge studies
  # ---------------------------------------------------------------------------
  # Strip units from a move2/sf object before stacking so that columns that
  # carry units in one study but not another (or carry different units) don't
  # cause bind_rows to error.  Units metadata is recorded as an attribute on
  # each column so it can be reattached if needed; stripping to double is safe
  # because the physical meaning is preserved by the column name.
  .strip_units_cols <- function(obj) {
    if (is.null(obj) || !is.data.frame(obj)) {
      return(obj)
    }
    for (col in names(obj)) {
      if (inherits(obj[[col]], "units")) {
        obj[[col]] <- as.numeric(obj[[col]])
      }
    }
    obj
  }
  .na_like <- function(src, n) {
    if (inherits(src, "POSIXct")) {
      return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
    }
    if (is.double(src)) {
      return(rep(NA_real_, n))
    }
    if (is.integer(src)) {
      return(rep(NA_integer_, n))
    }
    if (is.character(src)) {
      return(rep(NA_character_, n))
    }
    if (is.logical(src)) {
      return(rep(NA, n))
    }
    rep(NA_character_, n) # safe fallback
  }

  # ---------------------------------------------------------------------------
  # Column type normalisation for cross-study binding
  # ---------------------------------------------------------------------------
  # Converts an sf/tibble to a maximally bind-safe form:
  #   - units columns → plain numeric
  #   - ordered/factor columns → character (avoids level-set conflicts)
  #   - int64 (bit64) columns → character (avoids integer overflow in bind)
  # Geometry column is left untouched.
  .normalise_cols <- function(x) {
    geom_col <- if (inherits(x, "sf")) attr(x, "sf_column") else NULL
    for (nm in names(x)) {
      if (!is.null(geom_col) && nm == geom_col) {
        next
      }
      v <- x[[nm]]
      if (inherits(v, "units")) {
        x[[nm]] <- as.numeric(v)
        next
      }
      if (is.ordered(v) || is.factor(v)) {
        x[[nm]] <- as.character(v)
        next
      }
      if (inherits(v, "integer64")) {
        x[[nm]] <- as.character(v)
        next
      }
    }
    x
  }

  # ---------------------------------------------------------------------------
  # .drop_all_na_cols() — remove columns where every value (non-geometry) is NA
  #
  # Keeps geometry and move2 metadata intact. Applied to all three output objects
  # (full, location, daily) per study and after merging so analysts never see
  # columns that are entirely empty.
  # ---------------------------------------------------------------------------
  .drop_all_na_cols <- function(x) {
    if (is.null(x) || nrow(x) == 0) {
      return(x)
    }
    geom_col <- if (inherits(x, "sf")) attr(x, "sf_column") else character(0)
    keep <- vapply(
      names(x),
      function(nm) {
        if (nm %in% geom_col) {
          return(TRUE)
        } # always keep geometry
        v <- x[[nm]]
        if (inherits(v, "sfc")) {
          return(TRUE)
        } # always keep sfc columns
        !all(is.na(v))
      },
      logical(1)
    )
    dropped <- names(x)[!keep]
    if (length(dropped) > 0) {
      .msg(
        "  Dropping ",
        length(dropped),
        " all-NA column(s): ",
        paste(dropped, collapse = ", ")
      )
    }
    x[, keep, drop = FALSE]
  }

  # ---------------------------------------------------------------------------
  # .normalise_one() — normalise a single move2 object before stacking
  #
  # Prepares a move2 object for mt_stack() by:
  #   1. Ensuring it is keyed to deployment_id
  #   2. Normalising event column types (units→numeric, ordered/factor→character,
  #      integer64→character) so bind_rows across studies doesn't see type clashes
  #   3. Normalising track data column types the same way so mt_stack track-data
  #      merge doesn't see clashes (e.g. capture_location <dbl> vs <chr>)
  # ---------------------------------------------------------------------------
  .normalise_one <- function(x) {
    # Ensure keyed to deployment_id
    if (
      move2::mt_track_id_column(x) != "deployment_id" &&
        "deployment_id" %in% names(x)
    ) {
      x$deployment_id <- as.character(x$deployment_id)
      attr(x, "track_id_column") <- "deployment_id"
    }

    # Normalise event columns
    x <- .normalise_cols(x)

    # Normalise track data columns
    td <- tryCatch(move2::mt_track_data(x), error = function(e) NULL)
    if (!is.null(td)) {
      changed <- FALSE
      for (nm in names(td)) {
        v <- td[[nm]]
        if (inherits(v, "units")) {
          td[[nm]] <- as.numeric(v)
          changed <- TRUE
        } else if (is.ordered(v)) {
          td[[nm]] <- as.character(v)
          changed <- TRUE
        } else if (is.factor(v)) {
          td[[nm]] <- as.character(v)
          changed <- TRUE
        } else if (inherits(v, "integer64")) {
          td[[nm]] <- as.character(v)
          changed <- TRUE
        }
        # Leave sfc geometry columns intact — that is the whole point
      }
      if (changed) {
        x <- tryCatch(move2::mt_set_track_data(x, td), error = function(e) x)
      }
    }
    x
  }
  download_one_study <- function(id) {
    .msg("Downloading study: ", id)
    si <- move2::movebank_download_study_info(study_id = id)
    w <- .wanted_sensor_ids(si, sensor_selected)
    wanted_ids <- w$wanted_ids
    if (length(wanted_ids) == 0) {
      stop(
        "Study ",
        id,
        " has no matching sensors.\n",
        "Study: ",
        paste(w$study_sensor_names, collapse = ", "),
        "\n",
        "Requested: ",
        paste(sensor_selected$name, collapse = ", ")
      )
    }

    b <- tryCatch(
      move2::movebank_download_study(
        study_id = id,
        sensor_type_id = wanted_ids
      ),
      error = function(e) {
        msg <- conditionMessage(e)
        if (grepl("none seem to be deployed|deployment_id.*NA", msg, ignore.case = TRUE)) {
          .msg(
            "  Skipping study ", id,
            ": no deployments defined in Movebank (deployment_id is NA for all records)."
          )
          return(NULL)
        }
        stop(e)
      }
    )
    if (is.null(b)) return(NULL)

    # ---------------------------------------------------------------------------
    # Re-key to deployment_id immediately after download.
    #
    # Each deployment is its own move2 track (one track data row per deployment).
    # This means:
    #   - No list-columns ever — no stringification of geometry
    #   - capture_location / deploy_on_location stay as proper sfc POINT columns
    #   - mt_stack() across studies works without type clashes
    #
    # individual_local_identifier and deployment_id are promoted to event columns
    # so individual- and deployment-level grouping both work without track data.
    #
    # mt_as_move2() rebuilds track data from event rows and loses track-data-only
    # columns (capture_location, deploy_on_location, etc.).  We therefore:
    #   1. Save the original track data (with geometry intact).
    #   2. Add deployment_id as an event column by joining from track data.
    #   3. Re-key via attr swap (not mt_as_move2) — preserves track data exactly.
    #   4. Re-attach the saved track data, re-indexed by deployment_id.
    # ---------------------------------------------------------------------------
    b <- tryCatch(
      {
        orig_td <- move2::mt_track_data(b)
        orig_track_col <- move2::mt_track_id_column(b)

        # ---- Promote individual_local_identifier to event column ----
        if (
          !"individual_local_identifier" %in% names(b) &&
            "individual_local_identifier" %in% names(orig_td)
        ) {
          lkp <- stats::setNames(
            as.character(orig_td$individual_local_identifier),
            as.character(orig_td[[orig_track_col]])
          )
          b$individual_local_identifier <- unname(lkp[as.character(move2::mt_track_id(
            b
          ))])
        }
        if (
          !"individual_local_identifier" %in% names(b) ||
            all(is.na(b$individual_local_identifier))
        ) {
          b$individual_local_identifier <- as.character(move2::mt_track_id(b))
        }

        # ---- If already keyed to deployment_id, coerce to character for join safety ----
        if (orig_track_col == "deployment_id") {
          if ("deployment_id" %in% names(b)) {
            b$deployment_id <- as.character(b$deployment_id)
          }
          if (
            "deployment_id" %in%
              names(orig_td) &&
              !is.character(orig_td$deployment_id)
          ) {
            orig_td$deployment_id <- as.character(orig_td$deployment_id)
            b <- tryCatch(
              move2::mt_set_track_data(b, orig_td),
              error = function(e) b
            )
          }
          b
        } else if ("deployment_id" %in% names(orig_td)) {
          # ---- Coerce track data deployment_id to character immediately ----
          # This prevents mt_as_event_attribute() join failures caused by
          # int64 (track data) vs character (event col) type mismatch.
          orig_td$deployment_id <- as.character(orig_td$deployment_id)

          # ---- Build deployment_id event column from mt_track_id ----
          # Use mt_track_id(b) rather than b[[orig_track_col]] because the
          # original track id column may not exist as an event column.
          track_id_vec <- as.character(move2::mt_track_id(b))
          dep_lkp <- stats::setNames(
            as.character(orig_td$deployment_id),
            as.character(orig_td[[orig_track_col]])
          )
          b$deployment_id <- as.character(dep_lkp[track_id_vec])

          # ---- Swap track id attribute (preserves track data intact) ----
          attr(b, "track_id_column") <- "deployment_id"

          # ---- Re-index track data so deployment_id is the key column ----
          # orig_td already has deployment_id (it came from Movebank track data).
          # We just need to: coerce it to character, move it to position 1,
          # and drop the old track id column (e.g. individual_local_identifier).
          # No join required — orig_td is already one row per deployment.
          new_td <- orig_td %>%
            dplyr::mutate(deployment_id = as.character(deployment_id)) %>%
            dplyr::select(
              deployment_id,
              dplyr::everything(),
              -dplyr::any_of(setdiff(orig_track_col, "deployment_id"))
            )

          b <- move2::mt_set_track_data(b, new_td)

          .msg(
            "  Re-keyed to deployment_id (",
            dplyr::n_distinct(b$deployment_id, na.rm = TRUE),
            " deployments)."
          )
          b
        } else {
          .msg(
            "  deployment_id not in track data; keeping original track id: ",
            orig_track_col
          )
          b
        }
      },
      error = function(e) {
        .msg(
          "  Re-key to deployment_id failed (",
          conditionMessage(e),
          "); using original track id."
        )
        b
      }
    )

    # .fix_track_data_lists only needed if re-keying failed (multi-deployment individual track)
    b <- .fix_track_data_lists(b)
    b <- .add_event_attrs(b)
    b <- .set_tag_type(b, study_id_current = id)
    b <- .add_lonlat(b)
    b <- .label_sensor_type(b, sensor_selected)

    # Diagnostic: report a safe summary of what came back from the download.
    # Never paste raw column values — only names and table summaries.
    if (isTRUE(verbose)) {
      col_names <- names(b)
      .msg("  Downloaded: ", nrow(b), " rows x ", ncol(b), " columns.")
      .msg("  Columns: ", paste(col_names, collapse = ", "))
      if ("sensor_type" %in% col_names) {
        .msg("  sensor_type distribution:")
        print(table(b$sensor_type, useNA = "ifany"))
      }
      if ("tag_type" %in% col_names) {
        .msg(
          "  tag_type: ",
          paste(
            names(table(b$tag_type)),
            table(b$tag_type),
            sep = "=",
            collapse = ", "
          )
        )
      }
      pcols <- grep(
        "pressure|baro|altitude|tinyfox",
        col_names,
        value = TRUE,
        ignore.case = TRUE
      )
      if (length(pcols)) {
        .msg("  TinyFox/pressure columns: ", paste(pcols, collapse = ", "))
      }
    }
    b <- tryCatch(.add_start(b), error = function(e) {
      .msg("  .add_start failed: ", conditionMessage(e))
      b
    })

    .source_local(script_add_min_pressure)
    b <- .safe_try(
      add_min_pressure_to_locations(df = b),
      "add_min_pressure_to_locations"
    ) %||%
      b

    # Populate temperature_min, temperature_max, avg_temp on location rows for
    # all tag types (TinyFox flat columns + NanoFox sensor-row join fallback).

    # Convert pressure to altitude for all tag types.
    # NanoFox: min_3h_pressure is joined by add_min_pressure_to_locations() above.
    # TinyFox: tinyfox_pressure_min_last_24h is a flat column on location rows.
    # Both are handled by a single direct hypsometric conversion — no sourced
    # script dependency, no window grouping, no row-ordering issues.
    b <- .add_altitude_from_pressure_col(b)

    # Expand TinyFox flat-schema columns into separate sensor rows (one per
    # measurement type per transmission), adding time_start and time_end.
    # Must run AFTER .add_altitude_from_pressure_col() so altitude_m is already
    # on rows, and BEFORE .add_night_day_id() so new rows get night/day labels too.
    b <- .safe_try(.expand_tinyfox_rows(b), "expand_tinyfox_rows") %||% b

    b <- .safe_try(.add_night_day_id(b), "add_night_day_id") %||% b

    # Location subset: speed (km/h), distance (km), delta_altitude_m (m)
    b_loc <- .make_location_metrics(b)
    b_loc <- add_prev_latlon(b_loc)

    # Join per-transmission sensor aggregates onto location rows
    b_loc <- .safe_try(
      .add_location_sensor_metrics(b, b_loc),
      "add_location_sensor_metrics"
    ) %||% b_loc

    # Flag likely tag-fell-off rows per deployment and tag type.
    # tag_type is still present here (dropped later in the cleanup loop).
    b_loc <- .safe_try(
      .detect_tag_fell_off_loc(b_loc),
      "detect_tag_fell_off_loc"
    ) %||% b_loc

    # Propagate tag_fell_off from b_loc to the full multi-sensor object.
    # All sensor rows (VeDBA, temp, pressure) at or after the cutoff timestamp
    # per deployment are also flagged.
    b <- .safe_try(
      .propagate_fell_off(b_loc, b),
      "propagate_fell_off_full"
    ) %||% b

    # Daily subset: same metrics pipeline (speed km/h, delta_altitude_m)
    .source_local(script_daily)
    b_daily <- .make_daily(b)
    if (is.null(b_daily) || nrow(b_daily) == 0) {
      .msg(
        "Warning: daily dataset empty for study ",
        id,
        " \u2014 skipping metrics."
      )
      b_daily2 <- b_daily
    } else {
      b_daily2 <- .make_location_metrics(b_daily)
      b_daily2 <- add_prev_latlon(b_daily2)
      b_daily2 <- .safe_try(
        .propagate_fell_off(b_loc, b_daily2),
        "propagate_fell_off_daily"
      ) %||% b_daily2
    }

    # Temporal covariates + final cleanup on all three objects
    for (obj_name in c("b", "b_loc", "b_daily2")) {
      obj <- get(obj_name)
      if (!is.null(obj) && nrow(obj) > 0) {
        obj$year <- as.character(lubridate::year(obj$timestamp))
        obj$yday <- as.character(lubridate::yday(obj$timestamp))
        obj$season <- ifelse(
          lubridate::month(obj$timestamp) > 7,
          "Fall",
          "Spring"
        )
        # Guarantee species on every output object (re-apply after row filtering).
        obj <- .fill_from_td(obj, "taxon_canonical_name", "species")
        obj$species <- as.character(obj$species)
        # Remove internal columns not needed in outputs:
        #   tag_type             — internal inference label
        #   taxon_canonical_name — redundant with species
        drop_cols <- intersect(
          names(obj),
          c("tag_type", "taxon_canonical_name")
        )
        if (length(drop_cols) > 0) {
          obj <- obj[, !names(obj) %in% drop_cols, drop = FALSE]
        }

        # ---- tag_tech_spec → min_temp_c + min_temp_group ----
        # tag_tech_spec carries ordinal minimum-temperature bins from the
        # "derived" / "min.temp" sensor: "<=0", ">0 / <=5", ">5 / <=10", ">10".
        # Rename to min_temp_c (the raw text label) and add min_temp_group,
        # an ordered factor with levels in ascending temperature order so that
        # model.matrix(), lm(), and ggplot2 all treat it as ordinal automatically.
        if ("tag_tech_spec" %in% names(obj)) {
          obj$min_temp_c <- as.character(obj$tag_tech_spec)
          obj$min_temp_group <- factor(
            obj$min_temp_c,
            levels = c("<=0", ">0 / <=5", ">5 / <=10", ">10"),
            ordered = TRUE
          )
          obj <- obj[, !names(obj) %in% "tag_tech_spec", drop = FALSE]
        }
        # Coerce factors/ordered → character for merge safety
        obj <- .normalise_cols(obj)
        # Drop all-NA columns
        obj <- .drop_all_na_cols(obj)
        assign(obj_name, obj)
      }
    }

    list(study_id = id, full = b, location = b_loc, daily = b_daily2)
  }

  res_list <- lapply(study_id, download_one_study)
  names(res_list) <- as.character(study_id)

  # Create a typed NA vector that matches `src` in class so bind_rows
  # doesn't see a logical vs POSIXct / numeric / character clash.

  merge_stack <- function(objs) {
    objs <- Filter(Negate(is.null), objs)
    if (length(objs) == 0) {
      return(NULL)
    }
    if (length(objs) == 1) {
      return(objs[[1]])
    }

    # Step 1: normalise all objects (key to deployment_id, fix column types)
    objs <- lapply(objs, .normalise_one)

    # Step 2: try mt_stack directly — it handles track data merging correctly
    # including sfc geometry columns.  Uses .track_combine = track_combine which
    # defaults to "merge" (keeps all track data columns from all studies).
    out <- tryCatch(
      Reduce(
        function(a, b) move2::mt_stack(a, b, .track_combine = track_combine),
        objs
      ),
      error = function(e) {
        .msg(
          "  merge_stack: mt_stack failed (",
          conditionMessage(e),
          "); falling back to manual bind_rows merge."
        )
        NULL
      }
    )

    if (!is.null(out)) {
      return(out)
    }

    # Fallback: strip units from event columns, drop move2 class, bind_rows, re-cast
    .to_plain_sf <- function(x) {
      x <- .strip_units_cols(sf::st_as_sf(x))
      class(x) <- class(x)[!class(x) %in% "move2"]
      x
    }
    sf_objs <- lapply(objs, .to_plain_sf)

    # Build union column set and typed-NA fill map
    geom_cols <- vapply(sf_objs, function(x) attr(x, "sf_column"), character(1))
    all_cols <- unique(unlist(mapply(
      function(x, gc) setdiff(names(x), gc),
      sf_objs,
      geom_cols,
      SIMPLIFY = FALSE
    )))

    type_map <- list()
    for (x in sf_objs) {
      for (nm in names(x)) {
        if (!nm %in% names(type_map) && !inherits(x[[nm]], "sfc")) {
          type_map[[nm]] <- x[[nm]]
        }
      }
    }

    align_one <- function(x, gc) {
      n <- nrow(x)
      for (col in setdiff(all_cols, names(x))) {
        src <- type_map[[col]]
        x[[col]] <- if (!is.null(src)) {
          .na_like(src, n)
        } else {
          rep(NA_character_, n)
        }
      }
      x[, c(all_cols, gc), drop = FALSE]
    }
    sf_objs <- mapply(align_one, sf_objs, geom_cols, SIMPLIFY = FALSE)

    combined <- tryCatch(
      do.call(dplyr::bind_rows, sf_objs),
      error = function(e) {
        .msg(
          "  merge_stack: bind_rows failed (",
          conditionMessage(e),
          "); coercing all non-geometry columns to character."
        )
        sf_objs2 <- lapply(sf_objs, function(x) {
          gc <- attr(x, "sf_column")
          for (nm in setdiff(names(x), gc)) {
            x[[nm]] <- if (!inherits(x[[nm]], "sfc")) {
              as.character(x[[nm]])
            } else {
              x[[nm]]
            }
          }
          x
        })
        do.call(dplyr::bind_rows, sf_objs2)
      }
    )

    track_col <- if (
      "deployment_id" %in%
        names(combined) &&
        !all(is.na(combined$deployment_id))
    ) {
      "deployment_id"
    } else if (
      "individual_local_identifier" %in%
        names(combined) &&
        !all(is.na(combined$individual_local_identifier))
    ) {
      "individual_local_identifier"
    } else {
      combined$..row_id <- as.character(seq_len(nrow(combined)))
      "..row_id"
    }

    combined[[track_col]] <- as.character(combined[[track_col]])
    combined <- combined[order(combined[[track_col]], combined$timestamp), ]
    move2::mt_as_move2(
      combined,
      track_id_column = track_col,
      time_column = "timestamp",
      crs = sf::st_crs(sf_objs[[1]])
    )
  }
  full_merged <- if (isTRUE(merge_studies)) {
    merge_stack(lapply(res_list, `[[`, "full"))
  } else {
    lapply(res_list, `[[`, "full")
  }
  loc_merged <- if (isTRUE(merge_studies)) {
    merge_stack(lapply(res_list, `[[`, "location"))
  } else {
    lapply(res_list, `[[`, "location")
  }
  daily_merged <- if (isTRUE(merge_studies)) {
    merge_stack(lapply(res_list, `[[`, "daily"))
  } else {
    lapply(res_list, `[[`, "daily")
  }

  # ---------------------------------------------------------------------------
  # Post-merge: drop all-NA columns from merged outputs
  # ---------------------------------------------------------------------------
  if (isTRUE(merge_studies)) {
    if (!is.null(full_merged) && nrow(full_merged) > 0) {
      full_merged <- .drop_all_na_cols(full_merged)
    }
    if (!is.null(loc_merged) && nrow(loc_merged) > 0) {
      loc_merged <- .drop_all_na_cols(loc_merged)
    }
    if (!is.null(daily_merged) && nrow(daily_merged) > 0) {
      daily_merged <- .drop_all_na_cols(daily_merged)
    }
  }

  # ---------------------------------------------------------------------------
  # Summaries
  # ---------------------------------------------------------------------------
  if (verbose && isTRUE(merge_studies)) {
    .msg("Merged studies: ", paste(study_id, collapse = ", "))
    .msg("Sensor types (merged full):")
    print(table(full_merged$sensor_type, useNA = "ifany"))
    .msg("Timestamp range (merged full):")
    print(summary(full_merged$timestamp))
    # Note: units are stripped from merged objects by .strip_units_cols() before
    # mt_stack(), so deparse_unit() would error here — report class instead.
    if (!is.null(loc_merged)) {
      if ("speed" %in% names(loc_merged)) {
        .msg("Speed column class (location): ", class(loc_merged$speed)[1])
      }
      if ("delta_altitude_m" %in% names(loc_merged)) {
        .msg(
          "Delta altitude column class (location): ",
          class(loc_merged$delta_altitude_m)[1]
        )
      }
      if ("cum_dist_km" %in% names(loc_merged)) {
        .msg("Cumulative distance column present (location): yes")
      }
    }
  } else if (verbose) {
    .msg("Downloaded ", length(study_id), " studies (not merged).")
  }

  list(
    sensors_table = sensors_tbl,
    sensors_selected = sensor_selected,
    studies = res_list,
    full = full_merged,
    location = loc_merged,
    daily = daily_merged
  )
}
