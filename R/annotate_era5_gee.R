# ─────────────────────────────────────────────────────────────────────────────
# annotate_era5_gee.R
# ─────────────────────────────────────────────────────────────────────────────
# GEE-based ERA5 annotation via python/annotate_era5_gee.py.
#
# Calls the Python script using system2() — no rgee/reticulate required.
# The Python script uses earthengine-api directly, which is simpler to
# set up on Windows than rgee.
#
# Setup (Python side, once):
#   conda activate rgee311          # or any env with earthengine-api
#   pip install earthengine-api pandas
#   python -c "import ee; ee.Authenticate()"
#   python -c "import ee; ee.Initialize()"  # confirm it works
#
# Reuses:
#   wind_support(), cross_wind(), airspeed()  (calculate_wind_features.R)
#   altitude_to_pressure_hPa()               (annotate_era5.R)
#   .era5_detect_col(), .era5_wind_support(), .era5_flight_pressure()
#                                             (annotate_era5.R)
# ─────────────────────────────────────────────────────────────────────────────


#' Annotate a move2/sf object with ERA5 data via Google Earth Engine
#'
#' Calls \code{python/annotate_era5_gee.py} using \code{system2()} to extract
#' ERA5 variables from the GEE data catalog.  No local ERA5 files or rgee
#' required — only \pkg{earthengine-api} and \pkg{pandas} in a Python
#' environment.
#'
#' @section Extracted variables:
#' \describe{
#'   \item{era5_u10, era5_v10}{10 m zonal / meridional wind (m/s)}
#'   \item{era5_u100, era5_v100}{100 m zonal / meridional wind (m/s)}
#'   \item{era5_u500, era5_v500}{500 hPa zonal / meridional wind (m/s)}
#'   \item{era5_u850, era5_v850}{850 hPa zonal / meridional wind (m/s)}
#'   \item{era5_t2m}{2 m temperature (K)}
#'   \item{era5_msl}{Mean sea-level pressure (Pa)}
#'   \item{era5_sp}{Surface pressure (Pa)}
#'   \item{era5_tp}{Total precipitation (m)}
#'   \item{era5_i10fg}{Instantaneous 10 m wind gust (m/s)}
#'   \item{era5_tcc}{Total cloud cover (fraction 0–1)}
#'   \item{era5_cbh}{Cloud base height (m)}
#' }
#'
#' @section Python setup:
#' \preformatted{
#' conda activate your_env
#' pip install earthengine-api pandas
#' python -c "import ee; ee.Authenticate()"
#' }
#'
#' @param data             A \code{move2} or \code{sf} object with timestamps.
#' @param python           Path to the Python executable that has
#'   \pkg{earthengine-api} installed.  \code{NULL} = auto-detect from PATH.
#' @param script           Path to \code{annotate_era5_gee.py}.  Defaults to
#'   \code{python/annotate_era5_gee.py} relative to \code{getwd()}.
#' @param gee_project      GEE Cloud project ID (required by newer API
#'   versions).  \code{NULL} = let the script use the stored default.
#' @param altitude_col     Column name with altitude in metres (GPS data).
#' @param tag_pressure_col Column name with barometric pressure from tag (hPa).
#' @param compute_wind_support Logical; compute tailwind/crosswind/airspeed?
#' @param pressure_levels  Pressure levels (hPa) to include in wind support.
#'   Only 500 and 850 are available from GEE; others will be ignored unless
#'   their \code{era5_uXXX}/\code{era5_vXXX} columns are already present.
#' @param max_time_gap_hours Warn when nearest ERA5 hour exceeds this gap.
#' @param verbose          Logical; show Python script output?
#'
#' @return The input object with ERA5 columns appended.
#'
#' @seealso \code{\link{annotate_era5}} for the local GRIB-file version.
#'
#' @examples
#' \dontrun{
#' source("R/annotate_era5_gee.R")
#' source("R/annotate_era5.R")
#' source("R/calculate_wind_features.R")
#' source("R/pressure_to_altitude_m.R")
#'
#' dat <- annotate_era5_gee(
#'   data    = leisler,
#'   python  = "C:/Users/Edward/anaconda3/envs/rgee311/python.exe",
#'   verbose = TRUE
#' )
#' }
#'
#' @export
annotate_era5_gee <- function(
    data,
    python              = NULL,
    script              = file.path("python", "annotate_era5_gee.py"),
    gee_project         = NULL,
    altitude_col        = NULL,
    tag_pressure_col    = NULL,
    compute_wind_support = TRUE,
    pressure_levels     = c(500, 850),
    max_time_gap_hours  = 3,
    verbose             = TRUE
) {

  stopifnot(inherits(data, "sf"))
  n <- nrow(data)
  if (n == 0L) { warning("Input has 0 rows."); return(data) }

  # ── Find Python ───────────────────────────────────────────────────────────
  if (is.null(python)) {
    python <- Sys.which("python")
    if (python == "") python <- Sys.which("python3")
    if (python == "")
      stop(
        "Python not found on PATH.\n",
        "Set python = 'C:/Users/Edward/anaconda3/envs/rgee311/python.exe'"
      )
  }
  if (!file.exists(python))
    stop("Python executable not found: ", python)

  if (!file.exists(script))
    stop("Python script not found: ", script,
         "\n  (run from project root, or set script = full path)")

  # ── Column detection ──────────────────────────────────────────────────────
  altitude_col <- .era5_detect_col(
    data, altitude_col,
    c("height_raw", "altitude", "altitude_m", "altitude_sea",
      "altitude.sea", "height_above_msl"),
    "altitude", verbose
  )
  tag_pressure_col <- .era5_detect_col(
    data, tag_pressure_col,
    c("tinyfox_pressure_min_last_24h", "min_3h_pressure",
      "tag_pressure", "barometric_pressure", "pressure_hpa_used"),
    "tag pressure", verbose
  )

  # ── Build a flat CSV for Python ───────────────────────────────────────────
  coords  <- sf::st_coordinates(sf::st_transform(data, 4326))
  df_flat <- sf::st_drop_geometry(data)

  timestamps <- if (inherits(data, "move2")) move2::mt_time(data) else data$timestamp
  if (is.null(timestamps)) stop("Cannot find timestamps in data.")

  df_flat$.longitude <- coords[, 1]
  df_flat$.latitude  <- coords[, 2]
  df_flat$.timestamp <- format(as.POSIXct(timestamps, tz = "UTC"),
                                "%Y-%m-%dT%H:%M:%SZ")
  df_flat$.row_id    <- seq_len(n)

  # Mask empty geometries so Python doesn't try to extract from (NA, NA)
  empty <- sf::st_is_empty(data)
  if (any(empty)) {
    df_flat$.longitude[empty] <- NA_real_
    df_flat$.latitude[empty]  <- NA_real_
  }

  tmp_in  <- tempfile(fileext = ".csv")
  tmp_out <- tempfile(fileext = ".csv")
  on.exit({ unlink(tmp_in); unlink(tmp_out) }, add = TRUE)

  utils::write.csv(df_flat, tmp_in, row.names = FALSE, na = "")

  # ── Call Python script ────────────────────────────────────────────────────
  args <- c(
    normalizePath(script, mustWork = FALSE),
    normalizePath(tmp_in,  mustWork = TRUE),
    normalizePath(tmp_out, mustWork = FALSE),
    "--lon",  ".longitude",
    "--lat",  ".latitude",
    "--time", ".timestamp"
  )
  if (!is.null(gee_project)) args <- c(args, "--project", gee_project)

  if (verbose) message("Running Python ERA5 extraction ...")
  ret <- system2(python, args = args, wait = TRUE,
                 stdout = if (verbose) "" else FALSE,
                 stderr = if (verbose) "" else FALSE)

  if (ret != 0L)
    stop("Python script exited with code ", ret,
         ".\n  Check output above for error details.")

  if (!file.exists(tmp_out))
    stop("Python script did not produce output file.")

  # ── Read results and attach to original data ──────────────────────────────
  result <- utils::read.csv(tmp_out, stringsAsFactors = FALSE)

  era5_cols <- grep("^era5_", names(result), value = TRUE)
  if (length(era5_cols) == 0L)
    warning("No era5_* columns found in Python output — check script output above.")

  # Match by row order (.row_id is just seq_len so index directly)
  for (col in era5_cols) {
    data[[col]] <- result[[col]]
  }

  if (verbose)
    message("  ERA5 columns added: ",
            paste(era5_cols, collapse = ", "))

  # ── Wind support ──────────────────────────────────────────────────────────
  if (compute_wind_support) {
    if (verbose) message("Computing wind support ...")
    data <- .era5_wind_support(data, pressure_levels,
                                altitude_col, tag_pressure_col)
  }

  if (verbose) message("GEE annotation complete.")
  data
}
