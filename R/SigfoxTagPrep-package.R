#' @keywords internal
"_PACKAGE"

# Package-wide imports. Heavily used packages are imported whole; the rest
# selectively. Optional (Suggests) packages are called as pkg:: or attached
# inside the functions that need them.
#' @import dplyr
#' @import ggplot2
#' @import sf
#' @import move2
#' @importFrom rlang .data sym enquo quo_name
#' @importFrom stringr regex str_detect str_extract str_match str_pad
#'   str_replace_all str_sub str_to_lower str_to_title str_trim str_split_fixed
#' @importFrom tidyr pivot_longer pivot_wider complete
#' @importFrom tibble tibble as_tibble
#' @importFrom lubridate as_datetime days with_tz ymd_hms hour second
#' @importFrom data.table := as.data.table fread melt setnames setorder setorderv
#' @importFrom terra crs
#' @importFrom assertthat assert_that
#' @importFrom stats approx approxfun as.formula ave median na.omit predict
#'   quantile sd setNames
#' @importFrom utils head install.packages installed.packages packageVersion
#'   read.csv write.table
#' @importFrom graphics arrows legend mtext par points
#' @importFrom grDevices adjustcolor colorRampPalette
NULL
