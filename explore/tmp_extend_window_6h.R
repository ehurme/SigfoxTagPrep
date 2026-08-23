library(sf)
library(dplyr)
source("R/pressure_wind_profile.R")
source("explore/test_pressure_wind_profile_6h.R")

t_start_ext <- periods_6h$t_start[1]
t_end_ext   <- as.POSIXct("2024-09-16 08:00:00", tz = "UTC")

cat("\n── Extended window test (20:00 – 08:00) ──────────────────────────────\n")
cat("Window:", format(t_start_ext), "–", format(t_end_ext), "\n")

res_ext <- tryCatch(
  pressure_wind_profile(
    data          = data_sf,
    cfg           = cfg_6h,
    individual_id = periods_6h$individual_local_identifier[1],
    t_start       = t_start_ext,
    t_end         = t_end_ext,
    elev_z        = 4,
    buffer_deg    = 1.0,
    verbose       = TRUE
  ),
  error = function(e) { message("  ERROR: ", conditionMessage(e)); NULL }
)

if (!is.null(res_ext)) {
  cat("raw fixes:", nrow(res_ext$raw_data), "\n")
  cat("plot_data rows:", nrow(res_ext$plot_data), "\n")
  cat("wind_long rows:", nrow(res_ext$wind_long), "\n")
}
