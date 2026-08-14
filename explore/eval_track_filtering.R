# ------------------------------------------------------------
# Evaluate SSM-based filtering/smoothing of noisy Sigfox fixes
# against the practice used for Argos/GPS telemetry (aniMotum,
# successor to foieGras) and Stan-HMM radio-telemetry filtering
# (movetrack, Ruppel et al. 2026 MEE).
#
# Data: move_icarus_lasiopterus.robj (move2 object `b`), Nyctalus
# lasiopterus Sigfox/TinyFox tracks. sigfox_computed_location_radius
# is the per-fix position-error radius (m); n_base_stations the
# number of base stations used to solve the fix.
# ------------------------------------------------------------
suppressPackageStartupMessages({
  library(move2)
  library(sf)
  library(dplyr)
  library(ggplot2)
  library(aniMotum)
})

data_path <- "C:/Users/Edward/Dropbox/MPI/Noctule/Data/rdata/move_icarus_lasiopterus.robj"
out_dir <- "C:/Users/Edward/AppData/Local/Temp/claude/c--Users-Edward-Desktop-Github-SigfoxTagPrep/57a84e43-b30b-46ef-89fb-e635048cb890/scratchpad"

e <- new.env()
load(data_path, envir = e)
b <- get("b", envir = e)
df_all <- as.data.frame(sf::st_drop_geometry(b))

track_ids <- c(
  general   = "9002046552", # largest track (226 fixes), mixed n_base_stations
  singlebs  = "8674408496", # 125 fixes, 100% single-base-station fixes
  outlier   = "9002046556"  # contains multiple genuine ~150-300 km/h Sigfox-error
                             # jumps (30-min intervals, radius 3.3-17.6 km -- the
                             # jump distance is far larger than the stated radius,
                             # so these are true multilateration errors, not the
                             # NA-radius "deploy start" pseudo-fix artifact)
)

# ---- build a common raw table per track --------------------------------
build_raw <- function(id) {
  sub <- df_all %>%
    filter(deployment_id == id) %>%
    arrange(timestamp) %>%
    transmute(
      id = deployment_id,
      date = timestamp,
      lon = lon,
      lat = lat,
      radius_m = sigfox_computed_location_radius,
      n_base_stations = n_base_stations,
      lqi = sigfox_lqi
    ) %>%
    filter(!is.na(lon), !is.na(lat), !is.na(date), !is.na(radius_m)) %>%
    distinct(date, .keep_all = TRUE) %>%
    arrange(date)
  sub
}

raw_list <- lapply(track_ids, build_raw)
names(raw_list) <- names(track_ids)

cat("Raw fix counts per track:\n")
print(sapply(raw_list, nrow))

# ---- Method A: aniMotum SSM, generic-location (GL) format --------------
# radius (m, isotropic) -> per-fix lon/lat standard errors in degrees.
# 1 deg latitude ~= 111320 m; 1 deg longitude ~= 111320*cos(lat) m.
to_animotum_gl <- function(raw) {
  raw %>%
    mutate(
      lc = "GL",
      x.sd = radius_m / (111320 * cos(lat * pi / 180)),
      y.sd = radius_m / 111320
    ) %>%
    select(id, date, lc, lon, lat, x.sd, y.sd)
}

# Nyctalus lasiopterus: generous vmax for migratory flight bursts (m/s).
VMAX_MS <- 20

fit_one <- function(raw, model = "crw", time.step = NA) {
  x <- to_animotum_gl(raw)
  t0 <- Sys.time()
  fit <- tryCatch(
    fit_ssm(x, vmax = VMAX_MS, model = model, time.step = time.step,
            control = ssm_control(verbose = 0)),
    error = function(e) { message("fit_ssm failed: ", conditionMessage(e)); NULL }
  )
  elapsed <- as.numeric(Sys.time() - t0, units = "secs")
  list(fit = fit, elapsed = elapsed)
}

results_A <- list()
for (nm in names(raw_list)) {
  cat("\n=== aniMotum fit:", nm, "(", nrow(raw_list[[nm]]), "fixes) ===\n")
  res <- fit_one(raw_list[[nm]])
  results_A[[nm]] <- res
  cat("elapsed (s):", round(res$elapsed, 2), "\n")
  if (!is.null(res$fit)) {
    print(res$fit)
    # prefilter flags: keep == FALSE means aniMotum's speed/angle/distlim
    # prefilter rejected this observation before fitting the SSM
    pf <- res$fit$ssm[[1]]$data
    n_flagged <- sum(!pf$keep, na.rm = TRUE)
    cat("prefilter-flagged fixes:", n_flagged, "/", nrow(pf), "\n")
  }
}

saveRDS(results_A, file.path(out_dir, "results_animotum.rds"))

# ---- plot raw vs fitted for each track ----------------------------------
plot_compare <- function(raw, fit_obj, title) {
  raw_sf <- raw
  p <- ggplot() +
    geom_path(data = raw_sf, aes(lon, lat), colour = "grey60", linewidth = 0.3) +
    geom_point(data = raw_sf, aes(lon, lat, size = radius_m), colour = "grey40", alpha = 0.5) +
    scale_size_continuous(name = "radius (m)", range = c(0.5, 4))
  if (!is.null(fit_obj)) {
    fitted <- grab(fit_obj, what = "fitted", as_sf = FALSE)
    p <- p +
      geom_path(data = fitted, aes(lon, lat), colour = "steelblue", linewidth = 0.6) +
      geom_point(data = fitted, aes(lon, lat), colour = "steelblue", size = 0.8)
  }
  p + labs(title = title, x = NULL, y = NULL) + theme_minimal()
}

for (nm in names(raw_list)) {
  p <- plot_compare(raw_list[[nm]], results_A[[nm]]$fit,
                     title = paste0("aniMotum CRW SSM — ", nm, " (", track_ids[[nm]], ")"))
  ggsave(file.path(out_dir, paste0("animotum_", nm, ".png")), p, width = 7, height = 6, dpi = 120)
}

cat("\nDone. Plots + results saved to:", out_dir, "\n")
