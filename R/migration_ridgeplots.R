migration_ridgeplots <- function(
    data,
    plot_fun = plot_variable_env_ridges,

    # filtering
    tolerance_hours = 6,
    season_filter = "Spring",
    dist_prev_min = 50,
    ids = NULL,  # optional vector of individual_local_identifier to keep

    # ridge settings
    hours = c("-48h","-24h","0h","24h","48h"),
    p_adjust = "holm",
    show_ns = FALSE,
    verbose = TRUE,

    # color controls
    default_color = "cornflowerblue",
    colors = NULL,  # named vector: c(tp="cyan", tcc="darkgrey", ...)

    # variable definitions
    vars = NULL,     # optional custom list (see default below)

    # arranging / saving
    return_arranged = TRUE,
    save = FALSE,
    out_env_file  = NULL,
    out_wind_file = NULL,
    width = 8,
    height = 11,
    dpi = 300,
    bg = "white"
) {
  # ---- deps ----
  require(dplyr)
  require(ggpubr)

  # ---- defaults: variable spec ----
  # Each entry: env_name, env_component, transform, group ("env" or "wind"), optional main_color override
  if (is.null(vars)) {
    vars <- list(
      tp = list(env_name = "tp",       env_component = "Total precipitation (log1p)",     transform = "precip_log1p", group = "env"),
      tcc = list(env_name = "tcc",     env_component = "Total cloud cover",              transform = "none",         group = "env"),
      t2m = list(env_name = "t2m",     env_component = "Temperature (°C)",               transform = "temp_C",       group = "env"),
      msl = list(env_name = "msl",     env_component = "Mean sea level pressure (hPa)",  transform = "pressure_hPa", group = "env"),
      windsp10 = list(env_name = "windsp10",  env_component = "Wind speed (m/s)",        transform = "none",         group = "wind"),
      ws100    = list(env_name = "ws100",     env_component = "Wind support (m/s)",      transform = "none",         group = "wind"),
      cw100    = list(env_name = "cw100",     env_component = "Cross wind (m/s)",        transform = "none",         group = "wind"),
      airspeed100 = list(env_name = "airspeed100", env_component = "Air speed (m/s)",    transform = "none",         group = "wind")
    )
  }

  # ---- filter subset ----
  mig <- data %>%
    as.data.frame() %>%
    filter(
      dt_prev > (24 - tolerance_hours) * 3600,
      dt_prev < (24 + tolerance_hours) * 3600,
      !is.na(season),
      !is.na(dist_prev),
      dist_prev > dist_prev_min,
      season == season_filter
    )

  if (!is.null(ids)) {
    mig <- mig %>% filter(individual_local_identifier %in% ids)
  }

  if (nrow(mig) == 0) {
    stop("No rows after filtering. Check season_filter / dist_prev_min / tolerance_hours / ids.")
  }

  # ---- color mapping ----
  # colors can be named by key (tp/tcc/...) OR by env_name (e.g. "windsp10")
  # precedence: colors[[key]] > colors[[env_name]] > default_color
  get_col <- function(key, env_name) {
    if (!is.null(colors)) {
      if (!is.null(colors[[key]])) return(colors[[key]])
      if (!is.null(colors[[env_name]])) return(colors[[env_name]])
    }
    default_color
  }

  # ---- run plots ----
  results <- list()
  for (key in names(vars)) {
    v <- vars[[key]]

    main_color <- get_col(key, v$env_name)
    # allow explicit override inside vars
    if (!is.null(v$main_color)) main_color <- v$main_color

    results[[key]] <- plot_fun(
      data = mig,
      hours = hours,
      env_name = v$env_name,
      env_component = v$env_component,
      transform = v$transform,
      p_adjust = p_adjust,
      show_ns = show_ns,
      verbose = verbose,
      main_color = main_color
    )
  }

  # ---- arrange ----
  env_keys  <- names(vars)[vapply(vars, function(x) x$group == "env",  logical(1))]
  wind_keys <- names(vars)[vapply(vars, function(x) x$group == "wind", logical(1))]

  env_plot <- NULL
  wind_plot <- NULL

  if (return_arranged) {
    if (length(env_keys) > 0) {
      env_plot <- ggpubr::ggarrange(
        plotlist = lapply(env_keys, function(k) results[[k]]$plot),
        ncol = 1
      )
    }
    if (length(wind_keys) > 0) {
      wind_plot <- ggpubr::ggarrange(
        plotlist = lapply(wind_keys, function(k) results[[k]]$plot),
        ncol = 1
      )
    }
  }

  # ---- save ----
  if (save) {
    if (!is.null(env_plot) && !is.null(out_env_file)) {
      ggplot2::ggsave(out_env_file, env_plot, width = width, height = height, dpi = dpi, bg = bg)
    }
    if (!is.null(wind_plot) && !is.null(out_wind_file)) {
      ggplot2::ggsave(out_wind_file, wind_plot, width = width, height = height, dpi = dpi, bg = bg)
    }
  }

  invisible(list(
    data = mig,
    results = results,      # each element contains $plot + test summary (as returned by your plot_fun)
    env_plot = env_plot,
    wind_plot = wind_plot
  ))
}


# cols <- c(
#   tp = "cyan",
#   tcc = "darkgrey",
#   t2m = "darkred",
#   msl = "purple",
#   windsp10 = "cornflowerblue",
#   ws100 = "gold",
#   cw100 = "darkorange",
#   airspeed100 = "darkgreen"
# )
#
# out <- make_leisler_migration_ridgeplots(
#   data = b_daily_wind,
#   tolerance_hours = 6,
#   season_filter = "Spring",
#   dist_prev_min = 50,
#   hours = c("-48h","-24h","0h","24h","48h"),
#   colors = cols,
#   default_color = "black",
#   save = TRUE,
#   out_env_file  = "../../../Dropbox/MPI/Leisleri/Plots/env_daily_leisleri_migration.png",
#   out_wind_file = "../../../Dropbox/MPI/Leisleri/Plots/wind_daily_leisleri_migration.png",
#   width = 8, height = 11, dpi = 300, bg = "white"
# )
#
# out$env_plot
# out$wind_plot
