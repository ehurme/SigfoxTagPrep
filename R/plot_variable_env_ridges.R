plot_variable_env_ridges <- function(
    data,
    env_name,
    env_component,
    hours,
    transform = c("none", "temp_C", "pressure_hPa", "precip_log1p"),
    transform_fn = NULL,                 # optional custom function(x) ...
    log_base = exp(1),                   # used if precip_log1p with base != e
    main_color = "cornflowerblue",
    ridge_scale = 1.5,
    verbose = FALSE
) {
  require(dplyr)
  require(ggplot2)
  require(ggridges)
  require(tidyr)

  transform <- match.arg(transform)

  # ---- columns ----
  required_cols <- paste0(env_name, "_", hours)
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    stop("Missing columns: ", paste(missing_cols, collapse = ", "),
         ". Check env_name/hours and your column suffixes.")
  }

  # ---- long ----
  long_data <- data %>%
    dplyr::select(dplyr::all_of(required_cols)) %>%
    tidyr::pivot_longer(cols = everything(), names_to = "time", values_to = "value") %>%
    dplyr::filter(is.finite(value))

  # ---- apply transformation ----
  # (transform_fn overrides transform)
  if (!is.null(transform_fn)) {
    long_data$value <- transform_fn(long_data$value)
    if (verbose) message("Applied custom transform_fn().")
  } else if (transform == "temp_C") {
    # Kelvin -> Celsius (you asked for 272.15 specifically)
    long_data$value <- long_data$value - 272.15
  } else if (transform == "pressure_hPa") {
    # Pa -> hPa
    long_data$value <- long_data$value / 100
  } else if (transform == "precip_log1p") {
    # log(1 + x) (handles zeros safely); allow base change
    if (isTRUE(all(long_data$value >= 0, na.rm = TRUE))) {
      long_data$value <- log1p(long_data$value)
    } else {
      # if negatives exist, fall back to log(abs)+sign handling
      long_data$value <- sign(long_data$value) * log1p(abs(long_data$value))
    }
    if (!isTRUE(all.equal(log_base, exp(1)))) {
      long_data$value <- long_data$value / log(log_base)
    }
  }

  # ---- parse suffix to days ----
  suf <- sub(paste0("^", env_name, "_"), "", required_cols)   # "-48h", ...
  hrs <- as.numeric(sub("h$", "", suf))                      # -48, ...
  if (anyNA(hrs)) stop("Could not parse hours from: ", paste(required_cols, collapse = ", "))
  days_from_zero <- hrs / 24
  labels <- as.character(days_from_zero)                      # "-2","-1","0","1","2"
  names(labels) <- required_cols

  long_data$time <- factor(long_data$time, levels = required_cols, labels = labels)

  # ---- colors ----
  u_days <- unique(days_from_zero)
  if (length(u_days) == 1) {
    colors_vector <- setNames(main_color, labels[1])
  } else {
    ord <- order(days_from_zero)
    labels_ord <- labels[ord]
    ramp <- colorRampPalette(c("white", main_color, "white"))
    cols <- ramp(length(labels_ord))
    colors_vector <- setNames(cols, labels_ord)
  }

  # ---- plot ----
  ggplot(long_data, aes(x = value, y = time, fill = time)) +
    geom_density_ridges(
      scale = ridge_scale, size = 0.3,
      quantile_lines = TRUE, quantiles = 2,
      alpha = 1, vline_size = 0.5, vline_color = "black"
    ) +
    scale_fill_manual(values = colors_vector) +
    theme_ridges(font_size = 11, font_family = "Helvetica") +
    theme(legend.position = "none", plot.title = element_blank()) +
    ylab("Days to migration") +
    xlab(env_component)
}

# run of migration data
migs <- b_daily_wind %>% as.data.frame() %>%
  filter(season == "Spring",
         dt_prev > 20 * 3600,
         dt_prev < 30 * 3600,
         migration_night == 1)
migf <- b_daily_wind %>% as.data.frame() %>%
  filter(season == "Fall",
         dt_prev > 20 * 3600,
         dt_prev < 30 * 3600,
         migration_night == 1)

## t2m ----
s_t2m <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "t2m",
  env_component = "2m temperature (°C)",
  transform = "temp_C"
)

f_t2m <- plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "t2m",
  env_component = "2m temperature (°C)",
  transform = "temp_C", main = "darkorange"
)

ggarrange(s_t2m, f_t2m, ncol = 1)
## msl ----
plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "msl",
  env_component = "Mean sea-level pressure (hPa)",
  transform = "pressure_hPa"
)

plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "msl",
  env_component = "Mean sea-level pressure (hPa)",
  transform = "pressure_hPa",
  main_color = "darkorange"
)

# wind ----
## windsp ----
plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "windsp100",
  env_component = "Wind speed (m/s)",
  transform = "none"
)

plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "windsp100",
  env_component = "Wind speed (m/s)",
  transform = "none", main_color = "darkorange"
)

## airspeed ----
plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "airspeed100",
  env_component = "Air speed (m/s)",
  transform = "none"
)

plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "airspeed100",
  env_component = "Air speed (m/s)",
  transform = "none", main_color = "darkorange"
)

plot_variable_env_ridges(
  data = b_daily_wind %>% as.data.frame(),
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "tp",
  env_component = "Total precipitation (mm, log1p)",
  transform_fn = function(x) log1p(x * 1000)
)

