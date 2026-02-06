plot_variable_env_ridges <- function(
    data,
    env_name,
    env_component,
    hours,
    transform = c("none", "temp_C", "pressure_hPa", "precip_log1p"),
    transform_fn = NULL,                 # optional custom function(x)
    log_base = exp(1),
    main_color = "cornflowerblue",
    ridge_scale = 1.5,

    # --- stats options ---
    ref_day = "0",                       # compare all days to this label (after /24)
    paired = FALSE,
    p_adjust = c("holm","BH","bonferroni","none"),
    show_ns = FALSE,
    label_x = c("right", "quantile"),    # where to place stars
    label_quantile = 0.98,
    verbose = TRUE
) {
  require(dplyr)
  require(ggplot2)
  require(ggridges)
  require(tidyr)

  transform <- match.arg(transform)
  p_adjust  <- match.arg(p_adjust)
  label_x   <- match.arg(label_x)

  # ---- columns expected: env_name_<hours> e.g., tp_-24h ----
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

  # ---- transform ----
  if (!is.null(transform_fn)) {
    long_data$value <- transform_fn(long_data$value)
    if (verbose) message("Applied custom transform_fn().")
  } else if (transform == "temp_C") {
    long_data$value <- long_data$value - 272.15
  } else if (transform == "pressure_hPa") {
    long_data$value <- long_data$value / 100
  } else if (transform == "precip_log1p") {
    # stable for zeros; handles negatives just in case
    if (isTRUE(all(long_data$value >= 0, na.rm = TRUE))) {
      long_data$value <- log1p(long_data$value)
    } else {
      long_data$value <- sign(long_data$value) * log1p(abs(long_data$value))
    }
    if (!isTRUE(all.equal(log_base, exp(1)))) {
      long_data$value <- long_data$value / log(log_base)
    }
  }

  # ---- parse suffix to day labels ----
  suf <- sub(paste0("^", env_name, "_"), "", required_cols)   # "-48h", ...
  hrs <- as.numeric(sub("h$", "", suf))                      # -48, ...
  if (anyNA(hrs)) stop("Could not parse hours from: ", paste(required_cols, collapse = ", "))
  days_from_zero <- hrs / 24
  day_labels <- as.character(days_from_zero)                 # "-2","-1","0","1","2"
  names(day_labels) <- required_cols

  long_data$time <- factor(long_data$time, levels = required_cols, labels = day_labels)

  # ---- colors ----
  if (length(unique(day_labels)) == 1) {
    colors_vector <- setNames(main_color, unique(day_labels))
  } else {
    ord <- order(days_from_zero)
    ramp <- colorRampPalette(c("white", main_color, "white"))
    cols <- ramp(length(ord))
    colors_vector <- setNames(cols, day_labels[ord])
  }

  # ---- pairwise Wilcoxon: each day vs ref_day ----
  split_vals <- split(long_data$value, long_data$time)
  if (!ref_day %in% names(split_vals)) {
    stop("ref_day='", ref_day, "' not found in available days: ",
         paste(names(split_vals), collapse = ", "))
  }

  ref_vals <- split_vals[[ref_day]]

  tests <- lapply(names(split_vals), function(day) {
    x <- split_vals[[day]]
    if (day == ref_day) return(NULL)

    # remove NA (should already be finite, but safe)
    x <- x[is.finite(x)]
    y <- ref_vals[is.finite(ref_vals)]

    if (length(x) < 1 || length(y) < 1) {
      return(data.frame(day = day, n = length(x), n_ref = length(y),
                        p = NA_real_, statistic = NA_real_, stringsAsFactors = FALSE))
    }

    if (paired) {
      nmin <- min(length(x), length(y))
      x <- x[seq_len(nmin)]
      y <- y[seq_len(nmin)]
    }

    wt <- suppressWarnings(stats::wilcox.test(x, y, paired = paired, exact = FALSE))
    data.frame(
      day = day,
      n = length(x),
      n_ref = length(y),
      statistic = unname(wt$statistic),
      p = wt$p.value,
      stringsAsFactors = FALSE
    )
  })

  tests_df <- dplyr::bind_rows(tests)

  if (nrow(tests_df) > 0) {
    if (p_adjust == "none") {
      tests_df$p_adj <- tests_df$p
    } else {
      tests_df$p_adj <- stats::p.adjust(tests_df$p, method = p_adjust)
    }

    tests_df$sig <- dplyr::case_when(
      is.na(tests_df$p_adj)        ~ NA_character_,
      tests_df$p_adj <= 0.001      ~ "***",
      tests_df$p_adj <= 0.01       ~ "**",
      tests_df$p_adj <= 0.05       ~ "*",
      TRUE                         ~ "ns"
    )
  } else {
    tests_df <- data.frame(day=character(), n=integer(), n_ref=integer(),
                           statistic=double(), p=double(), p_adj=double(), sig=character())
  }

  # ---- annotation positions (one per day) ----
  # Put stars at far right (or at a quantile) for each ridge labels
  x_all <- long_data$value
  x_rng <- range(x_all, na.rm = TRUE)
  x_pad <- diff(x_rng) * 0.06
  if (!is.finite(x_pad)) x_pad <- 0.5

  ann <- tests_df %>%
    dplyr::mutate(
      time = factor(day, levels = levels(long_data$time)),
      label = sig
    ) %>%
    dplyr::filter(!is.na(label)) %>%
    { if (!show_ns) dplyr::filter(., label != "ns") else . } %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      x = if (label_x == "right") {
        x_rng[2] + x_pad
      } else {
        # per-day quantile so label sits near the right tail of that ridge
        vals <- split_vals[[as.character(day)]]
        stats::quantile(vals, probs = label_quantile, na.rm = TRUE, names = FALSE)
      }
    ) %>%
    dplyr::ungroup()

  # ---- plot ----
  p <- ggplot(long_data, aes(x = value, y = time, fill = time)) +
    geom_density_ridges(
      scale = ridge_scale, size = 0.3,
      quantile_lines = TRUE, quantiles = 2,
      alpha = 1, vline_size = 0.5, vline_color = "black"
    ) +
    scale_fill_manual(values = colors_vector) +
    theme_ridges(font_size = 11, font_family = "Helvetica") +
    theme(
      legend.position = "none",
      plot.title = element_blank(),
      plot.margin = margin(5.5, 30, 5.5, 5.5) # extra right margin for stars
    ) +
    ylab("Days to migration") +
    xlab(env_component) +
    coord_cartesian(clip = "off")

  if (nrow(ann) > 0) {
    p <- p + geom_text(
      data = ann,
      aes(x = x, y = time, label = label),
      inherit.aes = FALSE,
      hjust = 0, vjust = 0.5, size = 4
    )
  }

  # ---- print summary ----
  if (verbose) {
    message("Wilcoxon tests vs day ", ref_day, " (paired=", paired, ", p_adjust=", p_adjust, "):")
    print(
      tests_df %>%
        dplyr::arrange(as.numeric(day)) %>%
        dplyr::mutate(across(c(p, p_adj), ~ signif(.x, 3)))
    )
  }

  invisible(list(plot = p, tests = tests_df))
}

library(ggpubr)
migs <- b_daily_wind %>% as.data.frame() %>%
  filter(dt_prev > 20*3600, dt_prev < 30 *3600,
        abs(d_displacement) > displacement_d_threshold,
        season == "Spring") # 95 rows

# remove fall spanish bats
bats <- unique(b_daily_wind$individual_local_identifier[b_daily_wind$season == "Fall"])
spanish_bats <- c(which(grepl(pattern = "catalonia", bats)),
                  which(grepl(pattern = "navarre", bats)))


migf <- b_daily_wind %>% as.data.frame() %>%
  filter(dt_prev > 20*3600, dt_prev < 30 *3600,
         abs(d_displacement) > displacement_d_threshold,
         season == "Fall", # 145 rows
         individual_local_identifier %in% bats[-spanish_bats]) # 130 rows

# Total Precipitation ----
rs_tp <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "tp",
  env_component = "Total precipitation (log1p)",
  transform = "precip_log1p",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)
rf_tp <- plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "tp",
  env_component = "Total precipitation (log1p)",
  transform = "precip_log1p",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)
ggarrange(rs_tp$plot, rf_tp$plot, ncol = 1)

# Total Cloud Cover ----
rs_tcc <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "tcc",
  env_component = "Total cloud cover",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)
rs_tcc
rf_tcc <- plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "tcc",
  env_component = "Total cloud cover",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)
ggarrange(rs_tcc$plot, rf_tcc$plot, ncol = 1)

# Temperature ----
rs_t2m <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "t2m",
  env_component = "Total cloud cover",
  transform = "temp_C",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)

rf_t2m <-  plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "t2m",
  env_component = "Total cloud cover",
  transform = "temp_C",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)

ggarrange(rs_t2m$plot, rf_t2m$plot, ncol = 1)

# Pressure ----
rs_msl <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "msl",
  env_component = "Mean sea level pressure (hPa)",
  transform = "pressure_hPa",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)

rf_msl <-  plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "msl",
  env_component = "Mean sea level pressure (hPa)",
  transform = "pressure_hPa",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)

ggarrange(rs_msl$plot, rf_msl$plot, ncol = 1)

# Wind speed ----
rs_windsp <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "windsp100",
  env_component = "Wind speed (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)

rf_windsp <-  plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "windsp100",
  env_component = "Wind speed (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)

ggarrange(rs_windsp$plot, rf_windsp$plot, ncol = 1)

# Wind support ----
rs_ws <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "ws100",
  env_component = "Wind support (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)

rf_ws <-  plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "ws100",
  env_component = "Wind support (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)

ggarrange(rs_ws$plot, rf_ws$plot, ncol = 1)

# Air speed ----
rs_airspeed <- plot_variable_env_ridges(
  data = migs,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "airspeed100",
  env_component = "Air speed (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  verbose = TRUE
)

rf_airspeed <-  plot_variable_env_ridges(
  data = migf,
  hours = c("-48h","-24h","0h","24h","48h"),
  env_name = "airspeed100",
  env_component = "Air speed (m/s)",
  transform = "none",
  p_adjust = "holm",
  show_ns = FALSE,
  main_color = "darkorange",
  verbose = TRUE
)

ggarrange(rs_airspeed$plot, rf_airspeed$plot, ncol = 1)

