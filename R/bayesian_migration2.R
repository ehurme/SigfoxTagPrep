suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(sf)
  library(move2)
  library(cmdstanr)
  library(posterior)
  library(ggplot2)
})

# helpers ----

guess_nanofox_columns <- function(x) {
  nm <- names(x)

  pick_first <- function(candidates, nm) {
    hit <- candidates[candidates %in% nm]
    if (length(hit) == 0) return(NA_character_)
    hit[1]
  }

  list(
    id_col = pick_first(
      c("individual_local_identifier", "tag_local_identifier", "local_identifier"),
      nm
    ),
    time_col = pick_first(
      c("timestamp", "event_time", "time"),
      nm
    ),
    accel_col = pick_first(
      c("vedba_sum", "VeDBA", "vedba", "acceleration"),
      nm
    ),
    temp_col = pick_first(
      c("avg.temp", "external temperature", "temperature", "temp"),
      nm
    ),
    pressure_col = pick_first(
      c("min.baro.pressure", "barometer", "pressure"),
      nm
    )
  )
}

safe_scale <- function(x) {
  x <- as.numeric(x)
  ok <- is.finite(x)
  if (sum(ok) <= 1) return(rep(0, length(x)))
  mu <- mean(x[ok], na.rm = TRUE)
  s  <- sd(x[ok], na.rm = TRUE)
  if (!is.finite(s) || s <= 0) s <- 1
  out <- (x - mu) / s
  out[!is.finite(out)] <- 0
  out
}

safe_log <- function(x, eps = 1e-6) {
  x <- as.numeric(x)
  x[!is.finite(x)] <- NA_real_
  log(pmax(x, eps))
}

plot_nanofox_track_states <- function(seg) {
  ggplot(seg, aes(x, y, color = state_label, group = id)) +
    geom_path(alpha = 0.7) +
    geom_point(size = 0.6) +
    #facet_wrap(~ id, scales = "free") +
    coord_equal() +
    theme_bw()
}

plot_hmm_state_params <- function(
    fit_summary,
    include_kappa = TRUE,
    interval = c("q5", "q95"),
    free_y = TRUE
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(stringr)
    library(tidyr)
    library(ggplot2)
  })

  interval <- match.arg(interval[1], choices = c("q5", "q95"))
  lo_col <- "q5"
  hi_col <- "q95"

  plot_df <- fit_summary %>%
    mutate(variable = as.character(variable)) %>%
    filter(
      str_detect(variable, "^mu_") |
        str_detect(variable, "^sigma_") |
        (include_kappa & str_detect(variable, "^kappa_angle"))
    ) %>%
    mutate(
      family = case_when(
        str_detect(variable, "^mu_") ~ "mu",
        str_detect(variable, "^sigma_") ~ "sigma",
        str_detect(variable, "^kappa_angle") ~ "kappa",
        TRUE ~ NA_character_
      ),
      measure = str_match(variable, "^(mu|sigma)_([A-Za-z0-9\\.]+)\\[(\\d+)\\]$")[, 3],
      state = str_match(variable, ".*\\[(\\d+)\\]$")[, 2],
      state = as.integer(state),
      measure = case_when(
        variable == "kappa_angle[1]" ~ "angle",
        variable == "kappa_angle[2]" ~ "angle",
        variable == "kappa_angle[3]" ~ "angle",
        TRUE ~ measure
      ),
      measure = factor(
        measure,
        levels = c("speed", "progress", "accel", "temp", "angle")
      ),
      family = factor(family, levels = c("mu", "sigma", "kappa"))
    ) %>%
    filter(!is.na(state), !is.na(family))

  if (nrow(plot_df) == 0) {
    stop("No matching mu/sigma/kappa parameters found in fit_summary.")
  }

  p <- ggplot(
    plot_df,
    aes(x = factor(state), y = mean, ymin = .data[[lo_col]], ymax = .data[[hi_col]])
  ) +
    geom_hline(yintercept = 0, linetype = 2, linewidth = 0.3) +
    geom_pointrange() +
    facet_grid(
      family ~ measure,
      scales = if (free_y) "free_y" else "fixed",
      drop = FALSE
    ) +
    theme_bw() +
    labs(
      x = "State",
      y = "Posterior mean and 90% interval",
      title = "HMM state parameter estimates"
    )

  p
}

tidy_hmm_state_params <- function(fit_summary, include_kappa = TRUE) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(stringr)
  })

  fit_summary %>%
    mutate(variable = as.character(variable)) %>%
    filter(
      str_detect(variable, "^mu_") |
        str_detect(variable, "^sigma_") |
        (include_kappa & str_detect(variable, "^kappa_angle"))
    ) %>%
    mutate(
      family = case_when(
        str_detect(variable, "^mu_") ~ "mu",
        str_detect(variable, "^sigma_") ~ "sigma",
        str_detect(variable, "^kappa_angle") ~ "kappa",
        TRUE ~ NA_character_
      ),
      measure = str_match(variable, "^(mu|sigma)_([A-Za-z0-9\\.]+)\\[(\\d+)\\]$")[, 3],
      state = str_match(variable, ".*\\[(\\d+)\\]$")[, 2],
      state = as.integer(state),
      measure = case_when(
        str_detect(variable, "^kappa_angle") ~ "angle",
        TRUE ~ measure
      )
    ) %>%
    select(family, measure, state, mean, median, sd, q5, q95, rhat, ess_bulk, ess_tail) %>%
    arrange(family, measure, state)
}

plot_hmm_state_profiles <- function(fit_summary) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(stringr)
    library(ggplot2)
  })

  df <- fit_summary %>%
    mutate(variable = as.character(variable)) %>%
    filter(str_detect(variable, "^mu_")) %>%
    mutate(
      measure = str_match(variable, "^mu_([A-Za-z0-9\\.]+)\\[(\\d+)\\]$")[, 2],
      state   = as.integer(str_match(variable, "^mu_([A-Za-z0-9\\.]+)\\[(\\d+)\\]$")[, 3])
    ) %>%
    filter(!is.na(measure), !is.na(state))

  ggplot(df, aes(x = measure, y = mean, group = factor(state), shape = factor(state))) +
    geom_hline(yintercept = 0, linetype = 2, linewidth = 0.3) +
    geom_point(size = 2) +
    geom_line() +
    geom_errorbar(aes(ymin = q5, ymax = q95), width = 0.1) +
    theme_bw() +
    labs(
      x = "Measure",
      y = "Posterior mean",
      shape = "State",
      title = "State mean profiles"
    )
}

# prepare interval-level data ----


prepare_nanofox_hmm_data <- function(
    x,
    crs_projected = 3035,
    expected_dt_hours = 3,
    daily_migration_km = 40
) {
  stopifnot(inherits(x, "move2"))

  cols <- guess_nanofox_columns(x)

  if (is.na(cols$id_col)) stop("Could not detect ID column.")
  if (is.na(cols$time_col)) stop("Could not detect timestamp column.")

  x_proj <- sf::st_transform(x, crs_projected)
  xy_proj <- sf::st_coordinates(x_proj)

  x_ll <- sf::st_transform(x, 4326)
  xy_ll <- sf::st_coordinates(x_ll)

  df <- x_proj %>%
    sf::st_drop_geometry() %>%
    mutate(
      x = xy_proj[, 1],
      y = xy_proj[, 2],
      longitude = xy_ll[, 1],
      latitude  = xy_ll[, 2],
      id = .data[[cols$id_col]],
      timestamp = as.POSIXct(.data[[cols$time_col]], tz = "UTC")
    ) %>%
    arrange(id, timestamp)

  df <- df %>%
    mutate(
      accel_raw = if (!is.na(cols$accel_col)) .data[[cols$accel_col]] else NA_real_,
      temp_raw  = if (!is.na(cols$temp_col))  .data[[cols$temp_col]]  else NA_real_,
      pressure_raw = if (!is.na(cols$pressure_col)) .data[[cols$pressure_col]] else NA_real_
    )

  df <- df %>%
    group_by(id) %>%
    mutate(
      row_in_track = row_number(),
      timestamp_prev = lag(timestamp),
      x_prev = lag(x),
      y_prev = lag(y),
      dx = x - x_prev,
      dy = y - y_prev,
      step_m = sqrt(dx^2 + dy^2),
      dt_sec = as.numeric(difftime(timestamp, timestamp_prev, units = "secs")),
      dt_hours = dt_sec / 3600,
      speed_m_h = step_m / dt_hours,
      log_speed = safe_log(speed_m_h),
      heading = atan2(dy, dx),
      heading_prev = lag(heading),
      turn_angle = atan2(sin(heading - heading_prev), cos(heading - heading_prev)),
      t_mid = timestamp_prev + (timestamp - timestamp_prev) / 2
    ) %>%
    ungroup()

  origin_df <- df %>%
    group_by(id) %>%
    summarise(
      x0 = first(x),
      y0 = first(y),
      .groups = "drop"
    )

  df <- df %>%
    left_join(origin_df, by = "id") %>%
    group_by(id) %>%
    mutate(
      net_disp_m = sqrt((x - x0)^2 + (y - y0)^2),
      net_disp_prev_m = lag(net_disp_m),
      delta_disp_m = net_disp_m - net_disp_prev_m,
      progress_m_h = delta_disp_m / dt_hours,
      directedness = delta_disp_m / pmax(step_m, 1),
      directedness = pmax(pmin(directedness, 1), -1)
    ) %>%
    ungroup()

  df <- df %>%
    mutate(
      hour_mid = hour(t_mid),
      is_night = ifelse(hour_mid >= 19 | hour_mid <= 6, 1L, 0L),
      yday_mid = yday(t_mid),
      yday_sin = sin(2 * pi * yday_mid / 366),
      yday_cos = cos(2 * pi * yday_mid / 366),
      month_mid = month(t_mid),
      season = case_when(
        month_mid %in% c(3, 4, 5, 6) ~ "spring",
        month_mid %in% c(8, 9, 10, 11) ~ "fall",
        TRUE ~ "other"
      ),
      log_accel = safe_log(accel_raw + 1e-6),
      temp_c = as.numeric(temp_raw)
    )

  migration_progress_threshold_m_h <- daily_migration_km * 1000 / 24

  df <- df %>%
    mutate(
      prior_migration_flag = ifelse(progress_m_h >= migration_progress_threshold_m_h, 1L, 0L)
    ) %>%
    filter(
      !is.na(id),
      !is.na(timestamp),
      !is.na(timestamp_prev),
      is.finite(dt_hours),
      dt_hours > 0,
      is.finite(step_m),
      is.finite(speed_m_h),
      is.finite(log_speed),
      is.finite(progress_m_h),
      is.finite(directedness)
    ) %>%
    mutate(
      has_accel = ifelse(is.finite(log_accel), 1L, 0L),
      has_temp  = ifelse(is.finite(temp_c), 1L, 0L),
      has_angle = ifelse(is.finite(turn_angle) & step_m > 25, 1L, 0L),
      log_accel = ifelse(has_accel == 1L, log_accel, 0),
      temp_c    = ifelse(has_temp == 1L, temp_c, 0),
      turn_angle = ifelse(has_angle == 1L, turn_angle, 0)
    ) %>%
    group_by(id) %>%
    arrange(timestamp, .by_group = TRUE) %>%
    mutate(track_start = ifelse(row_number() == 1, 1L, 0L)) %>%
    ungroup() %>%
    mutate(
      z_log_speed = safe_scale(log_speed),
      z_progress  = safe_scale(progress_m_h),
      z_directed  = safe_scale(directedness),
      z_log_accel = safe_scale(log_accel),
      z_temp      = safe_scale(temp_c),
      z_log_dt    = safe_scale(log(dt_hours / expected_dt_hours))
    )

  num_cols <- c(
    "z_log_speed", "z_progress", "z_directed",
    "z_log_accel", "z_temp", "z_log_dt",
    "turn_angle", "latitude", "longitude",
    "yday_sin", "yday_cos"
  )

  for (nm in num_cols) {
    if (nm %in% names(df)) {
      df[[nm]][!is.finite(df[[nm]])] <- 0
    }
  }

  df
}

# transition covariates ----

# build_transition_covariates <- function(df) {
#   season_spring <- ifelse(df$season == "spring", 1, 0)
#   season_fall   <- ifelse(df$season == "fall", 1, 0)
#
#   X <- cbind(
#     intercept     = 1,
#     night         = df$is_night,
#     log_dt        = df$z_log_dt,
#     season_spring = season_spring,
#     season_fall   = season_fall,
#     yday_sin      = df$yday_sin,
#     yday_cos      = df$yday_cos
#   )
#
#   X <- as.matrix(X)
#   X[!is.finite(X)] <- 0
#   X
# }
build_transition_covariates <- function(df) {
  X <- cbind(
    intercept = 1,
    night = df$is_night
  )

  X <- as.matrix(X)
  X[!is.finite(X)] <- 0
  X
}

make_initial_state_guesses <- function(df, daily_migration_km = 40) {
  migration_progress_threshold_m_h <- daily_migration_km * 1000 / 24

  inactive_rule <- rep(FALSE, nrow(df))
  if (all(c("log_accel", "temp_c") %in% names(df))) {
    accel_cut <- quantile(df$log_accel[df$has_accel == 1], 0.35, na.rm = TRUE)
    temp_cut  <- quantile(df$temp_c[df$has_temp == 1], 0.40, na.rm = TRUE)

    inactive_rule <- (df$has_accel == 1 & df$log_accel <= accel_cut) &
      (df$has_temp == 1 & df$temp_c <= temp_cut)
  }

  migration_rule <- df$progress_m_h >= migration_progress_threshold_m_h |
    df$prior_migration_flag == 1

  ifelse(migration_rule, 3L, ifelse(inactive_rule, 1L, 2L))
}

build_stan_data_nanofox_3state <- function(df) {
  X_tr <- build_transition_covariates(df)

  stan_data <- list(
    N = nrow(df),
    K = 3L,
    P = ncol(X_tr),
    log_speed = as.numeric(df$z_log_speed),
    progress  = as.numeric(df$z_progress),
    directed  = as.numeric(df$z_directed),
    accel = as.numeric(df$z_log_accel),
    has_accel = as.integer(df$has_accel),
    temp = as.numeric(df$z_temp),
    has_temp = as.integer(df$has_temp),
    angle = as.numeric(df$turn_angle),
    has_angle = as.integer(df$has_angle),
    track_start = as.integer(df$track_start),
    X_tr = X_tr
  )

  stan_data$log_speed[!is.finite(stan_data$log_speed)] <- 0
  stan_data$progress[!is.finite(stan_data$progress)] <- 0
  stan_data$directed[!is.finite(stan_data$directed)] <- 0
  stan_data$accel[!is.finite(stan_data$accel)] <- 0
  stan_data$temp[!is.finite(stan_data$temp)] <- 0
  stan_data$angle[!is.finite(stan_data$angle)] <- 0
  stan_data$X_tr[!is.finite(stan_data$X_tr)] <- 0

  stan_data
}

# Stan model ----

stan_code <- '
data {
  int<lower=1> N;
  int<lower=1> K;
  int<lower=1> P;

  vector[N] log_speed;
  vector[N] progress;

  vector[N] accel;
  array[N] int<lower=0, upper=1> has_accel;

  vector[N] temp;
  array[N] int<lower=0, upper=1> has_temp;

  array[N] int<lower=0, upper=1> track_start;
  matrix[N, P] X_tr;
}

parameters {
  simplex[K] init;
  array[K] matrix[K - 1, P] beta_tr;

  ordered[K] mu_speed;
  vector<lower=0>[K] sigma_speed;

  vector[K] mu_accel;
  vector<lower=0>[K] sigma_accel;

  vector[K] mu_temp;
  vector<lower=0>[K] sigma_temp;
}

model {
  array[N] vector[K] log_alpha;
  matrix[N, K] log_emiss;

  mu_speed[1] ~ normal(-1.5, 0.6);
  mu_speed[2] ~ normal( 0.0, 0.6);
  mu_speed[3] ~ normal( 1.5, 0.7);
  sigma_speed ~ exponential(1.5);

  mu_progress[1] ~ normal(-0.7, 0.7);
  mu_progress[2] ~ normal(-0.2, 0.7);
  mu_progress[3] ~ normal( 1.2, 0.7);
  sigma_progress ~ exponential(1.5);

  mu_directed[1] ~ normal(-0.2, 0.7);
  mu_directed[2] ~ normal( 0.0, 0.7);
  mu_directed[3] ~ normal( 0.8, 0.5);
  sigma_directed ~ exponential(1.5);

  mu_accel[1] ~ normal(-1.0, 0.7);
  mu_accel[2] ~ normal( 0.7, 0.7);
  mu_accel[3] ~ normal( 0.3, 0.8);
  sigma_accel ~ exponential(1.5);

  mu_temp[1] ~ normal(-0.7, 0.8);
  mu_temp[2] ~ normal( 0.5, 0.8);
  mu_temp[3] ~ normal( 0.0, 0.8);
  sigma_temp ~ exponential(1.5);

  kappa_angle[1] ~ exponential(1.2);
  kappa_angle[2] ~ exponential(1.2);
  kappa_angle[3] ~ exponential(0.8);

  for (j in 1:K) {
    to_vector(beta_tr[j]) ~ normal(0, 0.35);
  }

  for (t in 1:N) {
    for (k in 1:K) {
      log_emiss[t, k] =
        normal_lpdf(log_speed[t] | mu_speed[k], sigma_speed[k]) +
        normal_lpdf(progress[t]  | mu_progress[k], sigma_progress[k]) +
        normal_lpdf(directed[t]  | mu_directed[k], sigma_directed[k]);

      if (has_accel[t] == 1) {
        log_emiss[t, k] += normal_lpdf(accel[t] | mu_accel[k], sigma_accel[k]);
      }

      if (has_temp[t] == 1) {
        log_emiss[t, k] += normal_lpdf(temp[t] | mu_temp[k], sigma_temp[k]);
      }

      if (has_angle[t] == 1) {
        log_emiss[t, k] += von_mises_lpdf(angle[t] | 0, kappa_angle[k]);
      }
    }
  }

  for (t in 1:N) {
    if (track_start[t] == 1) {
      for (k in 1:K) {
        log_alpha[t][k] = log(init[k]) + log_emiss[t, k];
      }
    } else {
      for (k in 1:K) {
        vector[K] acc;
        for (j in 1:K) {
          vector[K] eta;
          vector[K] log_p;

          for (d in 1:(K - 1)) {
            real tmp_eta;
            tmp_eta = X_tr[t] * beta_tr[j][d]\';
            eta[d] = fmin(fmax(tmp_eta, -20), 20);
          }
          eta[K] = 0;

          log_p = log_softmax(eta);
          acc[j] = log_alpha[t - 1][j] + log_p[k];
        }
        log_alpha[t][k] = log_emiss[t, k] + log_sum_exp(acc);
      }
    }
  }

  target += log_sum_exp(log_alpha[N]);
}

generated quantities {
  matrix[N, K] state_prob;
  array[N] int state_viterbi;

  array[N] vector[K] log_alpha;
  array[N] vector[K] log_beta;
  matrix[N, K] log_emiss;

  array[N] vector[K] delta;
  array[N, K] int psi;

  for (t in 1:N) {
    for (k in 1:K) {
      log_emiss[t, k] =
        normal_lpdf(log_speed[t] | mu_speed[k], sigma_speed[k]) +
        normal_lpdf(progress[t]  | mu_progress[k], sigma_progress[k]) +
        normal_lpdf(directed[t]  | mu_directed[k], sigma_directed[k]);

      if (has_accel[t] == 1) {
        log_emiss[t, k] += normal_lpdf(accel[t] | mu_accel[k], sigma_accel[k]);
      }

      if (has_temp[t] == 1) {
        log_emiss[t, k] += normal_lpdf(temp[t] | mu_temp[k], sigma_temp[k]);
      }

      if (has_angle[t] == 1) {
        log_emiss[t, k] += von_mises_lpdf(angle[t] | 0, kappa_angle[k]);
      }
    }
  }

  for (t in 1:N) {
    if (track_start[t] == 1) {
      for (k in 1:K) {
        log_alpha[t][k] = log(init[k]) + log_emiss[t, k];
      }
    } else {
      for (k in 1:K) {
        vector[K] acc;
        for (j in 1:K) {
          vector[K] eta;
          vector[K] log_p;

          for (d in 1:(K - 1)) {
            real tmp_eta;
            tmp_eta = X_tr[t] * beta_tr[j][d]\';
            eta[d] = fmin(fmax(tmp_eta, -20), 20);
          }
          eta[K] = 0;

          log_p = log_softmax(eta);
          acc[j] = log_alpha[t - 1][j] + log_p[k];
        }
        log_alpha[t][k] = log_emiss[t, k] + log_sum_exp(acc);
      }
    }
  }

  for (k in 1:K) {
    log_beta[N][k] = 0;
  }

  if (N > 1) {
    for (tt in 1:(N - 1)) {
      int t = N - tt;

      if (track_start[t + 1] == 1) {
        for (k in 1:K) {
          log_beta[t][k] = 0;
        }
      } else {
        for (j in 1:K) {
          vector[K] acc;
          vector[K] eta;
          vector[K] log_p;

          for (d in 1:(K - 1)) {
            real tmp_eta;
            tmp_eta = X_tr[t + 1] * beta_tr[j][d]\';
            eta[d] = fmin(fmax(tmp_eta, -20), 20);
          }
          eta[K] = 0;

          log_p = log_softmax(eta);

          for (k in 1:K) {
            acc[k] = log_p[k] + log_emiss[t + 1, k] + log_beta[t + 1][k];
          }
          log_beta[t][j] = log_sum_exp(acc);
        }
      }
    }
  }

  for (t in 1:N) {
    vector[K] lp;
    real z;
    for (k in 1:K) {
      lp[k] = log_alpha[t][k] + log_beta[t][k];
    }
    z = log_sum_exp(lp);
    for (k in 1:K) {
      state_prob[t, k] = exp(lp[k] - z);
    }
  }

  for (t in 1:N) {
    if (track_start[t] == 1) {
      for (k in 1:K) {
        delta[t][k] = log(init[k]) + log_emiss[t, k];
        psi[t, k] = 1;
      }
    } else {
      for (k in 1:K) {
        real best_val;
        int best_state;
        best_val = negative_infinity();
        best_state = 1;

        for (j in 1:K) {
          vector[K] eta;
          vector[K] log_p;
          real cand;

          for (d in 1:(K - 1)) {
            real tmp_eta;
            tmp_eta = X_tr[t] * beta_tr[j][d]\';
            eta[d] = fmin(fmax(tmp_eta, -20), 20);
          }
          eta[K] = 0;

          log_p = log_softmax(eta);
          cand = delta[t - 1][j] + log_p[k];

          if (cand > best_val) {
            best_val = cand;
            best_state = j;
          }
        }

        delta[t][k] = best_val + log_emiss[t, k];
        psi[t, k] = best_state;
      }
    }
  }

  {
    int best_final_state;
    real best_final_val;

    best_final_state = 1;
    best_final_val = delta[N][1];

    for (k in 2:K) {
      if (delta[N][k] > best_final_val) {
        best_final_val = delta[N][k];
        best_final_state = k;
      }
    }
    state_viterbi[N] = best_final_state;

    if (N > 1) {
      for (tt in 1:(N - 1)) {
        int t = N - tt;
        if (track_start[t + 1] == 1) {
          int best_state_here;
          real best_val_here;

          best_state_here = 1;
          best_val_here = delta[t][1];
          for (k in 2:K) {
            if (delta[t][k] > best_val_here) {
              best_val_here = delta[t][k];
              best_state_here = k;
            }
          }
          state_viterbi[t] = best_state_here;
        } else {
          state_viterbi[t] = psi[t + 1, state_viterbi[t + 1]];
        }
      }
    }
  }
}
'

writeLines(stan_code, "nanofox_3state_hmm.stan")
mod <- cmdstan_model("nanofox_3state_hmm.stan")

fit_nanofox_3state_hmm <- function(
    x,
    chains = 4,
    iter_warmup = 1000,
    iter_sampling = 1000,
    parallel_chains = chains,
    seed = 123
) {
  df_hmm <- prepare_nanofox_hmm_data(x)
  stan_data <- build_stan_data_nanofox_3state(df_hmm)
  init_fun <- make_init_function_3state(df_hmm)

  fit <- mod$sample(
    data = stan_data,
    init = init_fun,
    chains = chains,
    parallel_chains = parallel_chains,
    iter_warmup = iter_warmup,
    iter_sampling = iter_sampling,
    seed = seed,
    adapt_delta = 0.95,
    max_treedepth = 12,
    refresh = 100
  )

  list(
    fit = fit,
    data = df_hmm,
    stan_data = stan_data
  )
}

extract_nanofox_states <- function(fit, df_hmm) {
  prob_draws <- fit$draws("state_prob")
  vit_draws  <- fit$draws("state_viterbi")

  prob_sum <- posterior::summarise_draws(prob_draws, "mean")
  vit_sum  <- posterior::summarise_draws(vit_draws, "mean")

  prob_df <- prob_sum %>%
    mutate(variable = as.character(variable)) %>%
    tidyr::extract(
      variable,
      into = c("t", "state"),
      regex = "state_prob\\[(\\d+),(\\d+)\\]",
      convert = TRUE
    ) %>%
    select(t, state, mean) %>%
    pivot_wider(
      names_from = state,
      values_from = mean,
      names_prefix = "p_state_"
    ) %>%
    arrange(t)

  vit_df <- vit_sum %>%
    mutate(variable = as.character(variable)) %>%
    tidyr::extract(
      variable,
      into = "t",
      regex = "state_viterbi\\[(\\d+)\\]",
      convert = TRUE
    ) %>%
    transmute(
      t = t,
      state_viterbi = round(mean)
    ) %>%
    arrange(t)

  df_hmm %>%
    mutate(t = row_number()) %>%
    left_join(prob_df, by = "t") %>%
    left_join(vit_df, by = "t") %>%
    mutate(
      state_label = case_when(
        state_viterbi == 1 ~ "inactive",
        state_viterbi == 2 ~ "active_local",
        state_viterbi == 3 ~ "migration",
        TRUE ~ NA_character_
      )
    )
}

# run on data ----

load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")
x <- bats_loc %>% filter(species == "Nyctalus leisleri", tag_type == "nanofox")
x <- x[2000:3000,]
ggplot(x, aes(lon, lat, col = individual_local_identifier))+geom_path()

res <- fit_nanofox_3state_hmm(
  x,
  chains = 4,
  iter_warmup = 500,
  iter_sampling = 500,
  parallel_chains = 4,
  seed = 123
)

seg <- extract_nanofox_states(res$fit, res$data)
bouts <- summarize_nanofox_bouts(seg)

plot_nanofox_states_time(seg)
plot_nanofox_track_states(seg)


param_sum <- res$fit$summary(c(
  "mu_speed", "mu_progress", "mu_accel", "mu_temp",
  "sigma_speed", "sigma_progress", "sigma_accel", "sigma_temp",
  "kappa_angle"
))

tidy_hmm_state_params(param_sum)
plot_hmm_state_params(param_sum)
plot_hmm_state_profiles(param_sum)
