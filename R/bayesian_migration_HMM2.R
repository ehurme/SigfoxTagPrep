# Bayesian HMM segmentation
# 2026-03-09

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(sf)
  library(move2)
  library(cmdstanr)
  library(posterior)
  library(ggplot2)
  library(stringr)
})

add_seasonal_directionality <- function(df) {
  df %>%
    mutate(
      heading = atan2(dy, dx),  # radians, east = 0

      month_mid = lubridate::month(t_mid),
      season = case_when(
        month_mid %in% c(3, 4, 5, 6)  ~ "spring",
        month_mid %in% c(8, 9, 10, 11) ~ "fall",
        TRUE ~ "other"
      ),

      # target migration direction in radians
      target_heading = case_when(
        season == "spring" ~ pi / 4,        # 45 deg = NE
        season == "fall"   ~ 5 * pi / 4,    # 225 deg = SW
        TRUE ~ NA_real_
      ),

      dir_align = ifelse(
        is.na(target_heading),
        0,
        cos(heading - target_heading)
      )
    ) %>%
    mutate(
      z_dir_align = safe_scale(dir_align)
    )
}

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

prepare_nanofox_hmm2_data <- function(
    x,
    crs_projected = 3035,
    expected_dt_hours = 3,
    daily_migration_km = 40
) {
  stopifnot(inherits(x, "move2"))

  cols <- guess_nanofox_columns(x)

  if (is.na(cols$id_col)) stop("Could not detect ID column.")
  if (is.na(cols$time_col)) stop("Could not detect timestamp column.")

  # coordinates in projected CRS for movement
  x_proj <- sf::st_transform(x, crs_projected)
  xy_proj <- sf::st_coordinates(x_proj)

  # lon/lat for plotting if useful
  x_ll <- sf::st_transform(x, 4326)
  xy_ll <- sf::st_coordinates(x_ll)

  df <- x_proj %>%
    sf::st_drop_geometry() %>%
    mutate(
      id = .data[[cols$id_col]],
      timestamp = as.POSIXct(.data[[cols$time_col]], tz = "UTC"),
      x = xy_proj[, 1],
      y = xy_proj[, 2],
      longitude = xy_ll[, 1],
      latitude  = xy_ll[, 2]
    ) %>%
    arrange(id, timestamp)

  # interval-level movement variables
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

      t_mid = timestamp_prev + (timestamp - timestamp_prev) / 2,
      hour_mid = lubridate::hour(t_mid),
      is_night = ifelse(hour_mid >= 19 | hour_mid <= 6, 1L, 0L)
    ) %>%
    ungroup()

  # displacement from track origin
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
      progress_m_h = delta_disp_m / dt_hours
    ) %>%
    ungroup()

  # prior migration heuristic from earlier work
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
      is.finite(progress_m_h)
    ) %>%
    group_by(id) %>%
    arrange(timestamp, .by_group = TRUE) %>%
    mutate(
      track_start = ifelse(row_number() == 1, 1L, 0L)
    ) %>%
    ungroup() %>%
    mutate(
      z_log_speed = safe_scale(log_speed),
      z_progress  = safe_scale(progress_m_h),
      z_log_dt    = safe_scale(log(dt_hours / expected_dt_hours))
    )

  # final safety pass
  num_cols <- c("z_log_speed", "z_progress", "z_log_dt", "x", "y", "longitude", "latitude")
  for (nm in num_cols) {
    if (nm %in% names(df)) {
      df[[nm]][!is.finite(df[[nm]])] <- 0
    }
  }

  df
}

build_transition_covariates_hmm2 <- function(df) {
  X <- cbind(
    intercept = 1,
    night = df$is_night
  )

  X <- as.matrix(X)
  X[!is.finite(X)] <- 0
  X
}

make_initial_state_guesses_hmm2 <- function(df, daily_migration_km = 40) {
  migration_progress_threshold_m_h <- daily_migration_km * 1000 / 24

  ifelse(
    df$progress_m_h >= migration_progress_threshold_m_h |
      df$prior_migration_flag == 1,
    2L, 1L
  )
}

build_stan_data_nanofox_hmm2 <- function(df) {
  X_tr <- build_transition_covariates_hmm2(df)

  stan_data <- list(
    N = nrow(df),
    K = 2L,
    P = ncol(X_tr),
    log_speed = as.numeric(df$z_log_speed),
    progress  = as.numeric(df$z_progress),
    track_start = as.integer(df$track_start),
    X_tr = X_tr
  )

  stan_data$log_speed[!is.finite(stan_data$log_speed)] <- 0
  stan_data$progress[!is.finite(stan_data$progress)] <- 0
  stan_data$X_tr[!is.finite(stan_data$X_tr)] <- 0

  stan_data
}

stan_code_hmm2 <- '
data {
  int<lower=1> N;
  int<lower=1> K;
  int<lower=1> P;

  vector[N] log_speed;
  vector[N] progress;

  array[N] int<lower=0, upper=1> track_start;
  matrix[N, P] X_tr;
}

parameters {
  simplex[K] init;
  array[K] vector[P] beta_tr_raw;

  ordered[K] mu_speed;
  vector<lower=0>[K] sigma_speed;

  ordered[K] mu_progress;
  vector<lower=0>[K] sigma_progress;
}

model {
  array[N] vector[K] log_alpha;
  matrix[N, K] log_emiss;

  // informative priors:
  // state 1 = non-migration, state 2 = migration
  mu_speed[1] ~ normal(-0.5, 0.7);
  mu_speed[2] ~ normal( 1.2, 0.7);
  sigma_speed ~ exponential(1.5);

  mu_progress[1] ~ normal(-0.2, 0.6);
  mu_progress[2] ~ normal( 1.2, 0.7);
  sigma_progress ~ exponential(1.5);

  for (j in 1:K) {
    beta_tr_raw[j] ~ normal(0, 0.4);
  }

  // emissions
  for (t in 1:N) {
    for (k in 1:K) {
      log_emiss[t, k] =
        normal_lpdf(log_speed[t] | mu_speed[k], sigma_speed[k]) +
        normal_lpdf(progress[t]  | mu_progress[k], sigma_progress[k]);
    }
  }

  // forward algorithm
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

          eta[1] = fmin(fmax(dot_product(X_tr[t], beta_tr_raw[j]), -20), 20);
          eta[2] = 0;

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

  // emissions
  for (t in 1:N) {
    for (k in 1:K) {
      log_emiss[t, k] =
        normal_lpdf(log_speed[t] | mu_speed[k], sigma_speed[k]) +
        normal_lpdf(progress[t]  | mu_progress[k], sigma_progress[k]);
    }
  }

  // forward
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

          eta[1] = fmin(fmax(dot_product(X_tr[t], beta_tr_raw[j]), -20), 20);
          eta[2] = 0;

          log_p = log_softmax(eta);
          acc[j] = log_alpha[t - 1][j] + log_p[k];
        }
        log_alpha[t][k] = log_emiss[t, k] + log_sum_exp(acc);
      }
    }
  }

  // backward
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

          eta[1] = fmin(fmax(dot_product(X_tr[t + 1], beta_tr_raw[j]), -20), 20);
          eta[2] = 0;

          log_p = log_softmax(eta);

          for (k in 1:K) {
            acc[k] = log_p[k] + log_emiss[t + 1, k] + log_beta[t + 1][k];
          }
          log_beta[t][j] = log_sum_exp(acc);
        }
      }
    }
  }

  // smoothed state probabilities
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

  // Viterbi
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

          eta[1] = fmin(fmax(dot_product(X_tr[t], beta_tr_raw[j]), -20), 20);
          eta[2] = 0;

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

writeLines(stan_code_hmm2, "nanofox_2state_migration_hmm.stan")
mod_hmm2 <- cmdstan_model("nanofox_2state_migration_hmm.stan")

make_init_function_hmm2 <- function(df) {
  state_guess <- make_initial_state_guesses_hmm2(df)

  idx1 <- state_guess == 1
  idx2 <- state_guess == 2

  get_mean <- function(x, idx, fallback = 0) {
    val <- mean(x[idx], na.rm = TRUE)
    if (!is.finite(val)) val <- fallback
    val
  }

  function(chain_id = 1) {
    list(
      init = c(0.8, 0.2),
      mu_speed = c(
        get_mean(df$z_log_speed, idx1, -0.4),
        get_mean(df$z_log_speed, idx2,  1.2)
      ),
      sigma_speed = c(0.7, 0.7),
      mu_progress = c(
        get_mean(df$z_progress, idx1, -0.2),
        get_mean(df$z_progress, idx2,  1.2)
      ),
      sigma_progress = c(0.7, 0.7),
      beta_tr_raw = list(
        c(0, 0),
        c(0, 0)
      )
    )
  }
}

fit_nanofox_2state_hmm <- function(
    x,
    chains = 4,
    iter_warmup = 500,
    iter_sampling = 500,
    parallel_chains = chains,
    seed = 123
) {
  df_hmm <- prepare_nanofox_hmm2_data(x)
  stan_data <- build_stan_data_nanofox_hmm2(df_hmm)
  init_fun <- make_init_function_hmm2(df_hmm)

  fit <- mod_hmm2$sample(
    data = stan_data,
    init = init_fun,
    chains = chains,
    parallel_chains = parallel_chains,
    iter_warmup = iter_warmup,
    iter_sampling = iter_sampling,
    seed = seed,
    adapt_delta = 0.90,
    max_treedepth = 11,
    refresh = 100
  )

  list(
    fit = fit,
    data = df_hmm,
    stan_data = stan_data
  )
}

extract_nanofox_states_hmm2 <- function(fit, df_hmm) {
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
        state_viterbi == 1 ~ "non_migration",
        state_viterbi == 2 ~ "migration",
        TRUE ~ NA_character_
      )
    )
}

summarize_nanofox_bouts_hmm2 <- function(seg) {
  seg %>%
    arrange(id, timestamp) %>%
    group_by(id) %>%
    mutate(
      new_bout = ifelse(
        row_number() == 1 | state_label != lag(state_label),
        1L, 0L
      ),
      bout_id = cumsum(new_bout)
    ) %>%
    group_by(id, bout_id, state_label) %>%
    summarise(
      start_time = min(timestamp_prev, na.rm = TRUE),
      end_time = max(timestamp, na.rm = TRUE),
      n_intervals = n(),
      total_hours = sum(dt_hours, na.rm = TRUE),
      total_distance_km = sum(step_m, na.rm = TRUE) / 1000,
      mean_speed_m_h = mean(speed_m_h, na.rm = TRUE),
      mean_progress_m_h = mean(progress_m_h, na.rm = TRUE),
      mean_p_non_mig = mean(p_state_1, na.rm = TRUE),
      mean_p_mig = mean(p_state_2, na.rm = TRUE),
      .groups = "drop"
    )
}

plot_nanofox_states_time_hmm2 <- function(seg) {
  ggplot(seg, aes(timestamp, p_state_2, color = id)) +
    geom_line(alpha = 0.8) +
    geom_hline(yintercept = 0.5, linetype = 2) +
    theme_bw() +
    labs(
      x = "Time",
      y = "Posterior probability of migration",
      color = "ID"
    )
}

plot_nanofox_track_states_hmm2 <- function(seg) {
  ggplot(seg, aes(x, y, color = state_label, group = id)) +
    geom_path(alpha = 0.7) +
    geom_point(size = 0.6) +
    coord_equal() +
    theme_bw()
}

plot_hmm2_params <- function(fit) {
  fit$summary(c("mu_speed", "mu_progress", "sigma_speed", "sigma_progress")) %>%
    mutate(variable = as.character(variable)) %>%
    mutate(
      family = ifelse(str_detect(variable, "^mu_"), "mu", "sigma"),
      measure = str_match(variable, "^(mu|sigma)_([A-Za-z0-9\\.]+)\\[(\\d+)\\]$")[, 3],
      state = as.integer(str_match(variable, ".*\\[(\\d+)\\]$")[, 2])
    ) %>%
    ggplot(aes(x = factor(state), y = mean, ymin = q5, ymax = q95)) +
    geom_hline(yintercept = 0, linetype = 2, linewidth = 0.3) +
    geom_pointrange() +
    facet_grid(family ~ measure, scales = "free_y") +
    theme_bw() +
    labs(
      x = "State",
      y = "Posterior mean and 90% interval",
      title = "2-state migration HMM parameter estimates"
    )
}


load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")

x <- bats_loc %>%
  filter(species == "Nyctalus leisleri")

# optional subset for testing
# x <- x[1:3000, ]

res <- fit_nanofox_2state_hmm(
  x,
  chains = 4,
  iter_warmup = 500,
  iter_sampling = 500,
  parallel_chains = 4,
  seed = 123
)

seg <- extract_nanofox_states_hmm2(res$fit, res$data)
bouts <- summarize_nanofox_bouts_hmm2(seg)

plot_nanofox_states_time_hmm2(seg)
plot_nanofox_track_states_hmm2(seg)
plot_hmm2_params(res$fit)

res$fit$summary(c("mu_speed", "mu_progress", "sigma_speed", "sigma_progress"))
res$fit$diagnostic_summary()

