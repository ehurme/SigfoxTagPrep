# Core data handling
library(dplyr)
library(tidyr)
library(lubridate)
library(sf)

# Movement calculations
library(move2)

# Bayesian modelling
library(cmdstanr)
library(posterior)

# Plotting
library(ggplot2)

prepare_hmm_data <- function(x, crs_projected = 3035) {
  stopifnot(inherits(x, "move2"))

  # Reproject to a CRS in meters so distances make sense
  x_proj <- sf::st_transform(x, crs_projected)

  coords <- sf::st_coordinates(x_proj)

  df <- x_proj %>%
    sf::st_drop_geometry() %>%
    mutate(
      x = coords[, 1],
      y = coords[, 2],
      timestamp = as.POSIXct(timestamp, tz = "UTC")
    ) %>%
    arrange(individual_local_identifier, timestamp) %>%
    group_by(individual_local_identifier) %>%
    mutate(
      dx = x - lag(x),
      dy = y - lag(y),
      step = sqrt(dx^2 + dy^2),                       # meters
      dt = as.numeric(difftime(timestamp, lag(timestamp), units = "secs")),
      heading = atan2(dy, dx),
      turn_angle = heading - lag(heading)
    ) %>%
    ungroup()

  # Wrap turning angles to [-pi, pi]
  df <- df %>%
    mutate(
      turn_angle = atan2(sin(turn_angle), cos(turn_angle))
    )

  # Remove rows where movement metrics are undefined
  # Turning angle needs at least 3 points, so the first two rows per track drop out
  df <- df %>%
    group_by(individual_local_identifier) %>%
    mutate(row_id = row_number()) %>%
    ungroup() %>%
    filter(!is.na(step), !is.na(turn_angle), !is.na(dt), dt > 0)

  return(df)
}

make_model_data <- function(df) {
  df2 <- df %>%
    mutate(
      log_step = log(step + 1),   # +1 avoids log(0)
      id = as.integer(as.factor(individual_local_identifier))
    )

  list(
    df = df2,
    stan_data = list(
      N = nrow(df2),
      K = 2,  # two hidden states
      log_step = df2$log_step,
      angle = df2$turn_angle
    )
  )
}

stan_code <- '
data {
  int<lower=1> N;                  // number of observations
  int<lower=1> K;                  // number of states (2)
  vector[N] log_step;              // observed log step lengths
  vector[N] angle;                 // observed turning angles in radians
}

parameters {
  simplex[K] init;                 // initial state probabilities
  array[K] simplex[K] trans;       // transition matrix rows

  ordered[K] mu_step;              // state-specific mean log-step
  vector<lower=0>[K] sigma_step;   // state-specific sd of log-step
  vector<lower=0>[K] kappa;        // state-specific turning-angle concentration
}

model {
  matrix[N, K] log_omega;          // log emission probabilities
  array[N] vector[K] log_alpha;    // forward probabilities in log space

  // Priors
  mu_step ~ normal(0, 3);
  sigma_step ~ exponential(1);
  kappa ~ exponential(1);

  // Emission probabilities
  for (t in 1:N) {
    for (k in 1:K) {
      log_omega[t, k] =
        normal_lpdf(log_step[t] | mu_step[k], sigma_step[k]) +
        von_mises_lpdf(angle[t] | 0, kappa[k]);
    }
  }

  // Forward algorithm initialization
  for (k in 1:K) {
    log_alpha[1][k] = log(init[k]) + log_omega[1, k];
  }

  // Forward recursion
  for (t in 2:N) {
    for (k in 1:K) {
      vector[K] acc;
      for (j in 1:K) {
        acc[j] = log_alpha[t - 1][j] + log(trans[j][k]);
      }
      log_alpha[t][k] = log_omega[t, k] + log_sum_exp(acc);
    }
  }

  // Marginal likelihood
  target += log_sum_exp(log_alpha[N]);
}

generated quantities {
  matrix[N, K] state_prob;
  real log_lik;

  {
    matrix[N, K] log_omega;
    array[N] vector[K] log_alpha;
    array[N] vector[K] log_beta;
    vector[K] tmp;
    vector[K] lp;

    // Emission probabilities
    for (t in 1:N) {
      for (k in 1:K) {
        log_omega[t, k] =
          normal_lpdf(log_step[t] | mu_step[k], sigma_step[k]) +
          von_mises_lpdf(angle[t] | 0, kappa[k]);
      }
    }

    // Forward pass
    for (k in 1:K) {
      log_alpha[1][k] = log(init[k]) + log_omega[1, k];
    }

    for (t in 2:N) {
      for (k in 1:K) {
        for (j in 1:K) {
          tmp[j] = log_alpha[t - 1][j] + log(trans[j][k]);
        }
        log_alpha[t][k] = log_omega[t, k] + log_sum_exp(tmp);
      }
    }

    log_lik = log_sum_exp(log_alpha[N]);

    // Backward pass
    for (k in 1:K) {
      log_beta[N][k] = 0;
    }

    if (N > 1) {
      for (t_rev in 1:(N - 1)) {
        int t = N - t_rev;
        for (j in 1:K) {
          for (k in 1:K) {
            tmp[k] = log(trans[j][k]) + log_omega[t + 1, k] + log_beta[t + 1][k];
          }
          log_beta[t][j] = log_sum_exp(tmp);
        }
      }
    }

    // Posterior state probabilities
    for (t in 1:N) {
      for (k in 1:K) {
        lp[k] = log_alpha[t][k] + log_beta[t][k] - log_lik;
      }
      for (k in 1:K) {
        state_prob[t, k] = exp(lp[k]);
      }
    }
  }
}
'
writeLines(stan_code, "migration_hmm.stan")

mod <- cmdstan_model("migration_hmm.stan")


# fit_migration_hmm <- function(stan_data, chains = 4, iter_warmup = 1000, iter_sampling = 1000, seed = 123) {
#   mod <- cmdstan_model("migration_hmm.stan")
#
#   fit <- mod$sample(
#     data = stan_data,
#     chains = chains,
#     parallel_chains = chains,
#     iter_warmup = iter_warmup,
#     iter_sampling = iter_sampling,
#     seed = seed,
#     refresh = 200
#   )
#
#   return(fit)
# }

fit_migration_hmm <- function(mod, stan_data,
                        chains = 4,
                        iter_warmup = 1000,
                        iter_sampling = 1000,
                        seed = 123,
                        use_threads = FALSE,
                        n_cores = parallel::detectCores(logical = FALSE)) {

  if (!use_threads) {
    fit <- mod$sample(
      data = stan_data,
      chains = chains,
      parallel_chains = min(chains, n_cores),
      iter_warmup = iter_warmup,
      iter_sampling = iter_sampling,
      seed = seed,
      refresh = 100
    )
  } else {
    # simple default split
    parallel_chains <- min(chains, max(1, floor(n_cores / 2)))
    threads_per_chain <- max(1, floor(n_cores / parallel_chains))

    fit <- mod$sample(
      data = stan_data,
      chains = chains,
      parallel_chains = parallel_chains,
      threads_per_chain = threads_per_chain,
      iter_warmup = iter_warmup,
      iter_sampling = iter_sampling,
      seed = seed,
      refresh = 100
    )
  }

  fit
}

load("../../../Dropbox/MPI/Noctule/Data/rdata/move_icarus_bats.robj")
x <- bats_loc %>% filter(species == "Nyctalus leisleri", tag_type == "nanofox")

df_mov <- prepare_hmm_data(x)
model_input <- make_model_data(df_mov)

# fit <- fit_migration_hmm(model_input$stan_data)
mod <- cmdstan_model("migration_hmm.stan")
fit <- fit_migration_hmm(mod, model_input$stan_data, use_threads = FALSE)

fit$summary(c("mu_step", "sigma_step", "kappa", "trans"))

# Chain 7 finished in 2735.0 seconds.
# Chain 6 Iteration:  200 / 2000 [ 10%]  (Warmup)
# Chain 3 Iteration:  200 / 2000 [ 10%]  (Warmup)
