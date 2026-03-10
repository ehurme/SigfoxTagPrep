
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

