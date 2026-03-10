
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

