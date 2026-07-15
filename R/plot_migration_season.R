# Raincloud plot: <var> by season x migration-night, with within-season
# (migration vs non-migration) and between-season (migration vs migration)
# Wilcoxon comparisons.
# colors: named vector, one entry per "<season>.<mig_label>" combo, e.g.
#   "Spring.Non-migration", "Spring.Migration", "Fall.Non-migration", "Fall.Migration"
library(move2)
library(tidyverse)
library(ggrain)
library(ggpubr)

plot_migration_season <- function(
  data,
  var,
  ylab_lab = var,
  transform = identity,
  season_col = "season",
  mig_col = "migration_night",
  mig_labels = c("Non-migration", "Migration"),
  colors = c(
    "Spring.Non-migration" = "#A6CEE3",
    "Spring.Migration"     = "#1F78B4",
    "Fall.Non-migration"   = "#FDBE85",
    "Fall.Migration"       = "#E6550D"
  )
) {
  data$.y <- transform(data[[var]])
  data <- data[!is.na(data$.y), ]
  data$.mig_lab <- factor(data[[mig_col]], levels = c(0, 1), labels = mig_labels)

  seasons <- levels(droplevels(factor(data[[season_col]])))
  grp_levels <- as.vector(t(outer(seasons, mig_labels, paste, sep = ".")))
  data$.grp <- factor(
    paste(data[[season_col]], data$.mig_lab, sep = "."),
    levels = grp_levels
  )

  mig_level <- mig_labels[2]

  within_season <- lapply(seasons, function(s) {
    c(paste(s, mig_labels[1], sep = "."), paste(s, mig_labels[2], sep = "."))
  })
  between_season <- if (length(seasons) == 2) {
    list(c(paste(seasons[1], mig_level, sep = "."), paste(seasons[2], mig_level, sep = ".")))
  } else {
    list()
  }
  comparisons <- c(within_season, between_season)

  ggplot(
    data,
    aes(.grp, .y, fill = .grp, col = .grp, group = .grp)
  ) +
    geom_rain(
      alpha = 0.4,
      rain.side = "l",
      boxplot.args = list(color = "black", outlier.shape = NA),
      boxplot.args.pos = list(
        position = ggpp::position_dodgenudge(x = .1, width = 0.1),
        width = 0.1
      )
    ) +
    ggpubr::stat_compare_means(
      comparisons = comparisons,
      method = "wilcox.test",
      label = "p.signif",
      hide.ns = FALSE,
      tip.length = 0.01
    ) +
    scale_x_discrete(labels = function(x) sub("\\.", "\n", x)) +
    xlab(NULL) +
    ylab(ylab_lab) +
    theme_classic() +
    theme(legend.position = "none") +
    scale_fill_manual(values = colors) +
    scale_color_manual(values = colors)
}