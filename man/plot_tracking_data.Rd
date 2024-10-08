% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sigfox_to_move2.R
\name{plot_tracking_data}
\alias{plot_tracking_data}
\title{Plot Tracking Data}
\usage{
plot_tracking_data(m2, ml, legend, plot_lines = TRUE)
}
\arguments{
\item{m2}{move2 object with tracking data.}

\item{ml}{Lines representing movements of tags.}

\item{legend}{Logical; whether to include a legend.}
}
\value{
A ggplot object.
}
\description{
Creates a plot of tracking data using ggplot2 and rnaturalearth.
}
