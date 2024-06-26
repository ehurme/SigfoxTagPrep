% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tracking_data_processing.R
\name{calculate_bearing_for_sequential_points}
\alias{calculate_bearing_for_sequential_points}
\title{Calculate Bearing for Sequential Points in Tracking Data}
\usage{
calculate_bearing_for_sequential_points(data)
}
\arguments{
\item{data}{A data frame with tracking data that must include \code{tag_id}, \code{latitude}, and \code{longitude} columns,
and a \code{timestamp} to ensure the points are in sequential order.}
}
\value{
The input data frame with an additional column \code{bearing} indicating the bearing from each point
to the next in the sequence.
}
\description{
This function iterates through tracking data for each unique tag ID, calculating the bearing
between sequential points using their geographic coordinates.
}
