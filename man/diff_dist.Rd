% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tracking_data_processing.R
\name{diff_dist}
\alias{diff_dist}
\title{Calculate Distance Between Sequential Points in Tracking Data}
\usage{
diff_dist(data)
}
\arguments{
\item{data}{A data frame with tracking data that must include \code{tag_id}, \code{latitude}, and \code{longitude} columns,
and a \code{timestamp} to ensure the points are in sequential order.}
}
\value{
The input data frame with an additional column \code{distance} and \code{distance_from_start} indicating the distance in meters
from each point to the next in the sequence and from the origin.
}
\description{
This function iterates through tracking data for each unique tag ID, calculating the distance
between sequential points using their geographic coordinates.
}
