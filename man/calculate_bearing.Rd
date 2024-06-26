% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tracking_data_processing.R
\name{calculate_bearing}
\alias{calculate_bearing}
\title{Calculate Distance and Bearing Between Consecutive Points}
\usage{
calculate_bearing(lon1, lat1, lon2, lat2)
}
\arguments{
\item{lon1}{Longitude of the first point in degrees.}

\item{lat1}{Latitude of the first point in degrees.}

\item{lon2}{Longitude of the second point in degrees.}

\item{lat2}{Latitude of the second point in degrees.}
}
\value{
Bearing in degrees from the North.
}
\description{
Auxiliary functions to calculate bearing and invoke bearing calculation for sequential points
in tracking data. It's a helper for other analysis functions.
Calculate Bearing Between Two Geographic Points
}
\details{
This function calculates the bearing from one point to another using their longitude and latitude.
}
