% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sigfox_download.R
\name{retry_download}
\alias{retry_download}
\title{Retry downloading HTML data}
\usage{
retry_download(url, max_attempts = 5)
}
\arguments{
\item{url}{The URL to download from.}

\item{max_attempts}{Maximum number of attempts to download.}
}
\value{
Downloaded HTML tables or NULL if unsuccessful.
}
\description{
Retry downloading HTML data
}
