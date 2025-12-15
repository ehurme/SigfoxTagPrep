mt_distance2 <- function (x, units)
{
  assertthat::assert_that(mt_is_time_ordered_non_empty_points(x))
  empty <- st_as_sfc("POINT(EMPTY)", crs = st_crs(x))
  d <- st_distance(c(st_geometry(x), empty), c(empty, st_geometry(x)),
                   by_element = TRUE)#[-1L] # 2335
  ids <- mt_track_id(x)
  d[diff(as.numeric(if (is.character(ids)) {
    factor(ids)
  } else {
    ids
  })) != 0L] <- NA
  return(mt_change_units(d, units))
}


# where to put speed, distance, azimuth?