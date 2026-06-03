library(terra)
library(dplyr)
library(geosphere)
library(ggplot2)

extract_elevation_segments <- function(raster, tag) {

  coords <- st_coordinates(tag) # Extract coordinates

  # Initialize cumulative distance
  cumulative_distance <- 0
  results <- list()

  # Iterate over each segment (pair of points)
  for (i in 1:(nrow(coords) - 1)) {
    # Define the start and end points of the segment
    segment_coords <- rbind(coords[i, ], coords[i + 1, ])

    # Compute segment distance in kilometers
    segment_distance <- geosphere::distHaversine(segment_coords[1, ], segment_coords[2, ]) / 1000 # Convert meters to km

    # Create a line for the segment
    segment_line <- terra::vect(x = segment_coords, type = "lines", crs = crs(raster))

    # Extract elevation values along the segment
    extracted <- terra::extract(raster, segment_line, along = TRUE)

    # Skip degenerate segments (zero-length line returns 0 rows from extract)
    if (nrow(extracted) == 0) {
      cumulative_distance <- cumulative_distance + segment_distance
      next
    }

    # Compute interpolated distances within the segment
    segment_relative_distances <- (1:nrow(extracted))/nrow(extracted) * segment_distance
    segment_distances_from_origin <- cumulative_distance + segment_relative_distances

    # Update cumulative distance
    cumulative_distance <- cumulative_distance + segment_distance

    # Add segment-specific data
    seq_num   <- if ("sequence_number" %in% names(tag)) as.numeric(tag$sequence_number[i]) else NA_real_
    ts_val    <- if ("timestamp"       %in% names(tag)) tag$timestamp[i]                   else NA
    segment_result <- data.frame(
      start_x              = as.numeric(segment_coords[1, 1]),
      start_y              = as.numeric(segment_coords[1, 2]),
      end_x                = as.numeric(segment_coords[2, 1]),
      end_y                = as.numeric(segment_coords[2, 2]),
      sequence_number      = seq_num,
      timestamp            = ts_val,
      distance_from_origin = segment_distances_from_origin,
      elevation            = extracted[[2]]
    )

    # Store the result
    results[[i]] <- segment_result
  }

  # Combine all segment results into one data frame
  do.call(rbind, results)
}

# # Example usage:
# # Assuming `raster` is your elevation raster and `tag` is the sf object with point data
# elevation_data <- extract_elevation_segments(raster = a, tag = tag)
#
# # Plot elevation profile
# ggplot(elevation_data, aes(x = distance_from_origin, y = elevation)) +
#   geom_line() +
#   geom_point() +
#   labs(
#     x = "Distance from Origin (km)",
#     y = "Elevation (m)",
#     title = "Elevation Profile"
#   ) +
#   theme_minimal()


