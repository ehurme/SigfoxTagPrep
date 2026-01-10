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

    # Compute interpolated distances within the segment
    segment_relative_distances <- (1:nrow(extracted))/nrow(extracted) * segment_distance
    segment_distances_from_origin <- cumulative_distance + segment_relative_distances

    # Update cumulative distance
    cumulative_distance <- cumulative_distance + segment_distance

    # Add segment-specific data
    segment_result <- data.frame(
      start_x = segment_coords[1, 1] %>% as.numeric(),
      start_y = segment_coords[1, 2] %>% as.numeric(),
      end_x = segment_coords[2, 1] %>% as.numeric(),
      end_y = segment_coords[2, 2] %>% as.numeric(),
      #burst_id = tag$burst_id[i],
      sequence_number = tag$sequence_number[i] %>% as.numeric(),
      timestamp = tag$timestamp[i],
      distance_from_origin = segment_distances_from_origin, # Interpolated distance from origin
      elevation = extracted[[2]]  # Elevation values
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


