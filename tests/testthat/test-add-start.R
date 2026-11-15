test_that(".add_start uses deploy_on_location when present", {
  x <- make_track()
  dep_ts <- min(x$timestamp) - 3600
  x <- set_deploy_info(
    x, dep_ts,
    deploy_loc = sf::st_sfc(sf::st_point(c(10, 49)), crs = 4326),
    capture_loc = sf::st_sfc(sf::st_point(c(9, 48)), crs = 4326)
  )
  out <- .add_start(x)
  expect_equal(nrow(out), nrow(x) + 1)
  st <- out[which(out$comments == "start"), ]
  expect_equal(st$timestamp, dep_ts)
  expect_equal(unname(sf::st_coordinates(st)[1, ]), c(10, 49))
})

test_that(".add_start falls back to capture_location when deploy_on_location is empty", {
  x <- make_track()
  x <- set_deploy_info(
    x, min(x$timestamp) - 3600,
    deploy_loc = sf::st_sfc(sf::st_point(), crs = 4326),
    capture_loc = sf::st_sfc(sf::st_point(c(9, 48)), crs = 4326)
  )
  st <- .add_start(x)
  st <- st[which(st$comments == "start"), ]
  expect_equal(unname(sf::st_coordinates(st)[1, ]), c(9, 48))
  expect_equal(st$lon, 9)
})

test_that(".add_start falls back to the first fix when both locations are empty", {
  x <- make_track()
  empty <- sf::st_sfc(sf::st_point(), crs = 4326)
  x <- set_deploy_info(x, min(x$timestamp) - 3600, empty, empty)
  st <- .add_start(x)
  st <- st[which(st$comments == "start"), ]
  expect_equal(unname(sf::st_coordinates(st)[1, ]), c(8, 47))
})

test_that("deployed bats without location fixes get a start row at deploy-on", {
  t0 <- as.POSIXct("2026-04-01 22:00:00", tz = "UTC")
  # d1 has fixes; d3 has only sensor rows (sensor_type_id 2); d2 has no events
  x <- move2::mt_stack(make_track(id = "d1"), make_track(n = 2, id = "d3"))
  x$sensor_type_id <- ifelse(x$deployment_id == "d3", 2, 1)
  x$individual_local_identifier <- sub("d", "bat", x$deployment_id)
  deps <- tibble::tibble(
    deployment_id = c("d1", "d2", "d3"),
    individual_local_identifier = c("bat1", "bat2", "bat3"),
    deploy_on_timestamp = t0 - c(3600, 7200, 1800),
    deploy_on_location = sf::st_sfc(
      sf::st_point(c(10, 49)), sf::st_point(c(11, 50)), sf::st_point(), crs = 4326
    ),
    capture_location = sf::st_sfc(
      sf::st_point(), sf::st_point(), sf::st_point(c(12, 51)), crs = 4326
    )
  )
  out <- .add_undetected_deployments(x, deps, location_sensor_id = 1)

  expect_equal(nrow(out), nrow(x) + 2) # d1 untouched
  expect_setequal(move2::mt_track_data(out)$deployment_id, c("d1", "d2", "d3"))
  new <- out[out$sensor_type_id == 1 & out$deployment_id != "d1", ]
  expect_equal(new$deployment_id, c("d2", "d3"))
  expect_equal(new$timestamp, t0 - c(7200, 1800))
  expect_equal(unname(sf::st_coordinates(new)), rbind(c(11, 50), c(12, 51)))
  expect_equal(new$individual_local_identifier, c("bat2", "bat3"))

  # .add_start marks the injected row as the start row instead of duplicating it
  td <- move2::mt_track_data(out)
  td <- dplyr::left_join(td["deployment_id"], deps, by = "deployment_id")
  out <- move2::mt_set_track_data(out, td)
  out$sensor_type <- ifelse(out$sensor_type_id == 1, "location", "VeDBA")
  st <- .add_start(out)
  expect_equal(nrow(st), nrow(out) + 1) # only d1 gets a synthetic row
  expect_setequal(st$deployment_id[which(st$comments == "start")], c("d1", "d2", "d3"))
})
