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
