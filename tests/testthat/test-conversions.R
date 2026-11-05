test_that("pressure_to_altitude_m gives 0 m at reference pressure and rises as pressure falls", {
  expect_equal(pressure_to_altitude_m(1013.25), 0, tolerance = 1e-6)
  alt <- pressure_to_altitude_m(c(1000, 900, 800))
  expect_true(all(diff(alt) > 0))
  # ISA: ~988 m at 900 hPa
  expect_equal(pressure_to_altitude_m(900), 988, tolerance = 0.01)
})

test_that("altitude_to_pressure_hPa inverts pressure_to_altitude_m", {
  # Exponents 0.1903 and 5.2558 are rounded ISA constants, so not an exact inverse.
  p <- c(1000, 850, 700)
  expect_equal(altitude_to_pressure_hPa(pressure_to_altitude_m(p)), p, tolerance = 1e-4)
})

test_that("conversions accept a per-row reference pressure aligned with NA rows", {
  expect_equal(
    pressure_to_altitude_m(c(NA, 1000), p0_hpa = c(1013.25, 1000)),
    c(NA, 0)
  )
})

test_that("calculate_bearing returns compass bearings", {
  expect_equal(calculate_bearing(0, 0, 0, 1), 0)
  expect_equal(calculate_bearing(0, 0, 1, 0), 90)
  expect_equal(calculate_bearing(0, 0, -1, 0), 270)
})

test_that("mt_daily_night_date keys a night spanning midnight to its evening date", {
  # Fixes at 23:00 and 01:00 (sun below horizon in central Europe in April)
  # belong to the same night, labelled by the evening date.
  x <- make_track(n = 3, start = as.POSIXct("2026-04-01 23:00:00", tz = "UTC"))
  x <- x[c(1, 3), ]
  nd <- mt_daily_night_date(x)
  expect_equal(nd, as.Date(c("2026-04-01", "2026-04-01")))
})
