tiny_df <- function(tag_type = NA_character_) {
  data.frame(model = NA_character_, tag_type = tag_type)
}

test_that(".set_tag_type accepts lowercase overrides and returns canonical case", {
  x <- make_track()
  out <- .set_tag_type(x, study_id_current = 1, tag_type = "nanofox")
  expect_true(all(out$tag_type == "Nanofox"))
})

test_that(".set_tag_type applies a named override only to its own study", {
  x <- make_track()
  expect_error(
    .set_tag_type(x, study_id_current = 2, tag_type = list("1" = "tinyfox")),
    "could not be inferred"
  )
  out <- .set_tag_type(x, study_id_current = 1, tag_type = list("1" = "tinyfox"))
  expect_true(all(out$tag_type == "Tinyfox"))
})

test_that(".set_tag_type rejects unknown overrides", {
  expect_error(.set_tag_type(make_track(), 1, tag_type = "gps"), "Invalid tag_type")
})

test_that(".correct_vedba_scaling multiplies per-sample NanoFox values by 28", {
  d <- data.frame(vedba_sum = c(0.5, 1, 2))
  out <- .correct_vedba_scaling(d)
  expect_equal(out$vedba_sum, c(0.5, 1, 2) * 28)
  expect_equal(out$vedba_sum_orig, c(0.5, 1, 2))
  expect_true(all(out$.vedba_scaling_note == "corrected_per_sample_to_per_burst"))
})

test_that(".correct_vedba_scaling leaves per-burst values alone", {
  d <- data.frame(vedba_sum = c(10, 20, 30))
  out <- .correct_vedba_scaling(d)
  expect_equal(out$vedba_sum, c(10, 20, 30))
  expect_true(all(out$.vedba_scaling_note == "already_per_burst"))
})

test_that(".correct_tinyfox_vedba_scaling applies force_factor without stationary data", {
  d <- data.frame(
    tag_type = "Tinyfox",
    tag_firmware = c("V13P", "V13P", "V13"),
    tinyfox_total_vedba = c(100, 200, 50),
    tinyfox_vedba_rate = NA_real_
  )
  out <- .correct_tinyfox_vedba_scaling(d, force_factor = 10)
  expect_equal(out$tinyfox_total_vedba, c(10, 20, 50))
  expect_equal(out$tinyfox_total_vedba_orig, c(100, 200, 50))
})

test_that(".count_pipe_stations counts stations and keeps NA", {
  s <- c("148A3, -127, 1 | 099F, -132, 1", "148A3, -127, 1", NA, "")
  expect_equal(.count_pipe_stations(s), c(2L, 1L, NA, NA))
})

test_that("%||% returns the default only for NULL", {
  expect_equal(NULL %||% 1, 1)
  expect_equal(NA %||% 1, NA)
  expect_equal(0 %||% 1, 0)
})

test_that(".fill_tag_mass_total fills only missing values from weight", {
  x <- move2::mt_stack(make_track(id = "d1"), make_track(id = "d2"),
                       make_track(id = "d3"))
  td <- move2::mt_track_data(x)
  td$tag_mass_total <- units::set_units(c(1, NA, NA), "g")
  td$weight <- units::set_units(c(1.3, 1.25, NA), "g")
  x <- move2::mt_set_track_data(x, td)

  out <- move2::mt_track_data(.fill_tag_mass_total(x))
  expect_s3_class(out$tag_mass_total, "units")
  expect_equal(as.numeric(out$tag_mass_total), c(1, 1.25, NA))
})

test_that(".fill_tag_mass_total creates tag_mass_total when only weight exists", {
  x <- make_track(id = "d1")
  td <- move2::mt_track_data(x)
  td$weight <- 1.44
  x <- move2::mt_set_track_data(x, td)

  out <- move2::mt_track_data(.fill_tag_mass_total(x))
  expect_equal(as.numeric(out$tag_mass_total), 1.44)
})
