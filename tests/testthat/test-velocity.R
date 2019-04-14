context("test-velocity")

dfr = dplyr::tibble(
  request_id = c(1,2,3,4,5,6),
  order_id = c('a', 'a', 'b', 'c', 'd', 'd'),
  ip_address = c(1,NA,2,NA,1,5),
  ts = readr::parse_datetime(c('2019-01-01 00:00:00', '2019-01-01 00:05:00', '2019-01-01 00:08:00',
         '2019-01-01 00:10:00', '2019-01-01 00:11:00', '2019-01-01 00:15:00'))
)

test_that('window_calculation', {
  expect_equal(window_calculation(dfr), 6)
  expect_equal(window_calculation(dfr, order_id), 4)
  expect_equal(window_calculation(dfr, ip_address), 3)
  expect_equal(window_calculation(dfr, ip_address, na.rm=FALSE), 4)
})

test_that('time_window', {
  expect_equal(time_window(dfr, ts) %>% pull(request_id), c(1,2,3,4,5,6))
  expect_equal(time_window(dfr, ts, '5min') %>% pull(request_id), c(4,5,6))
  expect_equal(time_window(dfr, ts, offset='5mins') %>% pull(request_id), c(1,2,3,4))
})

test_that('window_calculation_along', {
  expect_equal(window_calculation_along(dfr, ts=ts), c(1,2,3,4,5,6))
  expect_equal(window_calculation_along(dfr, ts=ts, window_length='5mins'), c(1,2,2,3,3,3))
  expect_equal(window_calculation_along(dfr, ts=ts, window_length='5mins', x=order_id), c(1,1,2,3,3,2))
  expect_equal(window_calculation_along(dfr, ts=ts, filter=!is.na(ip_address)), c(1,1,2,2,3,4))
})

test_that('velocity', {
  expect_equal(velocity(dfr, order_id, ts=ts), c(1,2,1,1,1,2))
})

test_that('add_velocity', {
  expect_equal(add_velocity(dfr, order_id, ts=ts) %>% pull(vel), c(1,2,1,1,1,2))
})

