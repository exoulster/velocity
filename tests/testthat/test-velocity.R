context("test-velocity")

dfr = dplyr::tibble(
  request_id = c(1,2,3,4,5,6),
  order_id = c('a', 'a', 'b', 'c', 'd', 'd'),
  ip_address = c(1,NA,2,NA,1,5),
  ts = readr::parse_datetime(c('2019-01-01 00:00:00', '2019-01-01 00:05:00', '2019-01-01 00:08:00',
         '2019-01-01 00:10:00', '2019-01-01 00:11:00', '2019-01-01 00:15:00')),
  price = c(10,10,20,20,15,15)
)

test_that('window_calc', {
  expect_error(window_calc(integer()))
  expect_equal(window_calc(dfr$request_id), 6)
  expect_equal(window_calc(dfr$request_id, k=3), 3)
  expect_equal(window_calc(dfr$request_id, k=3, ts=dfr$ts), 1)
  expect_equal(window_calc(dfr$request_id, k='5 minutes', ts=dfr$ts), 2) # idx 5 and 6
  expect_equal(window_calc(dfr$request_id, func=mean), 3.5)
  expect_equal(window_calc(dfr$order_id, func=n_distinct), 4)
  expect_equal(window_calc(dfr$request_id, filter=dfr$price>=20), 2)
  expect_equal(window_calc(dfr$ip_address, na.rm=FALSE), 6)
})

test_that('roll_calc', {
  expect_error(roll_calc(integer()))
  expect_equal(roll_calc(dfr$request_id), c(1,2,3,4,5,6))
  expect_equal(roll_calc(dfr$request_id, k=3), c(1,2,3,3,3,3))
  expect_equal(roll_calc(dfr$request_id, k=3, ts=dfr$ts), c(1,1,1,1,1,1))
  expect_equal(roll_calc(dfr$request_id, k='5 minutes', ts=dfr$ts), c(1,1,2,2,3,2))
  expect_equal(roll_calc(dfr$request_id, func=mean), c(1,1.5,2,2.5,3,3.5))
  expect_equal(roll_calc(dfr$order_id, func=n_distinct), c(1,1,2,3,4,4))
  expect_equal(roll_calc(dfr$request_id, filter=dfr$price>=20), c(0,0,1,2,2,2))
  expect_equal(roll_calc(dfr$ip_address, na.rm=FALSE), c(1,2,3,4,5,6))
})

test_that('roll_calc_by', {
  expect_error(roll_calc_by(integer()))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id), c(1,2,1,1,1,2))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id, k=3), c(1,2,1,1,1,2))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id, k=3, ts=dfr$ts), c(1,1,1,1,1,1))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id, k='5 minutes', ts=dfr$ts), c(1,1,1,1,1,2))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id, func=mean), c(1,1.5,3,4,5,5.5))
  expect_equal(roll_calc_by(dfr$price, dfr$order_id, func=n_distinct), c(1,1,1,1,1,1))
  expect_equal(roll_calc_by(dfr$request_id, dfr$order_id, filter=dfr$price>=20), c(0,0,1,1,0,0))
  expect_equal(roll_calc_by(dfr$ip_address, dfr$order_id, na.rm=FALSE), c(1,2,1,1,1,2))
})

test_that('velocity', {
  expect_equal(velocity(dfr, request_id, order_id), c(1,2,1,1,1,2))
  expect_equal(velocity(dfr, request_id, order_id, k=3), c(1,2,1,1,1,2))
  expect_equal(velocity(dfr, request_id, order_id, k=3, ts=dfr$ts), c(1,1,1,1,1,1))
  expect_equal(velocity(dfr, request_id, order_id, k='5 minutes', ts=dfr$ts), c(1,1,1,1,1,2))
  expect_equal(velocity(dfr, request_id, order_id, func=mean), c(1,1.5,3,4,5,5.5))
  expect_equal(velocity(dfr, price, order_id, func=n_distinct), c(1,1,1,1,1,1))
  expect_equal(velocity(dfr, request_id, order_id, filter=price>=20), c(0,0,1,1,0,0))
  expect_equal(velocity(dfr, ip_address, order_id, na.rm=FALSE), c(1,2,1,1,1,2))
})

test_that('add_velocity', {
  expect_equal(add_velocity(dfr, request_id, order_id) %>% pull(), c(1,2,1,1,1,2))
})

