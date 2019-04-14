
#' Window Calculation
#' @description Summarise the time window and return vector of values
#' @details It has \code{data} as input, does a series of calculation, including velocity and aggregation, and return a set of metrics
#' @param data
#' @param ... Variables to group by.
#' @param x If NULL, count number of records of data, else count distinct values of x
#' @param func If both x and func are not NULL, apply aggregation funcs to x. Possible values include sum, mean, median, sd.
#' @return Vector of metrics.
#' @import dplyr rlang
window_calculation = function(data, x=NULL, func=NULL, na.rm=TRUE) {
  # enquo
  x_enq = enquo(x)
  func_enq = enquo(func)

  # remove grouping if grouped
  if (inherits(data, 'grouped_df')) {
    data = data %>%
      ungroup()
  }

  # return 0 if data is empty
  if (nrow(data) == 0) {
    return(0)
  }

  # summarise window
  if (quo_is_null(x_enq)) { # count number of rows
    res = data %>% count() %>% pull()
  } else if (quo_is_null(func_enq)) { # count distinct value of x
    if (data %>% tail(1) %>% select(!!x_enq) %>% pull() %>% is.na()) { # if value of last row is NA, then 0
      res = 0
    } else {
      res = data %>% summarise(n_distinct(!!x_enq, na.rm=na.rm)) %>% pull()
    }
  } else { # aggregate x using func
    res = data %>% summarise(eval_tidy(func_enq)(!!x_enq, na.rm=na.rm)) %>% pull()
  }

  return(res)
}


#' Time Window
#' @description Subset data by a time window of specific length
#' @param ts datetime of type POSIXct
#' @param window_length length of time window, specified as lubridate::period. Text input is allowed (1day, 5mins, etc)
#' @param offset offset from the ts of last row, specified as lubridate::period.
#' @return A subset of data
#' @import dplyr rlang
time_window = function(data, ts, window_length=NULL, offset=NULL) {
  # enquo
  ts_enq = enquo(ts)

  # Check if ts is datetime
  if (!data %>% select(!!ts_enq) %>% pull() %>% inherits('POSIXct')) {
    stop('ts is not POSIXct')
  }

  # sort
  data = data %>% arrange(!!ts_enq)

  # prepare for window_length and offset
  if (!is.null(window_length) & !inherits(window_length, 'Period')) {
    window_length = lubridate::period(window_length)
  } else {
    window_length = Inf
  }
  if (!is.null(offset) & !inherits(offset, 'Period')) {
    offset = lubridate::period(offset)
  } else {
    offset = 0
  }

  # subset time window
  end_date = data %>% select(!!ts_enq) %>% pull() %>% max() - offset
  start_date = end_date - window_length
  data = data %>%
    filter(!!ts_enq >= start_date,
           !!ts_enq <= end_date)  # exclude current row to avoid duplicate rows at the same time

  return(data)
}


#' Window Calculation Along
#' @description Window Calculation for each record of the table
#' @import dplyr rlang
window_calculation_along = function(data, ts, window_length=NULL, x=NULL, func=NULL, filter=NULL, offset=NULL, na.rm=TRUE) {
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  # prepare filter_enq
  if (quo_is_null(filter_enq)) {
    filter_enq = quo(TRUE)
  }

  # remove grouping if grouped
  if (inherits(data, 'grouped_df')) {
    data = data %>%
      ungroup()
  }

  # sort
  data = data %>% arrange(!!ts_enq)

  # window calculation along
  parallel::mclapply(1:nrow(data), function(rn) {
    data %>%
      slice(1:rn) %>% # roll along
      time_window(ts=!!ts_enq, window_length=window_length, offset=offset) %>%  # time window
      dplyr::filter(!!filter_enq) %>%
      window_calculation(x=!!x_enq, func=!!func_enq, na.rm=na.rm)
  }) %>% unlist()
}

# test = function(data, filter=NULL) {
#   filter_enq = enquo(filter)
#
#   if (rlang::quo_is_null(filter_enq)) {
#     filter_enq = quo(TRUE)
#   }
#
#   data %>% dplyr::filter(!!filter_enq)
# }


#' Velocity
#' @description Velocity function for mutate/transmute
#' @details
#' @param data
#' @param ts datetime vector of transaction timestamp.
#' @param window_length length of time window.
#' @param x If NULL, count number of records of data, else count distinct values of x
#' @param func If both x and func are not NULL, apply aggregation funcs to x. Possible values include sum, mean, median, sd.
#' @param filter Filter
#' @param offset \code{period} by which to offset from current timestamp.
#' @return vector of velocity value
#' @import dplyr rlang
#' @export
velocity = function(data, ..., ts, window_length=NULL, x=NULL, func=NULL, filter=NULL, offset=NULL, na.rm=TRUE) {
  # enquo
  group_by_enqs = quos(...)
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  # prepare filter_enq
  if (quo_is_null(filter_enq)) {
    filter_enq = quo(TRUE)
  }

  # remove grouping if grouped
  if (inherits(data, 'grouped_df')) {
    data = data %>%
      ungroup()
  }

  # prepare data
  data = data %>%
    mutate(idx = row_number())

  # group by and split
  data = data %>%
    group_by(!!!group_by_enqs) %>%
    group_split()

  # do calculation, nested loop
  parallel::mclapply(data, function(group) {
    window_calculation_along(group, ts=!!ts_enq, window_length=window_length, x=!!x_enq,
                            func=!!func_enq, filter=!!filter_enq, offset=offset, na.rm=na.rm)
  }) %>% unlist()
}


#' Add velocity
#' @import dplyr rlang
#' @export
add_velocity = function(data, ..., ts, window_length=NULL, x=NULL, func=NULL, filter=NULL, offset=NULL, na.rm=TRUE, name=NULL) {
  group_by_enqs = quo(...)
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)
  name_enq = enquo(name)

  # prepare filter_enq
  if (quo_is_null(filter_enq)) {
    filter_enq = quo(TRUE)
  }

  # prepare name_enq
  if (quo_is_null(name_enq)) {
    field_name = sym('vel')
  } else {
    field_name = name_enq
  }
  data %>% mutate(
    !!field_name := velocity(data=data, !!group_by_enqs, ts=!!ts_enq,
                             window_length=window_length, x=!!x_enq,
                             func=!!func_enq, filter=!!filter_enq, offset=offset, na.rm=na.rm)
  )
}
