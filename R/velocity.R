

#' Window Calculation
#' @description Summarise the time window and return vector of values
#' @details It has \code{data} as input, does a series of calculation, including velocity and aggregation, and return a set of metrics
#' @param data data.frame or tibble
#' @param x if NULL, count number of records of data, else count distinct values of x
#' @param func if both x and func are not NULL, apply aggregation funcs to x. Possible values include sum, mean, median, sd.
#' @return vector of metrics.
#' @import dplyr rlang
window_calculation = function(data, x=NULL, func=NULL, filter=NULL, na.rm=TRUE) {
  # enquo
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)
  if (quo_is_null(filter_enq)) filter_enq = quo(TRUE)

  # remove grouping if grouped
  if (inherits(data, 'grouped_df')) {
    data = data %>%
      ungroup()
  }

  # return 0 if data is empty
  if (nrow(data) == 0) {
    return(0)
  }

  # apply filter before calculation
  data = data %>% dplyr::filter(!!filter_enq)

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
#' @param time_window length of time window, specified as lubridate::period. Text input is allowed (1day, 5mins, etc)
#' @param offset offset from the ts of last row, specified as lubridate::period.
#' @return A subset of data
#' @import dplyr rlang
time_window = function(data, ts, time_window=NULL, offset=NULL) {
  # enquo
  ts_enq = enquo(ts)

  # Check if ts is datetime
  if (!inherits(data[[quo_text(ts_enq)]], 'POSIXct')) {
    stop('ts is not POSIXct')
  }

  # sort
  data = data %>% arrange(!!ts_enq)

  # prepare for time_window and offset
  if (!is.null(time_window) & !inherits(time_window, 'Period')) {
    time_window = lubridate::period(time_window)
  } else {
    time_window = Inf
  }
  if (!is.null(offset) & !inherits(offset, 'Period')) {
    offset = lubridate::period(offset)
  } else {
    offset = 0
  }

  # subset time window
  # end_date = data %>% summarise(max(!!ts_enq)) %>% pull() - offset
  # end_date = data[[nrow(data), quo_text(ts_enq)]] - offset
  end_date = max(data[[quo_text(ts_enq)]]) - offset
  start_date = end_date - time_window
  # data = data %>%
  #   filter(!!ts_enq >= start_date,
  #          !!ts_enq <= end_date)  # exclude current row to avoid duplicate rows at the same time
  idx = between(data[[quo_text(ts_enq)]], start_date, end_date)
  data = data[idx,]

  return(data)
}


#' Window Calculation Along
#' @description Window Calculation for each record of the table
#' @import dplyr rlang
window_calculation_along = function(data, ts, time_window=NULL, x=NULL,
        func=NULL, filter=NULL, offset=NULL, na.rm=TRUE,
        mc.cores = getOption("mc.cores", 2L)) {
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)
  if (quo_is_null(filter_enq)) filter_enq = quo(TRUE)

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
      time_window(ts=!!ts_enq, time_window=time_window, offset=offset) %>%  # time window
      window_calculation(x=!!x_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm)
  }) %>% unlist()
}


#' Window Calculation Along 2
#' @description Window Calculation for each record of the table
#' @import dplyr rlang
window_calculation_along2 = function(data, ts, time_window=Inf, x=NULL,
                                    func=NULL, filter=NULL, na.rm=TRUE) {
  count = function(x, na.rm=TRUE) {
    if (na.rm) sum(!is.na(x))
    else length(x)
  }

  if (missing(ts)) stop('ts is missing')
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  # x is NULL
  if (quo_is_null(x_enq)) {
    x_enq = quo(x)
    data = data %>%
      mutate(!!x_enq := row_number())
    func_enq = quo(count)  # only do count when x is null
  } else {
    if (quo_is_null(func_enq)) {
      func_enq = quo(n_distinct)
    }
  }

  # prepare for time_window and offset
  if (!is.infinite(time_window) & !inherits(time_window, 'Period')) {
    time_window = lubridate::period(time_window) %>% as.numeric()
  } else {
    time_window = Inf
  }

  # remove grouping if grouped
  if (inherits(data, 'grouped_df')) {
    data = data %>%
      ungroup()
  }

  # sort
  data = data %>% arrange(!!ts_enq)

  # window calculation along
  ## Filter -> Mangle
  if (quo_is_null(filter_enq)) filter_enq = quo(rep(TRUE, nrow(data)))
  data = data %>% mutate(!!x_enq := ifelse(!!filter_enq, !!x_enq, NA))

  ## return 0 if all NA
  if (is.na(data[[quo_text(x_enq)]]) %>% all()) {
    return(rep(0, nrow(data)))
  }

  ## Window subset
  if (is.infinite(time_window)) {
    lst = runner::window_run(x=data[[quo_text(x_enq)]], k=0)
  } else {
    lst = runner::window_run(x=data[[quo_text(x_enq)]], k=time_window, idx=data[[quo_text(ts_enq)]])
  }

  ## Do calculation
  res = lapply(lst, function(x) {
    eval_tidy(func_enq)(x, na.rm=na.rm)
  }) %>% unlist()

  return(res)
}


#' Velocity
#' @description Velocity function for mutate/transmute
#' @details
#' \code{velocity} calculates number of records or other aggregated values for a time window, tracing back from each records.
#' \code{add_velocity} does the same as velocity and gives back a tibble containing both original data and the respective velocity columns.
#' @param data data.frame or tibble
#' @param ... Variables to group by.
#' @param x Target column for calculation. If NULL, count number of records of data, else count distinct values of x
#' @param ts Datetime column of transaction timestamp.
#' @param time_window Length of time window, could be vector.
#' @param func If both x and func exist, apply aggregation funcs to x. Possible values include sum, mean, median, sd.
#' @param filter Filter records before calculation.
#' @return
#' \code{velocity} returns vector of velocity value
#' \code{velocity} returns a tibble containing both original data and new velocity columns
#' @import dplyr rlang
#' @export
velocity = function(data, ..., x=NULL, ts, time_window=Inf, func=NULL, filter=NULL, na.rm=TRUE) {
  # enquo
  group_by_enqs = quos(...)
  if (missing(ts)) stop('ts is missing')
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

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
    window_calculation_along2(group, ts=!!ts_enq, time_window=time_window,
          x=!!x_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm)
  }) %>% unlist()
}


#' Add velocity
#' @rdname velocity
#' @param name naming the resulting velocity variables. Time window specs will also be added the final variable names.
#' @import dplyr rlang
#' @export
add_velocity = function(data, ..., x=NULL, ts, time_window=Inf, func=NULL, filter=NULL, na.rm=TRUE, name=NULL) {
  group_by_enqs = quos(...)
  if (missing(ts)) stop('ts is missing')
  ts_enq = enquo(ts)
  x_enq = enquo(x)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  # prepare name_enq
  if (is.null(name)) {
    field_name = paste('VEL', toupper(time_window), sep='_')
  } else {
    field_name = paste(name, toupper(time_window), sep='_')
  }

  # loop through time_window vector
  for (i in seq_along(time_window)) {
    fn_enq = sym(field_name[i])
    data = data %>%
      mutate(
        !!fn_enq := velocity(data=data, !!!group_by_enqs, ts=!!ts_enq,
                                 time_window=time_window[i], x=!!x_enq,
                                 func=!!func_enq, filter=!!filter_enq, na.rm=na.rm)
    )
  }

  return(data)
}
