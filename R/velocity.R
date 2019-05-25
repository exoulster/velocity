count = function(x, na.rm=TRUE) {
  if (na.rm) sum(!is.na(x))
  else length(x)
}

validate = function(x, group_by=NULL, k=NULL, ts=NULL, func=NULL, filter=NULL) {
  if (missing(x)) stop('x is missing')
  if (length(x) == 0) stop('x is of 0 length')  # stop if x is missing or 0 length
  if (!is.null(group_by) & length(group_by) != length(x)) stop('length of group_by and x must be the same')
  if (length(k) > length(x)) stop('length of k cannot be longer than length of x')
  if (!is.null(ts) & length(x) != length(ts)) stop('ts must be of same length as x')
  if (!is.null(ts) & is.null(k)) stop('k must be present when ts is defined')
  if (!is.null(filter) & length(x) != length(filter)) stop('filter must be of same length as x')
}

parse_period = function(k) {
  if (is.character(k)) {
    if (is.null(ts)) stop('ts must be present if k is period')
    k = lubridate::period(k) %>% as.numeric()
  }
  return(k)
}

do_calculation = function(x, func, na.rm) {
  func_enq = enquo(func)
  if (rlang::quo_is_null(func_enq)) func_enq = quo(count)
  rlang::eval_tidy(func_enq)(x, na.rm=na.rm)
}


#' Window Calculation and Rolling Calculation
#' @description Applying (Rolling) Calculation to specified time window.
#' @details
#' \code{window_calc}, \code{roll_calc}, \code{roll_calc_by} are vectorized function.
#'
#' \code{velocity} and \code{velocity} are the tidy version function. They have data as input, does a series of calculation, including velocity and aggregation, and return a set of metrics
#' @param x vector to which calculation is applied
#' @param k integer offset or datetime period by which the window is generated
#' @param ts vector of timestamp (POSIXct or Date) for window calculation
#' @param func function to apply to x. Possible values include count, n_distinct, sum, mean, median, sd etc.
#' @param filter vector of index of elements to preserve on x, applies before calculation
#' @param na.rm remove NA values from x during calculation
#' @return
#' scalar for \code{window_calc} and vector for \code{roll_calc}, \code{roll_calc_by}, \code{velocity} and \code{add_velocity}
#' @import dplyr rlang
#' @export
window_calc = function(x, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE) {
  # param validation
  validate(x=x, k=k, ts=ts, func=func, filter=filter)

  # convert period to numeric if k is character input
  k = parse_period(k)

  # generate window of x for calculation
  idx = seq(length(x))
  if (!is.null(k)) {
    if (is.null(ts)) idx = tail(idx, k)
    else idx = intersect(idx, which(ts > ts[length(x)] - k))  # >= last item of x
  }
  if (!is.null(filter)) idx = intersect(idx, which(filter)) # filtering
  x = x[idx]

  # do calculation
  func_enq = rlang::enquo(func)
  res = do_calculation(x=x, func=!!func_enq, na.rm=na.rm)

  return(res)
}


<<<<<<< HEAD

#' @rdname window_calc
roll_calc = function(x, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE) {
  # param validation
  validate(x=x, k=k, ts=ts, func=func, filter=filter)

  # convert period to numeric if k is character input
  k = parse_period(k)

  # generate window of x for calculation
  ## filter -> set to NA
  if (!is.null(filter)) {
    x[!filter] = NA
    if (na.rm==FALSE) {
      na.rm = TRUE
      message('na.rm is set to TRUE when filter applies')
    }
  }
  ## window_run
  if (is.null(k)) k=0
  if (is.null(ts)) ts=1
  roll_windows = runner::window_run(x=x, k=k, idx=ts)

  # do calculation
  func_enq = rlang::enquo(func)
  res = lapply(roll_windows, function(x) {
    do_calculation(x=x, func=!!func_enq, na.rm=na.rm)
  }) %>%
    unlist()

  return(res)
}
=======

#' @rdname window_calc
roll_calc = function(x, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE) {
  # param validation
  validate(x=x, k=k, ts=ts, func=func, filter=filter)
>>>>>>> 3a092dc926540348c9bb9f8897e802ae1bdaefca

  # convert period to numeric if k is character input
  k = parse_period(k)

<<<<<<< HEAD
#' @rdname window_calc
#' @param group_by
roll_calc_by = function(x, group_by, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE, parallel=TRUE) {
  # param validation
  validate(x=x, group_by=group_by, k=k, ts=ts, func=func, filter=filter)

  # convert period to numeric if k is character input
  k = parse_period(k)

=======
>>>>>>> 3a092dc926540348c9bb9f8897e802ae1bdaefca
  # generate window of x for calculation
  ## filter -> set to NA
  if (!is.null(filter)) {
    x[!filter] = NA
    if (na.rm==FALSE) {
      na.rm = TRUE
      message('na.rm is set to TRUE when filter applies')
    }
  }
  ## window_run
  if (is.null(k)) k=0L
  if (is.null(ts)) ts=1L
  roll_windows = runner::window_run(x=x, k=k, idx=ts)

<<<<<<< HEAD
  if (parallel) mc.cores = getOption("mc.cores", 2L)
  else mc.cores = 1

  # window_run & calculation
  func_enq = rlang::enquo(func)
  if (is.null(k)) k=0  # not 0L
  if (is.null(ts)) {
    x_groups = split(x, group_by)
    mclapply(x_groups, function(x) {
      roll_calc(x=x, k=k, ts=ts, func=!!func_enq, na.rm=na.rm)
    }, mc.cores=mc.cores) %>%
=======
  # do calculation
  func_enq = rlang::enquo(func)
  res = lapply(roll_windows, function(x) {
    do_calculation(x=x, func=!!func_enq, na.rm=na.rm)
  }) %>%
    unlist()

  return(res)
}


#' @rdname window_calc
#' @param group_by
roll_calc_by = function(x, group_by, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE, parallel=TRUE) {
  # param validation
  validate(x=x, group_by=group_by, k=k, ts=ts, func=func, filter=filter)

  # convert period to numeric if k is character input
  k = parse_period(k)

  # generate window of x for calculation
  ## filter -> set to NA
  if (!is.null(filter)) {
    x[!filter] = NA
    if (na.rm==FALSE) {
      na.rm = TRUE
      message('na.rm is set to TRUE when filter applies')
    }
  }

  # window_run & calculation
  func_enq = rlang::enquo(func)
  if (is.null(k)) k=0L
  if (is.null(ts)) {
    x_groups = split(x, group_by)
    lapply(x_groups, function(x) {
      roll_calc(x=x, k=k, ts=ts, func=!!func_enq, na.rm=na.rm)
    }) %>%
>>>>>>> 3a092dc926540348c9bb9f8897e802ae1bdaefca
      unlist() %>%
      unname()
  } else {
    x_groups = split(x, group_by)
    ts_groups = split(ts, group_by)
<<<<<<< HEAD
    parallel::mcmapply(function(x, ts) {
=======
    mapply(function(x, ts) {
>>>>>>> 3a092dc926540348c9bb9f8897e802ae1bdaefca
      roll_windows = runner::window_run(x=x, k=k, idx=ts)
      lapply(roll_windows, function(x) {
        do_calculation(x=x, func=!!func_enq, na.rm=na.rm)
      }) %>% unlist()
<<<<<<< HEAD
    }, x_groups, ts_groups, mc.cores=mc.cores) %>%
=======
    }, x_groups, ts_groups) %>%
>>>>>>> 3a092dc926540348c9bb9f8897e802ae1bdaefca
      unlist() %>%
      unname()
  }
}


#' Velocity
#' @rdname window_calc
#' @import dplyr rlang
#' @export
velocity = function(data, x, group_by=NULL, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE, parallel=TRUE) {
  group_by_enq = enquo(group_by)
  x_enq = enquo(x)
  ts_enq = enquo(ts)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  if (quo_is_null(group_by_enq)) {
    data %>%
      mutate(roll_calc(x=!!x_enq, k=k, ts=!!ts_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm)) %>%
      pull()
  } else {
    data %>%
      mutate(roll_calc_by(group_by=!!group_by_enq, x=!!x_enq, k=k, ts=!!ts_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm, parallel=parallel)) %>%
      pull()
  }
}


#' Add velocity
#' @rdname window_calc
#' @param name naming the resulting velocity variables. Time window specs will also be added the final variable names.
#' @import dplyr rlang
#' @export
add_velocity = function(data, x, group_by, k=NULL, ts=NULL, func=NULL, filter=NULL, na.rm=TRUE, parallel=TRUE, name=NULL) {
  group_by_enq = enquo(group_by)
  x_enq = enquo(x)
  ts_enq = enquo(ts)
  func_enq = enquo(func)
  filter_enq = enquo(filter)

  name = case_when(
    is.null(func) & is.null(k) ~ 'ROLLING_COUNT',
    is.null(func) & !is.null(k) ~ paste('ROLLING', 'COUNT', k %>% as.character() %>% toupper(), sep='_'),
    !is.null(func) & is.null(k) ~ paste('ROLLING', quo_text(func_enq) %>% toupper(), sep='_'),
    !is.null(func) & !is.null(k) ~ paste('ROLLING', quo_text(func_enq) %>% toupper(), k %>% as.character() %>% toupper(), sep='_')
  )

  if (quo_is_null(group_by_enq)) {
    data %>%
      mutate(!!name := roll_calc(x=!!x_enq, k=k, ts=!!ts_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm))
  } else {
    data %>%
      mutate(!!name := roll_calc_by(group_by=!!group_by_enq, x=!!x_enq, k=k, ts=!!ts_enq, func=!!func_enq, filter=!!filter_enq, na.rm=na.rm, parallel=parallel))
  }
}
