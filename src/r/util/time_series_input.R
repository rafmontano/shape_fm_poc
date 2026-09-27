time_series_input <- function(job) {
  values <- as.numeric(unlist(job$context))
  list(
    values = values,
    series = stats::ts(values, frequency = as.integer(job$seasonality))
  )
}
