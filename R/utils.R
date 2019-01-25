
group_indices <- function(len, grp, split = FALSE) {

  stopifnot(
    is.integer(grp), length(grp) == 1L, grp >= 1L,
    is.integer(len), length(len) == 1L, len >= grp,
    is.logical(split), length(split) == 1L
  )

  if (grp == 1L) {
    if (split) return(list(seq_len(len)))
    else return(rep(1L, len))
  }

  i <- seq_len(len)

  fuzz <- min((len - 1L)/1000, 0.4 * len/grp)
  breaks <- seq(1 - fuzz, len + fuzz, length.out = grp + 1L)
  bins <- cut(i, breaks)

  if (split) {
    structure(split(i, bins), names = NULL)
  } else {
    as.integer(bins)
  }
}

rand_name <- function(alphabet = c(LETTERS, letters), length = 10L) {
  paste(sample(alphabet, length, replace = TRUE), collapse = "")
}

new_rand_name <- function(obj, ...) {
  repeat{
    new <- rand_name(...)
    if (!new %in% names(obj)) break
  }
  new
}

distribute_evenly <- function(weights, n_bins) {
  stopifnot(
    is.integer(n_bins), length(n_bins) == 1L, n_bins > 0L,
    is.integer(weights), length(weights) >= n_bins
  )
  bins <- rep(0L, n_bins)
  res <- rep(NA_integer_, length(weights))
  for (i in order(weights, decreasing = TRUE)) {
    bin <- which.min(bins)
    bins[bin] <- bins[bin] + weights[i]
    res[i] <- bin
  }
  res
}

grouped_split_by <- function(data, by, n_bins) {
  stopifnot(
    data.table::is.data.table(data),
    is.character(by), length(by) > 0L, all(by %in% colnames(data)),
    is.integer(n_bins), length(n_bins) == 1L, n_bins > 0L
  )
  tmp_col <- new_rand_name(data)
  groupings <- data[, setNames(list(.N), tmp_col), by = by]
  groupings <- groupings[, 
    (tmp_col) := distribute_evenly(get(tmp_col), n_bins)
  ]
  colorder <- colnames(data)
  data <- merge(data, groupings, by = by)
  data <- data.table::setcolorder(data, colorder)
  lapply(
    split(data, by = tmp_col), function(x) x[, (tmp_col) := NULL]
  )
}