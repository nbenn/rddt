
#' @export
rddt <- function(..., cluster = get_cl()) {

  id <- rand_name()

  dat <- data.table::data.table(...)
  dat <- split(dat, group_indices(nrow(dat), cluster$n_workers))

  cluster$scatter(dat, name = id)

  structure(
    list(
      heads = lapply(dat, head),
      tails = lapply(dat, tail),
      nrows = lapply(dat, nrow),
      cluster = cluster,
      id = id
    ),
    class = "rddt"
  )
}

#' @export
collect <- function(data) {
  stopifnot(inherits(data, "rddt"))
  data.table::rbindlist(
    tmp <- data$cluster$gather(data$id)
  )
}