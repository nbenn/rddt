
#' TODO: make an R6 class for reference sematincs, also helps with finalization
#' which can be triggered on gc
new_rddt <- function(data, cluster = get_cl()) {

  stopifnot(data.table::is.data.table(data), inherits(cluster, "cluster"))

  data <- split(data, group_indices(nrow(data), cluster$n_workers))

  res <- list2env(
    list(
      heads = lapply(data, head),
      tails = lapply(data, tail),
      nrows = vapply(data, nrow, integer(1L)),
      cluster = cluster,
      id = rand_name()
    )
  )

  reg.finalizer(res, function(x) {
    x$cluster$call(rm, list = x$id, envir = .GlobalEnv)
  })

  cluster$scatter(data, name = res$id)

  structure(res, class = "rddt")
}

rddt <- function(..., cluster = get_cl()) {
  new_rddt(data.table::data.table(...), cluster)
}

#' @export
collect <- function(data) {
  stopifnot(inherits(data, "rddt"))
  data.table::rbindlist(
    tmp <- data$cluster$gather(data$id)
  )
}
