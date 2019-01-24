
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

#' @export
rddt <- function(..., cluster = get_cl()) {
  new_rddt(data.table::data.table(...), cluster)
}

#' @export
as_rddt <- function(data, cluster = get_cl()) {
  new_rddt(data.table::as.data.table(data), cluster)
}

#' @export
collect <- function(data) {
  stopifnot(inherits(data, "rddt"))
  data.table::rbindlist(
    data$cluster$gather(data$id)
  )
}

#' @export
`[.rddt` <- function(x, ...) {
  dots <- match.call(expand.dots = FALSE)$`...`
  dt_call <- as.call(c(list(as.symbol("["), x = as.name(x$id)), dots))
  expr <- substitute(expression({dt_call; NULL}))
  x$cluster$eval(expr)
}