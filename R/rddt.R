
#' TODO: make an R6 class for reference sematincs, also helps with finalization
#' which can be triggered on gc
new_rddt <- function(data, cluster = get_cl()) {

  stopifnot(inherits(cluster, "cluster"))

  if (is.character(data)) {
    stopifnot(length(data) == 1L)
    info <- list(
      heads = cluster$eval(call("head", as.symbol(data))),
      tails = cluster$eval(call("tail", as.symbol(data))),
      nrows = as.integer(cluster$eval(call("nrow", as.symbol(data)))),
      cluster = cluster,
      id = data
    )
  } else if (data.table::is.data.table(data)) {
    data <- split(data, group_indices(nrow(data), cluster$n_workers))
    info <- list(
      heads = lapply(data, head),
      tails = lapply(data, tail),
      nrows = vapply(data, nrow, integer(1L)),
      cluster = cluster,
      id = rand_name()
    )
  }

  res <- list2env(info)

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
`[.rddt` <- function(x, ..., in_place = TRUE) {

  dots <- match.call(expand.dots = FALSE)$`...`
  dt_call <- as.call(c(list(as.symbol("["), x = as.name(x$id)), dots))

  if (in_place) {
    expr <- substitute(expression({dt_call; TRUE}))
  } else {
    new_id <- rand_name()
    expr <- substitute(expression({assign(new_id, dt_call); TRUE}))
  }

  res <- x$cluster$eval(expr)
  stopifnot(all(unlist(res)))

  if (in_place) {
    invisible(x)
  } else {
    new_rddt(new_id)
  }
}