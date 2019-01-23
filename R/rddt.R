
#' @export
rddt <- function(..., cluster = get_cl()) {

  id <- paste(
    sample(c(LETTERS, letters), 10L, replace = TRUE),
    collapse = ""
  )

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