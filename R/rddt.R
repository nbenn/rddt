
#' @export
rddt <- function(..., cluster = get_cl()) {

  id <- paste(
    sample(c(LETTERS, letters), 10L, replace = TRUE),
    collapse = ""
  )

  dat <- split(
    data.table::data.table(...),
    group_indices(nrow(dat), cluster$n_workers)
  )

  cluster$scatter(dat)

  structure(
    list(
      heads = lapply(dat, head),
      tails = lapply(dat, tail),
      nrows = lapply(dat, nrow),
      cluster = cluster
    ),
    class = "rddt"
  )
}