
new_rddt <- function(data, cluster = get_cl()) {

  stopifnot(inherits(cluster, "cluster"))

  if (is.character(data)) {
    stopifnot(length(data) == 1L)
    info <- list(
      heads = cluster$eval(call("head", as.symbol(data), n = 10L)),
      nrows = as.integer(cluster$eval(call("nrow", as.symbol(data)))),
      cluster = cluster,
      id = data
    )
  } else if (data.table::is.data.table(data)) {
    data <- split(data, group_indices(nrow(data), cluster$n_workers))
    info <- list(
      heads = lapply(data, head, n = 10L),
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
    invisible(new_rddt(new_id))
  }
}

#' @export
print.rddt <- function(x, n = 10L, width = NULL, ...) {

  lens <- vapply(x$heads, nrow, integer(1L))

  if (min(lens) < n) {
    dat <- x$cluster$eval(call("head", as.symbol(x$id), n = n))
  } else {
    dat <- lapply(x$heads, head, n = n)
  }

  dat <- data.table::rbindlist(dat)

  real_inds <- Map(
    function(start, length) seq.int(start, length.out = length),
    cumsum(x$nrows) - x$nrows + 1L, lens
  )

  inds <- meta_col(unlist(real_inds, use.names = FALSE))

  dat <- pillar::colonnade(cbind(inds, dat), has_row_id = FALSE, width = width)
  names(dat)[1L] <- ""
  extra <- pillar::extra_cols(pillar::squeeze(dat))
  dat <- format(dat)
  width <- pillar::get_max_extent(dat)

  footer <- paste("# with", big_mark(x$nrows - lens), "more rows")
  if (length(extra) > 0L) {
    footer[length(footer)] <- paste0(
      footer[length(footer)], ", and ", length(extra),
      if (length(extra) > 1L) " more variables: " else " more variable: ",
      paste0(extra, collapse = ", ")
    )
  }
  footer <- wrap(footer, width)
  footer[-length(footer)] <- paste(
    footer[-length(footer)], paste(rep("-", width), collapse = ""), sep = "\n"
  )
  footer <- pillar::style_subtle(footer)

  header <- paste0(
    "# An rddt [", big_mark(sum(x$nrows)), " x ",
    big_mark(ncol(x$heads[[1L]])), "] with ", length(x$nrows), " partitions"
  )
  header <- pillar::style_subtle(wrap(header, width))

  breaks <- cumsum(lens) + 2L
  breaks <- breaks + seq_along(breaks) - 1L

  out <- dat
  for (i in seq_along(breaks))
    out <- append(out, footer[i], breaks[i])
  out <- c(header, out)
  class(out) <- class(dat)
  print(out)

  invisible(x)
}

meta_col <- function(ind) {
  stopifnot(is.integer(ind))
  structure(ind, class = "meta_col")
}

#' @importFrom pillar pillar_shaft
#' @export
pillar_shaft.meta_col <- function(x, ...) {
  x <- pillar::style_subtle(format(x))
  pillar::new_pillar_shaft_simple(x, ...)
}

#' @importFrom pillar type_sum
#' @export
type_sum.meta_col <- function(x) {
  structure("", class = "ts_meta_col")
}

#' @importFrom pillar format_type_sum
#' @export
format_type_sum.ts_meta_col <- function(x, ...) {
  ""
}

big_mark <- function(x, ...) {
  mark <- if (identical(getOption("OutDec"), ",")) "." else ","
  ret <- formatC(x, big.mark = mark, ...)
  ret[is.na(x)] <- "??"
  ret
}

wrap <- function(x, width, insert = "\n# ") {
  vapply(x, 
    function(y) paste0(fansi::strwrap_ctl(y, width = width), collapse = insert),
    character(1L), USE.NAMES = FALSE
  )
}
