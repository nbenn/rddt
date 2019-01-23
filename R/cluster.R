
set_active_cluster <- NULL
get_active_cluster <- NULL
local({
  active_cluster <- NULL
  get_active_cluster <<- function() active_cluster
  set_active_cluster <<- function(x) active_cluster <<- x
})

cluster <- R6::R6Class(
  "cluster",
  public = list(
    initialize = function(...) {
      if (is.null(get_active_cluster())) {
        private$nodes <- private$start_cluster(...)
        set_active_cluster(self)
      } else {
        stop("only a single active cluster is allowed.")
      }
      invisible(NULL)
    },
    finalize = function() {
      private$stop_cluster(private$nodes)
      set_active_cluster(NULL)
      invisible(NULL)
    },
    broadcast = function(data, dest = seq.int(2L, self$n_nodes),
                         source = 1L, ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) > 1L,
        all(c(source, dest) >= 1L), all(c(source, dest) <= self$n_nodes),
        length(intersect(source, dest)) == 0L
      )
      private$bcast_cluster(data, private$nodes[dest], private$nodes[[source]],
                            ...)
    },
    scatter = function(data, dest = seq.int(2L, self$n_nodes),
                       source = 1L, ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) > 1L,
        all(c(source, dest) >= 1L), all(c(source, dest) <= self$n_nodes),
        length(intersect(source, dest)) == 0L
      )
      private$scatter_cluster(data, private$nodes[dest],
                              private$nodes[[source]], ...)
    },
    send = function(data, dest, source = 1L, ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) == 1L,
        source <= self$n_nodes, dest <= self$n_nodes,
        source >= 1L, dest >= 1L, source != dest
      )
      private$send_cluster(data, private$nodes[[dest]], private$nodes[[source]],
                           ...)
    }
  ),
  active = list(
    n_nodes = function() length(private$nodes),
    n_workers = function() self$n_nodes - 1L
  ),
  private = list(
    nodes = list(),
    start_cluster = function(...)
      message("need a \"start_cluster\" method."),
    stop_cluster = function(nodes)
      message("need a \"stop_cluster\" method."),
    bcast_cluster = function(dat, dst, src, ...) {
      lapply(dst, function(x) private$send_cluster(dat, x, src, ...))
    },
    scatter_cluster = function(dat, dst, src, ...) {
      Map(function(x, y) private$send_cluster(x, y, src, ...), dat, dst)
    },
    send_cluster = function(dat, dst, src, ...)
      message("need a \"send_cluster\" method.")
  )
)

#' @export
set_cl <- function(generator, ...) {

  find_classes <- function(x) {
    if (is.null(x)) return(NULL)
    parent <- x$get_inherit()
    c(x$classname, find_classes(parent))
  }

  stopifnot(
    R6::is.R6Class(generator),
    tail(find_classes(generator), n = 1L) == "cluster"
  )

  generator$new(...)

  invisible(NULL)
}

#' @export
get_cl <- function() {
  ac <- get_active_cluster()
  if (is.null(ac)) stop("create a cluster first")
  else ac
}

#' @export
rm_cl <- function() {
  get_cl()$finalize()
}