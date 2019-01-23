
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
                         source = 1L, name = rand_name(), ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) >= 1L,
        all(c(source, dest) >= 1L), all(c(source, dest) <= self$n_nodes),
        length(intersect(source, dest)) == 0L,
        is.list(data), length(data) == length(dest)
      )
      private$bcast_cluster(data, private$nodes[dest], private$nodes[[source]],
                            name, ...)
      invisible(NULL)
    },
    scatter = function(data, dest = seq.int(2L, self$n_nodes),
                       source = 1L, name = rand_name(), ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) >= 1L,
        all(c(source, dest) >= 1L), all(c(source, dest) <= self$n_nodes),
        length(intersect(source, dest)) == 0L,
        is.list(data), length(data) == length(dest)
      )
      private$scatter_cluster(data, private$nodes[dest],
                              private$nodes[[source]], name, ...)
      invisible(NULL)
    },
    send = function(data, dest, source = 1L, name = rand_name(), ...) {
      stopifnot(
        is.integer(source), is.integer(dest),
        length(source) == 1L, length(dest) == 1L,
        source <= self$n_nodes, dest <= self$n_nodes,
        source >= 1L, dest >= 1L, source != dest
      )
      private$send_cluster(data, private$nodes[[dest]], private$nodes[[source]],
                           name, ...)
      invisible(NULL)
    },
    gather = function(name, source = seq.int(2L, self$n_nodes),
                      dest = 1L, ...) {
      stopifnot(
        is.integer(dest), is.integer(source),
        length(dest) == 1L, length(source) >= 1L,
        all(c(dest, source) >= 1L), all(c(dest, source) <= self$n_nodes),
        length(intersect(dest, source)) == 0L,
        is.character(name), length(name) == 1L
      )
      private$gather_cluster(name, private$nodes[source],
                             private$nodes[[dest]], ...)
    },
    receive = function(name, source, dest = 1L, ...) {
      stopifnot(
        is.integer(dest), is.integer(source),
        length(dest) == 1L, length(source) == 1L,
        dest <= self$n_nodes, source <= self$n_nodes,
        dest >= 1L, source >= 1L, dest != source,
        is.character(name), length(name) == 1L
      )
      private$receive_cluster(name, private$nodes[[source]],
                              private$nodes[[dest]], ...)
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
    bcast_cluster = function(dat, dst, src, nme, ...) {
      lapply(dst, function(x) private$send_cluster(dat, x, src, nme, ...))
    },
    scatter_cluster = function(dat, dst, src, nme, ...) {
      Map(function(x, y) private$send_cluster(x, y, src, nme, ...), dat, dst)
    },
    send_cluster = function(dat, dst, src, nme, ...)
      message("need a \"send_cluster\" method."),
    gather_cluster = function(nme, src, dst, ...) {
      lapply(src, function(x) private$receive_cluster(nme, x, dst, ...))
    },
    receive_cluster = function(nme, src, dst, ...)
      message("need a \"receive_cluster\" method.")
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