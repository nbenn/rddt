
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
    nodes = NULL,
    initialize = function(...) {
      if (is.null(get_active_cluster())) {
        self$nodes <- private$start_cluster(...)
        set_active_cluster(self)
      } else {
        stop("only a single active cluster is allowed.")
      }
      invisible(NULL)
    },
    finalize = function() {
      private$stop_cluster(self$nodes)
      set_active_cluster(NULL)
      invisible(NULL)
    }
  ),
  private = list(
    start_cluster = function(...) NULL,
    stop_cluster = function(nodes) NULL
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