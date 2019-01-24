
#' @export
fork_cluster <- R6::R6Class(
  "fork_cluster",
  inherit = cluster,
  private = list(
    start_cluster = function(n_nodes, ...) {
      res <- parallel::makeForkCluster(nnodes = n_nodes, ...)
      structure(
        c(list(list(con = NULL, host = "localhost", rank = 0L)), res),
        class = class(res)
      )
    },
    stop_cluster = function(nodes) {
      parallel::stopCluster(nodes[-1L])
    },
    send_cluster = function(dat, dst, src, nme, ...) {
      stopifnot(src$rank == 0L)
      res <- send_to_node(dat, dst, nme)
      stopifnot(res$success, res$type == "VALUE", is.null(res$value))
    },
    receive_cluster = function(nme, src, dst, ...) {
      stopifnot(dst$rank == 0L)
      res <- retrieve_from_node(src, nme)
      stopifnot(res$success, res$type == "VALUE")
      res$value
    },
    call_node = function(fun, dst, ...) {
      stopifnot(dst$rank != 0L)
      res <- node_call(dst, fun, ...)
      stopifnot(res$success, res$type == "VALUE")
      res$value
    }
  )
)

node_call <- function(node, fun, ...) {
  serialize(
    list(
      type = "EXEC",
      data = list(fun = fun, args = list(...), return = TRUE, tag = NULL)
    ), node$con, xdr = FALSE
  )
  unserialize(node$con)
}

send_to_node <- local({
  assign_gobal <- function(n, v) {
    assign(n, v, envir = .GlobalEnv)
    NULL
  }
  function(dat, node, name) node_call(node, assign_gobal, name, dat)
})

retrieve_from_node <- function(node, name) {
  node_call(node, get, name, envir = .GlobalEnv)
}
