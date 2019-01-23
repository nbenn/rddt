
#' @export
socket_cluster <- R6::R6Class(
  "socket_cluster",
  inherit = cluster,
  private = list(
    start_cluster = function(n_nodes, ...) {
      parallel::makeForkCluster(nnodes = n_nodes, ...)
    },
    stop_cluster = function(nodes) {
      parallel::stopCluster(nodes)
    }
  )
)