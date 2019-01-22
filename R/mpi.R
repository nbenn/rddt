
#' @export
mpi_cluster <- R6::R6Class(
  "mpi_cluster",
  inherit = cluster,
  private = list(
    start_cluster = function(n_nodes) message("starting", n_nodes, "processes"),
    stop_cluster = function() message("finalizing", n_nodes, "processes")
  )
)