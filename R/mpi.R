
#' @export
mpi_cluster <- R6::R6Class(
  "mpi_cluster",
  inherit = cluster,
  private = list(
    start_cluster = function(n_nodes, comm = 1L, ...) {
      if (missing(n_nodes)) {
        stop("TODO (utilize cluster started with mpirun)")
      } else {
        if (is.null(n_nodes) || is.na(n_nodes)) {
          n_nodes <- Rmpi::mpi.universe.size() - 1L
        }
        Rmpi::mpi.spawn.Rslaves(nslaves = n_nodes, comm = comm, ...)
      }

      res <- lapply(seq_len(n_nodes), function(i) list(rank = i, comm = comm))
      attr(res, "spawned") <- TRUE
      res
    },
    stop_cluster = function(nodes) {
      if (isTRUE(attr(nodes, "spawned"))) {
        Rmpi::mpi.close.Rslaves()
      } else {
        stop("TODO (stop cluster started with mpirun)")
      }
    }
  )
)