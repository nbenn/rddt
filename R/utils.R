
group_indices <- function(len, grp, split = FALSE) {

  stopifnot(
    is.integer(grp), length(grp) == 1L, grp >= 1L,
    is.integer(len), length(len) == 1L, len >= grp,
    is.logical(split), length(split) == 1L
  )

  if (grp == 1L) {
    if (split) return(list(seq_len(len)))
    else return(rep(1L, len))
  }

  i <- seq_len(len)

  fuzz <- min((len - 1L)/1000, 0.4 * len/grp)
  breaks <- seq(1 - fuzz, len + fuzz, length.out = grp + 1L)
  bins <- cut(i, breaks)

  if (split) {
    structure(split(i, bins), names = NULL)
  } else {
    as.integer(bins)
  }
}
