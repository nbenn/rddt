
<!-- README.md is generated from README.Rmd. Please edit that file -->
[rddt](https://github.com/nbenn/rddt)
=====================================

The R package `rddt` is an attempt at providing a native distributed `data.frame` to R, inspired by distributed `Dataframe`s in Spark. An R package with similar intent and scope is [`big.data.table`](https://github.com/jangorecki/big.data.table). The main difference between the two R packages is how closely the data structure is coupled to the technology providing parallelism. While `big.data.table` builds on [`Rserve`](https://www.rdocumentation.org/packages/Rserve), `rddt` provides a layer of abstraction with backend implementations for [`parallel`](https://www.rdocumentation.org/packages/parallel) fork clusters and [`snow`](https://www.rdocumentation.org/packages/snow)\` MPI clusters.

Installation
------------

You can install the development version of [rddt](https://nbenn.github.io/rddt) from GitHub by running

``` r
source("https://install-github.me/nbenn/rddt")
```

Alternatively, if you have the `remotes` package available and are interested in the latest release, you can install from GitHub using `install_github()` as

``` r
# install.packages("remotes")
remotes::install_github("nbenn/rddt@*release")
```

Example
-------

Distributed `data.frame`s can be instantiated as `rddt` objects either by calling `rddt()`, `as_rddt()` or `read_rddt()`. If all data is available on the master process, it can be distributed as follows

``` r
library(rddt)
set_cl(fork_cluster, n_nodes = 2L)

# if the individual columns are available as vectors
dat <- rddt(
  a = rnorm(n = 1e5),
  b = sample(letters, size = 1e5, TRUE)
)

# if a complete data.frame type structure is available
dat <- as_rddt(nycflights13::flights, partition_by = c("origin", "dest"))
print(dat, n = 5)
#> # An rddt [336,776 x 19] with 2 partitions
#>         year month   day dep_time sched_dep_time dep_delay arr_time
#>        <int> <int> <int>    <int>          <int>     <dbl>    <int>
#>      1  2013     1     1     1315           1317        -2     1413
#>      2  2013     1     1     1655           1621        34     1804
#>      3  2013     1     1     2056           2004        52     2156
#>      4  2013     1     2     1332           1327         5     1419
#>      5  2013     1     2     1746           1621        85     1835
#> # with 168,383 more rows
#> 168389  2013     1     1     1318           1322        -4     1358
#> 168390  2013     1     1     2302           2200        62     2342
#> 168391  2013     1     2     1335           1322        13     1414
#> 168392  2013     1     2     2256           2151        65     2340
#> 168393  2013     1     3     1315           1315         0     1358
#> # with 168,383 more rows, and 12 more variables:
#> # sched_arr_time <int>, arr_delay <dbl>, carrier <chr>,
#> # flight <int>, tailnum <chr>, origin <chr>, dest <chr>,
#> # air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
#> # time_hour <dttm>
```

In most practical settings it will probably make most sense to have each process read its share of the data from file in parallel instead of reading all data on the master process and subsequently distributing the data.

``` r
# set up files to be read
tmp <- split(data.table::as.data.table(nycflights13::flights), by = "month")
files <- file.path(tempdir(), paste0("nyc_fllights_", names(tmp), ".csv"))
invisible(Map(write.csv, tmp, files))

dat <- read_rddt(files, read.csv, partition = "month")
print(dat, n = 5)
#> # An rddt [336,776 x 20] with 2 partitions
#>            X  year month   day dep_time sched_dep_time dep_delay arr_time
#>        <int> <int> <int> <int>    <int>          <int>     <int>    <int>
#>      1     1  2013     1     1      517            515         2      830
#>      2     2  2013     1     1      533            529         4      850
#>      3     3  2013     1     1      542            540         2      923
#>      4     4  2013     1     1      544            545        -1     1004
#>      5     5  2013     1     1      554            600        -6      812
#> # with 165,076 more rows
#> 165082     1  2013     4     1      454            500        -6      636
#> 165083     2  2013     4     1      509            515        -6      743
#> 165084     3  2013     4     1      526            530        -4      812
#> 165085     4  2013     4     1      534            540        -6      833
#> 165086     5  2013     4     1      542            545        -3      914
#> # with 171,690 more rows, and 12 more variables: sched_arr_time <int>,
#> # arr_delay <int>, carrier <fct>, flight <int>, tailnum <fct>,
#> # origin <fct>, dest <fct>, air_time <int>, distance <int>, hour <int>,
#> # minute <int>, time_hour <fct>

# cleanup
unlink(files)
```
