#' create odps.data and odps.vector in S4

init.odps.ext <- function() {
    #' tbl: table name, does not accept partition
    #' load.time: when object is created, to determin if the buffer data is outdated
    #' data: local buffer, if data is small enough, load it into buffer
    setClass("rodps.data", representation(tbl = "character", load.time = "POSIXct",
        data = "data.frame"))
    setClass("rodps.vector", representation(tbl = "character", col = "character",
        load.time = "POSIXct", data = "data.frame"))
}

# set up the odps.data class
rodps.data <- function(tblname) {
    rt <- new("rodps.data", tbl = tblname, load.time = Sys.time())
    return(rt)
}

# set up odps.vector class, a vector is nothing but a column in table
rodps.vector <- function(tblname, colname) {
    rt <- new("rodps.vector", tbl = tblname, col = colname, load.time = Sys.time())
    return(rt)
}

# extend hist hist returns a list of breaks,counts, density, mids, xname,
# equidist, class attr generator a sql select count( case when ... end) b1,
# count(case when ...) ... from tbl; then plot with the list

hist.rodps.vector <- function(ov) {
    h <- list()
    # nbins = 10 get the min/max
    sql <- paste("select min(", ov@col, "), ", " max(", ov@col, ") from ", ov@tbl,
        ";", sep = " ")
    cat(sql)
    cat("\n")
    mm <- rodps.sql(sql)
    w <- mm[1, 2] - mm[1, 1]
    s <- w/10

    sql <- " select "
    lb <- mm[1, 1]
    ub <- lb + s

    br <- c(lb)
    mid <- c()

    for (i in 1:10) {
        mid <- c(mid, (lb + ub)/2)
        sql <- paste(sql, " count(case when ", ov@col, ">=", lb, " and ", ov@col,
            " < ", ub, " then 1 end ),\n", sep = " ")
        lb <- ub
        ub <- lb + s

        br <- c(br, lb)
    }
    sql <- paste(sql, " count(*) \nfrom ", ov@tbl, ";")
    cat(sql)
    cat("\n")
    cnt <- rodps.sql(sql)
    v <- c()
    for (i in 1:10) {
        v <- c(v, cnt[1, i])
    }
    cat(v)
    cat("\n")
    h$breaks <- br
    h$counts <- v
    h$mids <- mid
    h$xname <- ov@col
    h$equidist <- T
    class(h) <- "histogram"
    plot(h)
    invisible(h)
}
