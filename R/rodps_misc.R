#' @export
init.odps.ext <- function() {
    #' tbl: table name, does not accept partition
    #' load.time: when object is created, to determine if the buffer data is outdated
    #' data: local buffer, if data is small enough, load it into buffer
    setClass("rodps.data", representation(tbl = "character", load.time = "POSIXct",
        data = "data.frame"))
    setClass("rodps.vector", representation(tbl = "character", col = "character",
        load.time = "POSIXct", data = "data.frame"))
}

#' Set up the odps.data class
#' @export
rodps.data <- function(tblname) {
    rt <- new("rodps.data", tbl = tblname, load.time = Sys.time())
    return(rt)
}

#' Set up odps.vector class, a vector is nothing but a column in table
#' @export
rodps.vector <- function(tblname, colname) {
    rt <- new("rodps.vector", tbl = tblname, col = colname, load.time = Sys.time())
    return(rt)
}

#' Remove NULL values from a table
#' @export
na.omit.rodps.data <- function(rd) {
    rodps.table.na.omit(rd@tbl)
}

#' Remove NULL values from a table
#' @export
rodps.table.na.omit <- function(tbl, tgttbl) {
    des <- rodps.table.desc(tbl)
    rows <- rodps.table.rows(tbl)
    cols <- des$columns
    cond <- paste(cols$names, " is not null ", sep = "", collapse = " \n and ")
    sql <- sprintf("create table %s as \n select * from %s \n where %s ", tgttbl,
        tbl, cond)
}
