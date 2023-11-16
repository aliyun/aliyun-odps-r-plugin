#' Remove NULL values from a table
#'
#' @export
na.omit.rodps.data <- function(rd) {
    rodps.table.na.omit(rd@tbl)
}

#' @export
rodps.table.na.omit <- function(tbl, tgttbl) {

    des <- rodps.table.desc(tbl)
    rows <- rodps.table.rows(tbl)
    cols <- des$columns

    cond <- paste(cols$names, " is not null ", sep = "", collapse = " \n and ")
    sql <- sprintf("create table %s as \n select * from %s \n where %s ", tgttbl,
        tbl, cond)

}
