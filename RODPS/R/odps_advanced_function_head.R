###########################################
# create odps.data and odps.vector in S4  #
###########################################

##############################################
# store head   result in a temp table        #
##############################################
head.rodps.data <- function( rd ,n=6L)
{
  rodps.table.head( rd@tbl )
}

rodps.table.head <-function( tbl, n=6L)
{
  #could be optimized by identify the tbl/partiton tbl/view
  sql <- sprintf("select * from %s limit %d;",tbl, n)
  df <- rodps.sql( sql )
  df
}
