##############################################
# remove NULL values from a tabl             #
##############################################

na.omit.rodps.data <- function( rd )
{
  rodps.table.na.omit( rd@tbl )
}

rodps.table.na.omit <- function( tbl ,tgttbl)
{

  des <- rodps.table.desc( tbl )
  rows <- rodps.table.rows( tbl )
  cols <- des$columns

  cond <- paste( cols$names, " is not null ", sep="", collapse=" \n and ")
  sql <- sprintf("create table %s as \n select * from %s \n where %s ", tgttbl, tbl, cond)

}

#remove the columns which has only constant value
rodps.const.omit <- function( tbl, tgttbl)
{
}
