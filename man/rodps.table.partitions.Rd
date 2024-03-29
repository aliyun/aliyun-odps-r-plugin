% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rodps_table.R
\name{rodps.table.partitions}
\alias{rodps.table.partitions}
\alias{rodps.partitions.table}
\title{List Partitions}
\usage{
rodps.table.partitions(full.tablename)

rodps.partitions.table(full.tablename)
}
\arguments{
\item{full.tablename, }{Table name, in format of 'ProjectName.TableName' or
'TableName' (using current project).}
}
\description{
List partitions of a table. Raise ERROR if the table has no partition.
}
\examples{
## list partitions of 'sales'
\dontrun{rodps.table.partitions('sales')}
}
\seealso{
\code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
  \code{\link{rodps.table.exist}}, \code{\link{rodps.table.list}},
  \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
  \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
}
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
