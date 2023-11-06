\name{rodps.table.partitions}
\alias{RODPS.TABLE.PARTITIONS}
\alias{rodps.table.partitions}
\title{ List Partitions }
\usage{
rodps.table.partitions( full.tablename )
}
\description{
 list partitions of table, ERROR if the table has no partition.
}
\arguments{
\item{ full.tablename }{ table name ,  in format 'ProjectName.TableName',or 'TableName'(using current project)}
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## list partitions of 'sales'
\dontrun{rodps.table.partitions('sales')}
}
\seealso{
 \code{\link{rodps.table.desc}},
 \code{\link{rodps.table.drop}},
 \code{\link{rodps.table.exist}},
 \code{\link{rodps.table.partitions}},
 \code{\link{rodps.table.list}},
 \code{\link{rodps.table.rows}},
 \code{\link{rodps.table.size}},
 \code{\link{rodps.table.read}},
 \code{\link{rodps.table.write}}
}
\keyword{programming}
