\name{rodps.table.size}
\alias{RODPS.TABLE.SIZE}
\alias{rodps.table.size}

\title{ Table Size }
\usage{
rodps.table.size( full.tablename , partition = NULL)
}
\description{
  get the size of table, unit is Byte. 
}
\arguments{
\item{ full.tablename }{ table name ,  in format 'ProjectName.TableName',or 'TableName'(using current project)}.
\item{ partition }{ partition spec }
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## get the size of 'sales'
\dontrun{rodps.table.size('sales')}
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
