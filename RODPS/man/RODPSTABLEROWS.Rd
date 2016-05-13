\name{rodps.table.rows}
\alias{RODPS.TABLE.ROWS}
\alias{rodps.table.rows}

\title{ Table Rows }
\usage{
rodps.table.rows( full.tablename , partition = NULL)
}
\description{
  get the number of rows
}
\arguments{
\item{ full.tablename }{ table name , in format 'ProjectName.TableName',or 'TableName'(using current project) }.
\item{ partition }{ partition spec}
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## get the number of rows
\dontrun{rodps.table.rows('sales')}
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
