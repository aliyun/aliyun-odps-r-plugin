\name{rodps.table.exist}
\alias{RODPS.TABLE.EXIST}
\alias{rodps.table.exist}
\title{ Table Exist }
\usage{
rodps.table.exist( full.tablename , partition=NULL)
}
\description{
  query whether the table exists
}
\arguments{
 \item{ full.tablename }{ table name }
 \item{ partition }{ partition spec }
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{

\dontrun{rodps.table.exist('mytable')}
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
