\name{rodps.table.read}
\alias{RODPS.TABLE.READ}
\alias{rodps.table.read}
\title{ Read Table }
\usage{
rodps.table.read( full.tablename,partition = NULL,limit=-1,memsize=10737518240,isdebug = FALSE, thread = 8)
}
\description{
 read data from ODPS and store in R.
}
\arguments{
\item{ full.tablename }{ table name }
\item{ partition }{ partition name }
\item{ limit }{ limit the rows to read,'-1' for not limit }
\item{ memsize }{ Maximum data capacity }
\item{ isdebug }{ is debug }
\item{ thread }{ thread number } 
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
\dontrun{ x<-rodps.table.read('sales',limit=100) }
}
\seealso{
 \code{\link{rodps.table.desc}},
 \code{\link{rodps.table.drop}},
 \code{\link{rodps.table.exist}},
 \code{\link{rodps.table.partitions}},
 \code{\link{rodps.table.list}},
 \code{\link{rodps.table.rows}},
 \code{\link{rodps.table.size}},
 \code{\link{rodps.table.write}}
}
\keyword{programming}
