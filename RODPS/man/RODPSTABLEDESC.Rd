\name{rodps.table.desc}
\alias{RODPS.TABLE.DESC}
\alias{rodps.table.desc}
\title{ Table Description }
\usage{
rodps.table.desc( full.tablename , partition = NULL)
}
\description{
  show description of table, include Owner,Project,Comment,Create_time,Last_modified_time,Size,Columns.
}
\arguments{
\item{ full.tablename }{ table name, in format 'ProjectName.TableName',or 'TableName'(using current project).} 
\item{ partition }{ partition spec }
}
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## show description of 'dual'
\dontrun{rodps.table.desc('dual')}
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
