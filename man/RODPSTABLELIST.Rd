\name{rodps.table.list}
\alias{RODPS.TABLE.LIST}
\alias{rodps.table.list}
\title{ List Table }
\usage{
rodps.table.list( pattern=NULL, projectname=NULL )
}
\description{
 list tables in the project,default in current project.
}
\arguments{
\item{ pattern }{ pattern, use '*' or specific PartitionName.}.
\item{ projectname }{ specific project to query,default is current project}.
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## list the tables in current project
\dontrun{rodps.table.list()}
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
