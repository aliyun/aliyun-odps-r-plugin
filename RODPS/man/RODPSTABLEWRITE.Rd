\name{rodps.table.write}
\alias{RODPS.TABLE.WRITE}
\alias{rodps.table.write}

\title{ Write Table }
\usage{
rodps.table.write( dataframe,full.tablename,partition=NULL,tablecomment=NULL,isdebug = FALSE, thread = 8)
}
\description{
 write 'dataframe' into 'full.tablename' of ODPS, make sure the target table 'full.tablename' is not exist. 
}
\arguments{
\item{ dataframe }{ data in data.frame type,make sure the ColumnName is allowable in ODPS.}.
\item{ full.tablename }{ table name , in format 'ProjectName.TableName',or 'TableName'(using current project)}.
\item{ partition }{ target partitin }
\item{ tablecomment }{ table comment }
\item{ isdebug }{ isdebug }
\item{ thread }{ thread}
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## write data.frame into 'mytable'
\dontrun{ x<-data.frame(c1=1:10,c2=1:10)}
\dontrun{ rodps.table.write(x,'mytable')}
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
