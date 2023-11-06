\name{rodps.unset}
\alias{RODPS.UNSET}
\alias{rodps.unset}
\title{ unset properties }
\usage{
rodps.unset( key )
}
\description{
  unset properties for sql task
}
\arguments{
\item{ key }{ key, ex. odps.sql.allow.fullscan }
}
\author{
\email{ruibo.lirb@alibaba-inc.com}
}
\examples{
## set full table scan to its default value
\dontrun{rodps.unset('odps.sql.allow.fullscan')}
}
\seealso{
 \code{\link{RODPS}},
 \code{\link{rodps.sql}},
 \code{\link{rodps.set}},
}
\keyword{programming}
