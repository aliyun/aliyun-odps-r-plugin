\name{rodps.set}
\alias{RODPS.SET}
\alias{rodps.set}
\title{ set properties }
\usage{
rodps.set( key, value )
}
\description{
  set properties for sql task
}
\arguments{
\item{ key }{ key, ex. odps.sql.allow.fullscan }
\item{ value }{ value, ex. true }
}
\author{
\email{ruibo.lirb@alibaba-inc.com}
}
\examples{
## enable full table scan
\dontrun{rodps.set('odps.sql.allow.fullscan', 'true')}
}
\seealso{
 \code{\link{RODPS}},
 \code{\link{rodps.sql}},
 \code{\link{rodps.unset}},
}
\keyword{programming}
