\name{RODPS}
\alias{rodps}
\alias{RODPS}
\docType{package}
\title{RODPS: R interface to interace with ODPS}
\description{
    RODPS is an R extension to enable R to interact with ODPS system, 
    also support other related algorithm packages.
}
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\details{
  The RODPS package supplies functions to interact
  with ODPS from within R. 
  There are functions for exporting and connecting
  as well as querying ODPS
  Please make sure the environment variable ODPS_CONFIG is set,
  it's in the same format as used in odpscmd, this file is required when connecting to ODPS.
} 
\seealso{
 \code{\link{rodps.sql}},
 \code{\link{rodps.table}},
 \code{\link{rodps.project}}
}
\keyword{programming}
\concept{RODPS}
