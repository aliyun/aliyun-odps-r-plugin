\name{rodps.project.use}
\alias{RODPS.PROJECT.USE}
\alias{rodps.project.use}
\title{ Change Project}
\usage{
rodps.project.use( projectname )
}
\description{
 change current project.
}
\arguments{
\item{ projectname }{ project name, make sure that you have the authority to access this Project. }.
} 
\author{
\email{yunyuan.zhangyy@alibaba-inc.com}
}
\examples{
## change project to prjb
\dontrun{rodps.project.use('prjb')}
}
\seealso{
 \code{\link{rodps.project.current}}
}
\keyword{programming}
