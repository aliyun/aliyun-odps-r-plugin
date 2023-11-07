#' @name rodps.project
#' @title Project functions
#' @description Provide functions to operate project.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @seealso  \code{\link{rodps.project.use}},
#'   \code{\link{rodps.project.current}}
NULL

#' Change current project.
#'
#' @param projectname target projectname; make sure that you have the authority to access this Project.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @seealso \code{\link{rodps.project.current}}
#' @examples
#' ## change project to prjb
#' \dontrun{rodps.project.use('prjb')}
#' @export
rodps.project.use <- function(projectname) {
    .check.init()
    if (is.null(projectname) || projectname == "") {
        stop(error("invalid_project_name"))
    }
    odpsOperator$useProject(projectname)
}

#' Get current project name.
#'
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @seealso \code{\link{rodps.project.use}}
#' @examples
#' ## get current project name
#' \dontrun{rodps.project.current()}
#' @export
rodps.project.current <- function() {
    .check.init()
    return(odpsOperator$getProjectName(""))
}

#' Get current project name.
#' 
#' @seealso \code{\link{rodps.project.current}}
#' @export
rodps.current.project <- rodps.project.current
