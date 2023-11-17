
#' @title RODPS: R interface to interact with ODPS
#'
#' @description RODPS is an R extension to enable R to interact with ODPS
#'   system, also support other related algorithm packages.
#'
#' @name RODPS
#' @docType package
#' @concept RODPS
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @details The RODPS package supplies functions to interact with ODPS from
#'   within R. There are functions for exporting and connecting as well as
#'   querying ODPS. Please make sure the environment variable ODPS_CONFIG is set,
#'   it's in the same format as used in odpscmd, this file is required when
#'   connecting to ODPS.
#'
#' @seealso  \code{\link{rodps.sql}}, \code{\link{rodps.set}},
#'   \code{\link{rodps.table}},\code{\link{rodps.project}}
#' 
#' @import rJava
#' @importFrom stats df
#' @importFrom stats runif
#' @importFrom utils read.table
#' @importFrom stats na.omit
NULL
