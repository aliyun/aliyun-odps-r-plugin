#' Sample table
#'
#' @seealso \code{\link{rodps.table.sample.strat}}
#' @export
rodps.table.sample.srs <- function(srctable, tgttable, samplerate, cond = NULL, select = NULL) {
  rv <- round(runif(3) * 100)
  
  .check.tablename(srctable)
  .check.tablename(tgttable)
  if (!is.numeric(samplerate)) {
    stop("samplerate should be numeric")
  }
  
  if (is.null(select)) {
    sel = "*"
  } else {
    if (!is.character(select)) {
      stop("Select should be character")
    }
    sel = paste(select, sep = ",", collapse = ",")
  }
  
  if (!rodps.table.exist(srctable)) {
    stop(paste("Table not exists ", srctable))
  }
  if (rodps.table.exist(tgttable)) {
    stop(paste("Target table already exists", tgttable))
  }
  
  sql <- sprintf(" SELECT %s FROM %s", sel, srctable)
  if (!is.null(cond)) {
    if (!is.character(cond)) {
      stop("Invalid condition expression")
    } else {
      sql <- paste(sql, " WHERE ", cond)
    }
  }
  
  distby = sprintf(" DISTRIBUTE BY rand(%d)*10 SORT BY rand(%d)", rv[1], rv[2])
  if (samplerate < 1) {
    # sample by percentage
    sql <- paste(sql, distby)
    sql <- paste(" CREATE TABLE ", tgttable, "  AS \n SELECT * FROM (", sql,
                 " ) sub \n WHERE rand(", rv[3], ")<= ", samplerate)
  } else {
    # sample by abs value
    sql <- paste(" CREATE TABLE ", tgttable, " AS  \n SELECT * FROM ( ", sql,
                 distby, " ) sub \n LIMIT ", samplerate)
  }
  
  ret <- try(rodps.sql(sql))
  if ("try-error" %in% class(ret)) {
    cat("Exception occurred when executing sql \n")
    cat(sql)
    cat("\n")
    return(FALSE)
  }
  return(T)
}

#' @rdname rodps.table.sample.srs
#' @export
rodps.sample.srs <- rodps.table.sample.srs

#' Sample Table
#'
#' The sample strategy is as similar as:
#'
#' select abc from ( *, row_number() over( partition by g order by
#' rand()) r_rn, rand() as r_select ) sub
#' 1. by percent sub where r_select < rate
#' 2. by number sub where rn <= rate
#'
#' @seealso \code{\link{rodps.table.sample.srs}}
#' @export
rodps.table.sample.strat <- function(srctable, tgttable, samplerate, strat, select = NULL) {
  .check.tablename(srctable)
  .check.tablename(tgttable)
  
  if (!is.numeric(samplerate)) {
    stop("sample rate should be numeric ")
  }
  if (!is.character(strat)) {
    stop("strat should be character")
  }
  if (!is.null(select) && !is.character(select)) {
    stop("select should be character")
  }
  if (rodps.table.exist(tgttable)) {
    stop("target table already exists")
  }
  
  rv <- round(runif(3) * 100)
  if (is.null(select)) {
    des <- rodps.table.desc(srctable)
    cols <- paste(des$columns$names, collapse = ",")
  } else {
    cols <- paste(select, collapse = ",")
  }
  pcols <- paste(strat, collapse = ",")
  
  temp <- "CREATE TABLE %s AS \n SELECT %s FROM ( \n SELECT %s, \n row_number() OVER (PARTITION BY %s ORDER BY rand(%d)) sel_rownumber, \n rand(%d) sel_random  FROM %s) sub"
  sql <- sprintf(temp, tgttable, cols, cols, pcols, rv[1], rv[2], srctable)
  
  if (samplerate < 1) {
    sql <- paste(sql, " WHERE sel_random <= ", samplerate)
  } else {
    sql <- paste(sql, " WHERE sel_rownumber <= ", samplerate)
  }
  
  ret <- try(rodps.sql(sql))
  if ("try-error" %in% class(ret)) {
    cat("Exception occurred when executing sql\n")
    cat(sql)
    cat("\n")
  }
  return(TRUE)
}

#' @rdname rodps.table.sample.strat
#' @export
rodps.sample.strat <- rodps.table.sample.strat