#' Show help information
#'
#' @return NULL
#' @export
rodps.help <- function() {
    cat("Please try help(rodps) \n")
}

#' Set business ID
#'
#' @param bizid business id, e.g. 012345^.
#' @author \email{ruibo.lirb@alibaba-inc.com}
#' @seealso \code{\link{RODPS}}, \code{\link{rodps.sql}}
#' @examples
#' ## set business id to 012345
#' \dontrun{rodps.bizid('012345^')}
#' @export
rodps.bizid <- function(bizid) {
    .check.init()
    odpsOperator$setBizId(bizid)
}

#' Set task properties
#'
#' Set properties for SQL task
#'
#' @param key setting name, e.g. odps.sql.allow.fullscan.
#' @param value setting value.
#' @author \email{ruibo.lirb@alibaba-inc.com}
#' @seealso  \code{\link{RODPS}}, \code{\link{rodps.sql}},
#'   \code{\link{rodps.unset}},
#' @examples
#' ## enable full table scan
#' \dontrun{rodps.set('odps.sql.allow.fullscan', 'true')}
#' @export
rodps.set <- function(key, value) {
    .check.init()
    odpsOperator$set(key, value)
}

#' Unset task properties
#'
#' Unset properties for SQL task
#'
#' @param key setting name, e.g. odps.sql.allow.fullscan.
#' @author \email{ruibo.lirb@alibaba-inc.com}
#' @seealso  \code{\link{RODPS}}, \code{\link{rodps.sql}},
#'   \code{\link{rodps.set}},
#' @examples
#' ## set full table scan to its default value
#' \dontrun{rodps.unset('odps.sql.allow.fullscan')}
#' @export
rodps.unset <- function(key) {
    .check.init()
    odpsOperator$unset(key)
}

.check.init <- function() {
    if (length(ls(envir = .GlobalEnv, pattern = "odpsOperator")) == 0 || is.null(odpsOperator)) {
        stop(print("RODPS uninitialized or session timeout, please exectue rodps.init(path), path for the path of odps_config.ini"))
    }
}

.dataframe.to.sqlite <- function(dataframe, thread, filename, tablename, isdebug) {
    if (!require(DBI, quietly = TRUE)) {
        stop("DBI library not available")
    }
    if (!require(RSQLite, quietly = TRUE)) {
        stop("RSQLite library not available")
    }
    dl <- list()
    recordNumPerThread <- nrow(dataframe)%/%thread
    dbNames = c()
    for (i in (0:(thread - 1))) {
        startPos <- i * recordNumPerThread + 1
        endPos <- startPos + recordNumPerThread - 1
        if (i == thread - 1) {
            endPos <- nrow(dataframe)
        }
        dl[[length(dl) + 1]] <- as.data.frame(dataframe[startPos:endPos, ])
    }

    for (i in (1:length(dl))) {
        dbName <- paste(filename, "_", i - 1, sep = "")
        if (file.exists(dbName)) {
            print(paste("warning:upload middle file", dbName, "already exist, now delete it."))
            file.remove(dbName)
        }

        con <- dbConnect(SQLite(), dbname = dbName)
        dbWriteTable(con, tablename, dl[[i]], row.names = FALSE)
        if (isdebug) {
            print(paste("write file", i, ":", dbName))
        }
        dbDisconnect(con)
        dbNames <- append(dbNames, dbName)
    }
    return(dbNames)
}

.sqlite.to.dataframe <- function(dbs, coltype, tablename, isdebug) {
    if (!require(DBI, quietly = TRUE)) {
        stop("DBI library not available")
    }
    if (!require(RSQLite, quietly = TRUE)) {
        stop("RSQLite library not available")
    }

    filenum <- dbs$size()
    if (filenum == 0) {
        stop("Internal error: no middle file return.")
    }
    data <- data.frame()
    for (i in 0:(filenum - 1)) {
        filename <- dbs$get(i)
        if (!file.exists(filename)) {
            stop(paste("file not exists:", filename))
        }
        con <- dbConnect(SQLite(), dbname = filename)
        sql = paste("select * from [", tablename, "]", sep = "")
        tmp_data <- dbGetQuery(con, sql)
        data <- rbind(data, tmp_data)
        dbDisconnect(con)
        if (isdebug) {
            print(paste("download temp file:", filename))
        } else {
            file.remove(filename)
        }
    }

    for (i in 0:(coltype$size() - 1)) {
        if (coltype$get(i) == "datetime" || coltype$get(i) == "date" || coltype$get(i) ==
            "timestamp") {
            data[[i + 1]] = as.POSIXct(as.POSIXlt(data[[i + 1]], origin = "1970-01-01"))
        } else if (coltype$get(i) == "boolean") {
            data[[i + 1]] = as.logical(data[[i + 1]])
        } else if (coltype$get(i) == "decimal") {
            data[[i + 1]] = as.numeric(data[[i + 1]])
        }
    }
    return(data)
}

.data.frame.get.namelist <- function(dataframe) {
    if (!is.data.frame(dataframe))
        stop("input data is not data frame")
    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum) {
        retlist$add(names(dataframe)[i])
    }
    return(retlist)
}

.data.frame.get.typelist <- function(dataframe) {
    if (!is.data.frame(dataframe))
        stop("input data is not data frame")
    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum) {
        retlist$add(.get.object.type(dataframe[[i]]))
    }
    return(retlist)
}

.data.frame.to.arraylist <- function(dataframe) {
    if (!is.data.frame(dataframe))
        stop("input data is not data frame")

    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum) {
        name <- names(dataframe)[i]
        type <- .get.object.type(dataframe[[i]])
        dataframeitem <- .jnew("com/aliyun/openservices/odps/roperator/DataFrameItem",
            name, type)
        for (j in 1:length(dataframe[[i]])) {
            dataframeitem$getData()$add(as.character(dataframe[[i]][j]))
        }
        retlist$add(dataframeitem)
    }
    return(retlist)
}

.get.object.type <- function(obj) {
    type <- is(obj)[1]
    return(rodps.type.r2java[type])
}

.dataframe.code.conv <- function(dataframe, fromcode, tocode) {
    collen <- ncol(dataframe)
    for (i in 1:collen) {
        type <- is(dataframe[[i]])[1]
        if (type == "character" || type == "factor") {
            dataframe[[i]] = iconv(dataframe[[i]], fromcode, tocode)
        }
    }
    return(dataframe)
}

.check.tablename <- function(tablename) {
    if (is.null(tablename) || tablename == "") {
        stop(error("invalid_value", "table name is null"))
    }
    if (!is.character(tablename)) {
        stop(error("argument_type_error", "tablename must be string type."))
    }
}

.change.data <- function(ret) {
    rdata <- list()
    if (!is.null(ret) && ret$size() > 0) {
        data <- .jcast(ret, new.class = "java/util/List", check = FALSE, convert.array = FALSE)
        vlist <- c(0:(data$size() - 1))
        for (pos in vlist) {
            dfitem <- data$get(as.integer(pos))
            rdata[[pos + 1]] <- .change.type(dfitem$getType())
            names(rdata)[pos + 1] <- dfitem$getName()
            values <- dfitem$getData()
            if (values$size() > 0) {
                for (i in (0:(values$size() - 1))) {
                  v <- values$get(as.integer(i))
                  if (is.null(v)) {
                    v <- NA
                  }
                  rdata[[pos + 1]][i + 1] <- .change.value(dfitem$getType(), v)
                }
            }
        }
    }
    return(as.data.frame(rdata, stringsAsFactors = FALSE))
}

.change.value <- function(type, value) {
    type <- tolower(type)
    if (is.null(type)) {
        return(as.character(value))
    }
    rtype <- rodps.type.java2r[type]
    if (!is.null(rtype) && rtype != "character") {
        return(eval(parse(text = paste("as.", rtype, "('", value, "')", sep = ""))))
    } else {
        return(as.character(value))
    }
}

.change.type <- function(type) {
    type <- tolower(type)
    rtype <- rodps.type.java2r[type]
    if (is.null(rtype)) {
        return(character())
    }
    if (type == "datetime") {
        return(Sys.time())
    }
    if (type == "date") {
        return(date())
    }
    return(eval(parse(text = paste(rtype, "()", sep = ""))))
}


.change.to.list <- function(ret) {
    lst <- list()
    if (!is.null(ret)) {
        data <- .jcast(ret, new.class = "java/util/List", check = FALSE, convert.array = FALSE)
        if (!data$isEmpty()) {
            vlist <- c(0:(data$size() - 1))
            for (pos in vlist) {
                dfitem <- data$get(as.integer(pos))
                values <- dfitem$getData()
                if (values$size() > 1) {
                  vs <- c()
                  for (i in (0:(values$size() - 1))) {
                    v <- values$get(as.integer(i))
                    if (is.null(v)) {
                      vs[i + 1] <- NA
                    } else {
                      vs[i + 1] <- .change.value(dfitem$getType(), v)
                    }
                  }
                  lst[[dfitem$getName()]] <- vs
                } else {
                  v <- values$get(as.integer(0))
                  if (is.null(v)) {
                    lst[[dfitem$getName()]] <- NA
                  } else {
                    lst[[dfitem$getName()]] <- .change.value(dfitem$getType(), values$get(as.integer(0)))
                  }
                }
            }
        }
    }
    return(lst)
}

# 将java中返回的List<DataFrameItem>转化成对象
.change.to.obj <- function(ret) {
    obj <- "object"
    if (!is.null(ret)) {
        data <- .jcast(ret, new.class = "java/util/List", check = FALSE, convert.array = FALSE)
        if (!data$isEmpty()) {
            vlist <- c(0:(data$size() - 1))
            for (pos in vlist) {
                dfitem <- data$get(as.integer(pos))
                values <- dfitem$getData()
                if (values$size() > 1) {
                  vs <- c()
                  for (i in (0:(values$size() - 1))) {
                    v <- values$get(as.integer(i))
                    if (is.null(v)) {
                      vs[i + 1] <- NA
                    } else {
                      vs[i + 1] <- .change.value(dfitem$getType(), v)
                    }
                  }
                  attr(obj, dfitem$getName()) <- vs
                } else {
                  v <- values$get(as.integer(0))
                  if (is.null(v)) {
                    attr(obj, dfitem$getname()) <- NA
                  } else {
                    attr(obj, dfitem$getName()) <- .change.value(dfitem$getType(),
                      values$get(as.integer(0)))
                  }
                }
            }
        }
    }
    return(obj)
}
