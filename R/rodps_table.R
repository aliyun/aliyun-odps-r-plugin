#' @name rodps.table
#' @title RODPS Table Functions
#' @description Provide functions to operate table.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @seealso \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#' \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#' \code{\link{rodps.table.list}}, \code{\link{rodps.table.rows}},
#' \code{\link{rodps.table.size}}, \code{\link{rodps.table.read}},
#' \code{\link{rodps.table.write}}
NULL

#' Table Head
#' 
#' Create odps.data and odps.vector in S4.
#' Store the head result in a temp table
#'
#' @export
head.rodps.data <- function(rd, n = 6L) {
    rodps.table.head(rd@tbl, n)
}

#' Table Head
#'
#' Show a few of head rows of table.
#'
#' @param tbl Table name
#' @param n The number of head rows
#' @export
rodps.table.head <- function(tbl, n = 6L) {
    # could be optimized by identify the tbl/partiton tbl/view
    sql <- sprintf("select * from %s limit %d;", tbl, n)
    df <- rodps.sql(sql)
    df
}

#' Split full table name into table name and project name
#' @param ftn Full table name.
#' @export
rodps.split.ftn <- function(ftn) {
    if (is.null(ftn) || !is.character(ftn) || nchar(ftn) == 0 || length(ftn) > 1) {
        stop("Invalid table name ")
    }
    p.t <- unlist(strsplit(ftn, "[.]"))
    if (length(p.t) > 2 || length(p.t) < 1) {
        stop("Invalid table name ")
    }
    ret <- list()
    if (length(p.t) == 1) {
        ret$tablename <- p.t[1]
    } else {
        ret$projectname <- p.t[1]
        ret$tablename <- p.t[2]
    }
    return(ret)
}

#' Table Existence
#'
#' Check whether a table exists.
#'
#' @param full.tablename table name.
#' @param partition partition spec, default NULL.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' \dontrun{rodps.table.exist('mytable')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.partitions}}, \code{\link{rodps.table.list}},
#'   \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.exist <- function(full.tablename, partition = NULL) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    if (is.null(projectname)) {
        projectname <- rodps.project.current()
    }

    .check.tablename(tablename)
    tableExist <- odpsOperator$isTableExist(.jnew("java/lang/String", projectname),
        tablename, partition)
    return(tableExist)
}

#' @rdname rodps.table.exist
#' @export
rodps.exist.table <- rodps.table.exist

#' List Tables
#'
#' List all tables in the project, default in current project.
#'
#' @param pattern Partition pattern, use '*' or specific PartitionName.
#' @param projectname Specific project to query,default is current project.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ##list the tables in current project
#' \dontrun{rodps.table.list()}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#'   \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.list <- function(pattern = NULL, projectname = NULL) {
    .check.init()

    if (is.null(projectname)) {
        projectname <- rodps.project.current()
    } else {
        rodps.project.use(projectname)
    }

    tables <- try(odpsOperator$getTables(projectname, pattern))
    if ("try-error" %in% class(tables)) {
        stop("Exception occured when listing tables")
    }
    return(.change.data(tables))
}

#' @rdname rodps.table.list
#' @export
rodps.list.table <- rodps.table.list

#' @rdname rodps.table.list
#' @export
rodps.list.tables <- rodps.table.list

#' List Partitions
#'
#' List partitions of a table. Raise ERROR if the table has no partition.
#'
#' @param full.tablename, Table name, in format of 'ProjectName.TableName' or
#'   'TableName' (using current project).
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## list partitions of 'sales'
#' \dontrun{rodps.table.partitions('sales')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.list}},
#'   \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.partitions <- function(full.tablename) {
    .check.init()
    df <- rodps.sql(paste("show partitions", full.tablename))
    return(df)
}

#' @rdname rodps.table.partitions
#' @export
rodps.partitions.table <- rodps.table.partitions

#' Drop Table
#'
#' Delete table if it exists.
#'
#' @param full.tablename Table name.
#' @param partition Partition spec.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' \dontrun{rodps.table.drop('sales_backup')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.exist}},
#'   \code{\link{rodps.table.partitions}}, \code{\link{rodps.table.list}},
#'   \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.drop <- function(full.tablename, partition = NULL) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)

    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    if (!is.null(projectname)) {
        ftn <- paste(projectname, ".", tablename, sep = "")
    } else {
        ftn <- tablename
    }
    if (is.null(partition)) {
        sql <- paste("drop table if exists", ftn)
    } else {
        sql <- paste("alter table", ftn, "drop partition(", partition, ")")
    }
    rodps.sql(sql)
    return(TRUE)
}

#' @rdname rodps.table.drop
#' @export
rodps.drop.table <- rodps.table.drop

#' Convert pt|string| into dataframe
#' @noRd
.column.to.dataframe <- function(cols) {
    len <- length(cols)
    names <- c()
    types <- c()
    comments <- c()
    for (i in 1:len) {
        items <- strsplit(cols[i], "|", fixed = TRUE)
        names[i] <- items[[1]][1]
        types[i] <- items[[1]][2]
        if (length(items[[1]]) > 2) {
            comments[i] <- items[[1]][3]
        } else {
            comments[i] <- NA
        }
    }
    return(data.frame(names, types, comments, stringsAsFactors = FALSE))
}

#' Table Description
#'
#' Show description of a table, including metadata of 
#' Owner, Project, Comment, Create_time, Last_modified_time, Size, Columns.
#'
#' @param full.tablename Table name, in format 'ProjectName.TableName',or
#'   'TableName' (using current project).
#' @param partition Partition spec
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## show description of 'dual'
#' \dontrun{rodps.table.desc('dual')}
#' @seealso \code{\link{rodps.table.drop}}, \code{\link{rodps.table.exist}},
#'   \code{\link{rodps.table.partitions}}, \code{\link{rodps.table.list}},
#'   \code{\link{rodps.table.rows}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.desc <- function(full.tablename, partition = NULL) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    if (is.null(projectname)) {
        projectname <- rodps.project.current()
    }

    .check.tablename(tablename)
    tableMeta <- odpsOperator$describeTable(.jnew("java/lang/String", projectname),
        tablename, partition)
    ret <- .change.to.list(tableMeta)
    ret$columns = .column.to.dataframe(ret$columns)
    if (length(ret$partition_keys) > 0) {
        ret$partition_keys <- .column.to.dataframe(ret$partition_keys)
    }
    if ("windows" == .Platform$OS.type) {
        ret$comment <- iconv(ret$comment, "utf-8", "gbk")
        ret$columns$comments <- iconv(ret$columns$comments, "utf-8", "gbk")
    }
    return(ret)
}

#' @rdname rodps.table.desc
#' @export
rodps.desc.table <- rodps.table.desc

#' Table Size
#'
#' Get the size of table in Bytes.
#'
#' @param full.tablename Table name, in format 'ProjectName.TableName',or
#'   'TableName' (using current project).
#' @param partition Partition spec
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## get the size of 'sales'
#' \dontrun{rodps.table.size('sales')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#'   \code{\link{rodps.table.list}}, \code{\link{rodps.table.rows}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.size <- function(full.tablename, partition = NULL) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    size <- odpsOperator$getTableSize(.jnew("java/lang/String", projectname), tablename,
        partition)

    return(size)
}

#' @rdname rodps.table.size
#' @export
rodps.size.table <- rodps.table.size

#' @noRd
.check.column.name <- function(colname) {
    if (length(grep("[.]|[$]", colname)) > 0 || nchar(colname) > 128 || substr(colname,
        1, 1) == "_")
        stop(paste("Invalid column name", colname))
}

#' @noRd
.rodps.generate.DDL <- function(full.tablename, dataframe, tablecomment = NULL) {
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    if (!is.data.frame(dataframe)) {
        stop("dataframe should be data.frame type")
    }

    namelist <- names(dataframe)
    for (n in namelist) .check.column.name(n)

    typelist <- sapply(dataframe, .get.object.type)

    sql <- paste(" CREATE TABLE ", full.tablename, " (\n", sep = "")
    ncol <- length(namelist)
    ntype <- length(typelist)

    for (i in seq(1, ncol)) {
        if (i != ncol) {
            sql <- paste(sql, " ", namelist[i], "\t", typelist[i], ",\n", sep = " ")
        } else {
            sql <- paste(sql, " ", namelist[i], "\t", typelist[i], ")", sep = " ")
        }
    }
    if (!is.null(tablecomment)) {
        sql <- paste(sql, "\n COMMENT '", tablecomment, "'")
    }
    return(sql)
}

#' Write Table
#'
#' Write 'dataframe' into 'full.tablename' of ODPS, make sure the target table
#' 'full.tablename' is not exist. Dataframe can be written to a non-exist table
#' or partition.
#'
#' @param dataframe Data in data.frame type, make sure the ColumnName is
#'   allowable in ODPS.
#' @param full.tablename Table name, in format 'ProjectName.TableName',or
#'   'TableName' (using current project).
#' @param partition Partition spec.
#' @param tablecomment Table comment.
#' @param isdebug Boolean value, if debugging is enabled.
#' @param thread Thread number.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## write data.frame into 'mytable'
#' \dontrun{ x<-data.frame(c1=1:10,c2=1:10)}
#' \dontrun{ rodps.table.write(x,'mytable')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#'   \code{\link{rodps.table.list}}, \code{\link{rodps.table.rows}},
#'   \code{\link{rodps.table.size}}, \code{\link{rodps.table.read}}
#' @export
rodps.table.write <- function(dataframe, full.tablename, partition = NULL, tablecomment = NULL,
    isdebug = FALSE, thread = 8) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    if (is.null(projectname)) {
        projectname <- rodps.project.current()
    }

    .check.tablename(tablename)
    if (!is.data.frame(dataframe)) {
        stop("dataframe should be class of data.frame")
    }

    if (length(colnames(dataframe)) == 0) {
        stop("dataframe should have as least one column")
    }

    if (!is.null(partition) && !rodps.table.exist(full.tablename)) {
        stop(sprintf("Table not exists,table=%s partition=%s", full.tablename, partition))
    }
    sql <- NULL
    if (!rodps.table.exist(full.tablename)) {
        sql <- .rodps.generate.DDL(full.tablename, dataframe, tablecomment)
    }
    if (!is.null(partition) && !rodps.table.exist(full.tablename, partition)) {
        sql <- paste("alter table", full.tablename, "add partition(", odpsOperator$formatPartition(partition,
            "'", ","), ")")
    }
    if (!is.null(sql)) {
        ret <- try(rodps.sql(sql))
        if ("try-error" %in% class(ret)) {
            cat("Exception occured when creating table\n")
            cat(sql)
            cat("\n")
        }
    }

    if (nrow(dataframe) == 0) {
        return(TRUE)
    }

    tempprefix <- paste("rodps", ceiling(runif(1, 1, 1e+06)), "_", sep = "")
    filename <- tempfile(tempprefix, rodpsTmpdir)

    actual_thread <- as.integer(thread)
    if (nrow(dataframe) < thread * 100) {
        actual_thread <- as.integer(1)
    }
    if ("windows" == .Platform$OS.type) {
        dataframe <- .dataframe.code.conv(dataframe, "", "UTF-8")
    }
    dbNames <- .dataframe.to.sqlite(dataframe, actual_thread, filename, tablename,
        isdebug)
    odpsOperator$writeTableFromDT(projectname, tablename, partition, filename, NULL,
        NULL, .jlong(length(dataframe[[1]])), actual_thread)
    if (!isdebug) {
        for (i in 1:length(dbNames)) {
            file.remove(dbNames[i])
        }
    }
    return(TRUE)
}

#' @rdname rodps.table.write
#' @export
rodps.write.table <- rodps.table.write

#' Reading Table
#'
#' Read data from ODPS and store in R data frame.
#'
#' @param full.tablename Table name
#' @param partition Partition spec
#' @param limit Limit the rows to read, '-1' for not limit.
#' @param memsize Maximum data capacity.
#' @param isdebug Boolean value, if debugging is enabled.
#' @param thread Thread number.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## show description of 'dual'
#' \dontrun{ x<-rodps.table.read('sales',partition='ds=20180124',limit=100) }
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#'   \code{\link{rodps.table.list}}, \code{\link{rodps.table.rows}},
#'   \code{\link{rodps.table.size}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.read <- function(full.tablename, partition = NULL, limit = -1, memsize = 10737518240,
    isdebug = FALSE, thread = 8) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    tablesize <- rodps.table.size(full.tablename, partition = partition)
    if ((tablesize > memsize) && (limit == -1)) {
        msg <- paste("whole table size (", tablesize, ") is larger than memsize (",
            memsize, "), can not be loaded.")
        stop(msg)
    }

    tempprefix <- paste("rodps", ceiling(runif(1, 1, 1e+05)), "_", sep = "")
    filename <- tempfile(tempprefix, rodpsTmpdir)

    results <- odpsOperator$loadTableFromDT(projectname, tablename, partition, filename,
        NULL, NULL, as.integer(limit), as.integer(thread))

    if (3 != results$size()) {
        stop("Internal error with load table")
    }
    res <- .sqlite.to.dataframe(results$get(as.integer(2)), results$get(as.integer(1)),
        tablename, isdebug)
    if ("windows" == .Platform$OS.type) {
        res <- .dataframe.code.conv(res, "UTF-8", "")
    }
    return(res)
}

#' @rdname rodps.table.read
#' @export
rodps.read.table <- rodps.table.read

#' @rdname rodps.table.read
#' @export
rodps.load.table <- rodps.table.read

#' Table Rows
#'
#' Get the number of rows in a table.
#'
#' @param full.tablename Table name , in format of 'ProjectName.TableName' or
#'   'TableName' (using current project)
#' @param partition Partition spec.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @examples
#' ## get the number of rows
#' \dontrun{rodps.table.rows('sales')}
#' @seealso   \code{\link{rodps.table.desc}}, \code{\link{rodps.table.drop}},
#'   \code{\link{rodps.table.exist}}, \code{\link{rodps.table.partitions}},
#'   \code{\link{rodps.table.list}}, \code{\link{rodps.table.size}},
#'   \code{\link{rodps.table.read}}, \code{\link{rodps.table.write}}
#' @export
rodps.table.rows <- function(full.tablename, partition = NULL) {
    .check.init()
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    sz <- rodps.table.size(full.tablename)

    if (sz < 10 * 1024 * 1024 * 1024 || !is.null(partition) && partition != "") {
        sql <- sprintf(" count %s ", full.tablename)
        if (!is.null(partition) && partition != "") {
            sql <- paste(sql, "partition(", partition, ")")
        }
        v <- rodps.sql(sql)
        ret <- as.numeric(v[[1]])
    } else {
        sql <- sprintf("select count(*) from %s", full.tablename)
        v <- rodps.sql(sql)
        ret <- as.numeric(v[1, 1])
    }
    return(ret)
}

#' @rdname rodps.table.rows
#' @export
rodps.rows.table <- rodps.table.rows
