.rodps.bigSql <- function(query, mcqa = FALSE, memsize = 10737518240, thread = 8) {
    .check.init()
    if (is.null(query) || query == "") {
        stop(error("input_query_error", "query is null"))
    }
    postfix <- paste(sample(c(letters[1:6], 0:9), 30, replace = TRUE), collapse = "")
    tmptable <- paste("rodps_result_", postfix, sep = "")
    query <- paste("CREATE TABLE ", tmptable, " LIFECYCLE 3 AS ", query, sep = "")

    odpsOperator$runSqlTask(query, mcqa)
    length <- rodps.table.size(tmptable)

    if (length > memsize) {
        x <- tmptable
        attr(x, "result:size") <- length
        return(x)
    } else {
        result <- try(rodps.table.read(tmptable, memsize = memsize, thread = thread))
        odpsOperator$runSqlTask(paste("DROP TABLE ", tmptable, sep = ""), mcqa)
        if ("try-error" == class(result)) {
            stop(paste("Exception ocurred when loading table:", tmptable, sep = ""))
        } else {
            return(result)
        }
    }
}

#' SQL Command
#'
#' Run SQL command and return result(in data.frame type).
#'
#' @param query SQL string
#' @param mcqa Whether enable MCQA or not
#' @param result.table.limit The size limit of resulted table as engine side table or fetched data frame.
#' @param thread The threading number to read table data when the table size is larger than `result.table.limit`.
#' @author \email{yunyuan.zhangyy@alibaba-inc.com}
#' @seealso   \code{\link{RODPS}}, \code{\link{rodps.table}},
#'   \code{\link{rodps.project}}
#' @examples
#' ## select the data of 'sales' in January ,and store the result in data.frame
#' \dontrun{ data <- rodps.sql('select * from sales where month=1')}
#' @export
rodps.sql <- function(query, mcqa = FALSE, result.table.limit = 10737518240, thread = 8) {
    .check.init()
    if (is.null(query) || query == "") {
        stop(error("input_query_error", "query is null"))
    }
    type <- blacklist(query)
    if (as.logical(type) && type == TRUE) {
        stop(error("input_query_error", paste("rodps.sql does not support '", query,
            "' command", sep = "")))
    }

    # set odps.instance.priority
    if (grepl("set", query) && grepl("odps.instance.priority", query) && grepl("[",
        query, fixed = TRUE) && grepl("]", query, fixed = TRUE)) {
        query_1 <- strsplit(query, "[", fixed = TRUE)
        query_1[[1]][1]
        query_1[[1]][2]
        query_2 <- strsplit(query_1[[1]][2], "]", fixed = TRUE)
        if (!is.na(query_2[[1]][2])) {
            query <- paste(query_1[[1]][1], query_2[[1]][2], sep = "")
        } else {
            query <- query_1[[1]][1]
        }
        if (nchar(query_2) > 0) {
            odpsOperator$runSqlTask(query_2[[1]], mcqa)
        }
        if (nchar(query) < 1) {
            return(TRUE)
        }
    }

    if (type == "select") {
        return(.rodps.bigSql(query, mcqa = mcqa, memsize = result.table.limit, thread = thread))
    }
    ret <- odpsOperator$runSqlTask(query, mcqa)
    if (is.null(ret) || ret$size() == 0) {
        return(NULL)
    }
    if (ret$size() == 1) {
        return(strsplit(ret$get(as.integer(0)), "\n"))
    }
    vlist <- list()
    for (i in c(0:(ret$size() - 1))) {
        vlist[i + 1] <- ret$get(as.integer(i))
    }
    return(vlist)
}

#' @rdname rodps.sql
#' @export
rodps.query <- rodps.sql

# 不支持运行的query
blacklist <- function(query) {
    tokens <- strsplit(tolower(query), "\\s+", fixed = FALSE)
    if (length(tokens[[1]]) == 0) {
        return(FALSE)
    }
    if (length(tokens[[1]]) >= 2 && tokens[[1]][1] == "") {
        head <- tokens[[1]][2]
    } else {
        head <- tokens[[1]][1]
    }
    if (head == "use" || head == "read") {
        return(TRUE)
    }
    return(head)
}
