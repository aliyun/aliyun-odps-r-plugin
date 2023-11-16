#' Store unique result in a temp table
#' 
#' @export
summary.rodps.data <- function(rd) {
    rodps.table.summary(rd@tbl)
}

#' Table Summary
#'
#' Get statistical summaries of a table.
#' @export
rodps.table.summary <- function(tbl) {
    stblname <- paste("rodps_", paste(sample(c(letters[1:6], 0:9), 30, replace = TRUE),
        collapse = "", sep = ""), sep = "")
    rtblname <- paste("rodps_", paste(sample(c(letters[1:6], 0:9), 30, replace = TRUE),
        collapse = "", sep = ""), sep = "")

    des <- rodps.table.desc(tbl)
    rows <- rodps.table.rows(tbl)
    dblcols <- des$columns[which(des$columns$types == "double"), ]
    cols <- paste(which(des$columns$types == "double") - 1, collapse = ",")

    if (nrow(dblcols) > 0) {
        sql <- sprintf("sort_rank -i %s -o %s,%s -c %s ", tbl, stblname, rtblname,
            cols)
        rodps.sql(sql)

        sql <- sprintf("select colname, percentile,pctvalue from %s where percentile in (0.0, 25.0, 50.0, 75.0, 100.0);",
            rtblname)
        df <- rodps.sql(sql)

        rodps.table.drop(stblname)
        rodps.table.drop(rtblname)
    } else {
        df <- data.frame()
    }

    # get mean value
    meancols <- paste("avg(", des$columns[which(des$columns$types == "double"), ]$names,
        ")", sep = "", collapse = ",")
    nacols <- paste(" count(case when ", des$columns$names, " is null then 1 end)",
        sep = "", collapse = ",")
    if (nrow(dblcols) > 0) {
        sql <- sprintf("select %s , %s from %s;", meancols, nacols, tbl)
    } else {
        sql <- sprintf("select %s from %s;", nacols, tbl)
    }
    df2 <- rodps.sql(sql)

    # assemble output
    tmp <- list()
    allcols = des$columns
    dbli <- 0
    for (i in seq(1:nrow(allcols))) {
        coltype = allcols[i, ]$types
        if (coltype == "double") {
            dbli <- dbli + 1
            # min, 1st qu, median, mean, 3rd qu. max. na
            minv <- df[which(df$colname == allcols[i, ]$names & df$percentile ==
                0), ]$pctvalue
            fstqv <- df[which(df$colname == allcols[i, ]$names & df$percentile ==
                25), ]$pctvalue
            medianv <- df[which(df$colname == allcols[i, ]$names & df$percentile ==
                50), ]$pctvalue
            meanv <- df2[1, dbli]
            trdqv <- df[which(df$colname == allcols[i, ]$names & df$percentile ==
                75), ]$pctvalue
            maxv <- df[which(df$colname == allcols[i, ]$names & df$percentile ==
                100), ]$pctvalue
            nav <- df2[1, nrow(dblcols) + i]
            tmpcol <- c(sprintf("Min.   :%.2f", minv), sprintf("1st Qu.:%.2f", fstqv),
                sprintf("Median :%.2f", medianv), sprintf("Mean   :%.2f", meanv),
                sprintf("3rd Qu.:%.2f", trdqv), sprintf("Max.   :%.2f", maxv), ifelse(nav >
                  0, sprintf("NA's   : %d", nav), NA))
        } else {
            lenv <- sprintf("Length:%d", rows)
            classv <- sprintf("Class :%s", allcols[i, ]$types)
            nav <- df2[1, nrow(dblcols) + i]
            tmpcol <- c(lenv, classv, ifelse(nav > 0, sprintf("NA's   : %d", nav),
                NA), NA, NA, NA, NA)
        }
        tmp[[i]] <- tmpcol
    }

    tmp <- unlist(tmp)
    dim(tmp) <- c(7, nrow(allcols))
    dimnames(tmp)[[1]] <- rep("", 7)
    dimnames(tmp)[[2]] <- allcols$names
    class(tmp) <- "table"
    tmp
}
