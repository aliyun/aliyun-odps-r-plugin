#' Create odps.data and odps.vector in S4
NULL

#' @rdname rodps.table.hist
hist.rodps.vector <- function(ov) {
    rodps.table.hist(ov@tbl, ov@col)
}

#' Extend hist hist returns a list of breaks, counts, density, mids, xname,
#' equidist, class attr generator a sql select count( case when ... end) b1,
#' count(case when ...) ... from tbl; then plot with the list
#'
#' @export
rodps.table.hist <- function(tablename, colname, breaks = NULL, freq = TRUE, include.lowest = TRUE,
    right = TRUE, main = paste("Histogram of ", colname), xlab = colname, ...) {

    if (!is.null(breaks) && !is.numeric(breaks)) {
        stop("Invalid breaks")
    }

    sql <- paste("select count(*), min(", colname, "), ", " max(", colname, ") from ",
        tblname, ";", sep = " ")
    cat("\n")
    mm <- rodps.sql(sql)
    # nclass.Sturges
    nbin <- ceiling(log2(mm[1, 1]) + 1)
    if (is.null(breaks)) {
        breaks <- pretty(c(mm[1, 3], mm[1, 2]), n = nbin)
    } else {
        if (length(breaks) <= 1) {
            stop("Invalid breaks length")
        }
        if (breaks[1] > mm[1, 2] || breaks[length(breaks)] < mm[1, 3]) {
            stop("Invalid breaks range")
        }
    }

    breaks <- sort(breaks)

    sql <- "select "
    lb <- breaks[1:(length(breaks) - 1)]
    ub <- breaks[2:length(breaks)]
    if (right) {
        cnt <- paste("  count (case when ", colname, ">", lb, " and ", colname, "<=",
            ub, " then 1 end)", sep = "")
        cntb <- paste("  count (case when ", colname, ">=", lb[1], " and ", colname,
            "<=", ub[1], " then 1 end)", sep = "")
        cnt[1] <- cntb
    } else {
        cnt <- paste("  count (case when ", colname, ">=", lb, " and ", colname,
            "<", ub, " then 1 end)", sep = "")
        cnte <- paste("  count (case when ", colname, ">=", lb[length(cnt)], " and ",
            colname, "<=", ub[length(cnt)], " then 1 end)", sep = "")
        cnt[length(cnt)] <- cnte
    }

    cnt <- paste(cnt, collapse = ",\n")

    sql <- sprintf("%s \n%s \n from %s", sql, cnt, tblname)
    cat(sql)

    cnt <- rodps.sql(sql)
    cnt <- t(unlist(cnt[1, ]))
    mids <- (lb + ub)/2
    dens <- cnt/(mm[1, 1] * diff(breaks))
    equi <- diff(range(diff(breaks))) < 1e-07

    h <- list()
    h$breaks <- breaks
    h$counts <- cnt
    h$mids <- mids
    h$density <- dens
    h$xname <- colname
    h$equidist <- T
    class(h) <- "histogram"

    plot(h, freq = freq, main = main, xlab = xlab, ...)
    invisible(h)
}

#' @rdname rodps.table.hist
rodps.hist <- rodps.table.hist
