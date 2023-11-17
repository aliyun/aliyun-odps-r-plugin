#' Extend predict function
#' @export
rodps.predict <- function(x, ...) {
    UseMethod("rodps.predict", x)
}

#' Extend Recursive Partitioning
#' 
#' @param object Rpart model
#' @param srctbl Data source table
#' @param tgttbl Target table of prediction results
#' @param dryrun Return the prediction SQL string instead of running the query
#' @export
rodps.predict.rpart <- function(object, srctbl, tgttbl, inc.col = NULL, dryrun = FALSE) {
    if (!require("rpart")) {
        stop("rpart package required in rodps.predict.rpart")
    }

    if (class(object) != "rpart") {
        stop("object is not rpart model")
    }
    if ((object$method != "class") && (object$method != "anova")) {
        stop("this model method is not class or anova, not supported yet")
    }
    yvar = as.character(attr(object$terms, "variables"))[-1][attr(object$terms, "response")]
    if (length(yvar) > 1) {
        stop("Multiple response variable found in formula")
    }
    yvar.p = sprintf("%s_predict", yvar)
    sql = sprintf("CREATE TABLE IF NOT EXISTS %s AS\n", tgttbl)

    srccol <- as.character(object$frame$var[object$frame$var != "<leaf>"])
    ylevels <- attr(object, "ylevels")
    ylabels <- labels(object)
    leafidx <- which(object$frame$var == "<leaf>")
    cw = " CASE "
    sidx = which(object$frame$var != "<leaf>")
    nodes = as.numeric(row.names(object$frame))

    for (i in leafidx) {
        pos = nodes[i]
        cond = ylabels[i]
        # get parent condition
        parentidx = pos%/%2
        while (parentidx > 1) {
            pcond = ylabels[which(nodes == parentidx)]
            cond = sprintf("%s AND %s", pcond, cond)
            parentidx = parentidx%/%2
        }

        if (object$method == "class") {
            cond = sprintf(" WHEN %s THEN '%s'", cond, ylevels[object$frame$yval[i]])
        } else if (object$method == "anova") {
            cond = sprintf(" WHEN %s THEN %.5f", cond, object$frame$yval[i])
        } else {
            stop("Invalid method")
        }
        cw = sprintf("%s \n %s", cw, cond)
    }
    cw = sprintf("%s \n END AS %s\n", cw, yvar.p)
    sel = " SELECT "
    if (!is.null(inc.col)) {
        sel = sprintf("%s\n  %s,", sel, paste(inc.col, sep = "", collapse = ",\n  "))
    }
    for (col in unique(srccol)) {
        sel = sprintf("%s \n  %s,", sel, col)
    }
    sel = sprintf("%s \n  %s,", sel, yvar)
    sql = sprintf("%s %s \n%s FROM %s;\n", sql, sel, cw, srctbl)

    if (dryrun) {
        return(sql)
    } else {
        cat(sql)
        if (!rodps.table.exist(srctbl)) {
            stop(sprintf("source table %s does not exist", srctbl))
        }

        if (rodps.table.exist(tgttbl)) {
            stop(sprintf("target table %s already exists", tgttbl))
        }
        rodps.sql(sql)
    }
}

#' Extend FDA

#' @param object FDA model
#' @param srctbl Data source table
#' @param tgttbl Target table of prediction results
#' @param dryrun Return the prediction SQL string instead of running the query
#' @export
rodps.predict.fda <- function(object, srctbl, tgttbl, prior, type = "class", dimension = 2) {
    if (!require(mda)) {
        stop("mda library not available")
    }
    if (class(object) != "fda") {
        stop("Invalid object class, only support fda model")
    }
    dist <- function(x, mean, m = ncol(mean)) (scale(x, mean, FALSE)^2) %*% rep(1,
        m)

    type <- match.arg(type)
    if (type != "class") {
        stop("type is not class, not supported yet")
    }
    if (object$fit$monomial != FALSE) {
        stop("unsupported monomial class")
    }
    if (attr(object$fit, "class") != "polyreg") {
        stop("unsupported fit class, only polyreg works")
    }
    if (object$fit$degree != 1) {
        stop("unsupported object$fit$degree")
    }
    means <- object$means
    Jk <- dim(means)
    J <- Jk[1]
    k <- Jk[2]
    if (k > 2) {
        stop("k is not 2, unsupported dimension <=2")
    }
    if (type == "hierarchical") {
        if (missing(dimension))
            dimension.set <- seq(k) else {
            dimension.set <- dimension[dimension <= k]
            if (!length(dimension.set))
                dimension.set <- k
            dimension <- max(dimension.set)
        }
    } else {
        dimension <- min(max(dimension), k)
    }
    # y <- predict(object$fit, newdata)　　#假设object$fit$degree=1,
    # object$fit$monomial=FALSE
    yvar.name = paste(all.vars(object$terms)[attr(object$terms, "response")], seq(1:ncol(object$fit$coefficients)),
        sep = "_ln")
    # 线性变换intercept + a1x1+a2x2+...
    vars = rownames(object$fit$coefficients)
    names(vars) = rownames(object$fit$coefficients)
    vars[which(vars == "Intercept")] = "1"
    var.exp = c()
    for (i in seq(1:ncol(object$fit$coefficients))) {
        var.exp[i] = paste(object$fit$coefficients[, i], vars, sep = "*", collapse = "+")
    }
    sql.linear.exp = paste(var.exp, yvar.name, sep = " AS ", collapse = ",\n  ")
    sql.linear.exp = gsub("\\+-", "-", sql.linear.exp)
    sql.linear = sprintf("  SELECT * ,\n  %s \n FROM %s\n ", sql.linear.exp, srctbl)

    # y <- y %*% object$theta[, seq(dimension), drop = FALSE] #这里转为ＳＱＬ

    lambda <- object$values
    alpha <- sqrt(lambda[seq(dimension)])
    sqima <- sqrt(1 - lambda[seq(dimension)])
    # 根据alpha值缩放，合并到上一步，sql.tran中 newdata <- scale(y, FALSE,
    # sqima * alpha)
    sa = sqima * alpha
    # 投影+缩放
    theta = object$theta[, seq(dimension), drop = FALSE]
    ytran.name = paste(all.vars(object$terms)[attr(object$terms, "response")], seq(1:ncol(object$fit$coefficients)),
        sep = "_tr")
    var.exp = c()
    for (i in seq(1:ncol(theta))) {
        var.exp[i] = sprintf(" (%s)/%f ", paste(theta[, i], yvar.name, sep = "*",
            collapse = "+"), sa[i])
    }
    sql.tran.exp = paste(var.exp, ytran.name, sep = " AS ", collapse = "  ,\n  ")
    sql.tran.exp = gsub("\\+-", "-", sql.tran.exp)
    sql.tran = sprintf(" SELECT *, \n %s \n FROM (\n %s ) sub1 \n", sql.tran.exp,
        sql.linear)
    if (missing(prior))
        prior <- object$prior else {
        if (any(prior < 0) | round(sum(prior), 5) != 1)
            stop("innappropriate prior")
    }
    means <- means[, seq(dimension), drop = FALSE]

    prior <- 2 * log(prior)
    dist_list = c()
    for (i in seq(1:nrow(means))) {
        dist_i = paste(ytran.name, "-", means[i, ], sep = "")
        dist_i = paste(" pow(", dist_i, ",2)", sep = "", collapse = "+")
        # 加上prior
        dist_i = paste(dist_i, prior[i], sep = "-")
        dist_list[i] = dist_i
    }
    dist_exp = paste(dist_list, sep = " , ", collapse = ",")
    dist_exp = gsub("--", "+", dist_exp)
    dist_exp = gsub("\\+-", "-", dist_exp)
    label_exp = paste(" WHEN ", seq(nrow(means)), " THEN '", rownames(means), "'",
        sep = "", collapse = "")
    case_exp = sprintf(" CASE least_index(%s) \n %s \n  END AS %s\n", dist_exp, label_exp,
        "predict_v")
    sql = sprintf(" CREATE TABLE %s AS \nSELECT *, \n%s \n FROM (\n %s \n) sub_2",
        tgttbl, case_exp, sql.tran)
}
