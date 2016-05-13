rodps.help <- function()
{
    cat("Please try help(rodps) \n")
}

rodps.table.list <- function(pattern=NULL, projectname=NULL)
{
    .check.init();
    cur.prj <- rodps.project.current()
    if( ! is.null(projectname)){
        rodps.project.use( projectname )
    }

    tables <- try(odpsOperator$getTables(NULL, pattern))
    if( ! is.null(projectname)){
        rodps.project.use( cur.prj )
    }
    if("try-error" %in% class( tables )) {
        stop('Exception occured when listing tables')
    }
    return(.change.data(tables))
}
rodps.table.partitions <- function(full.tablename){
    .check.init();
    df <- rodps.sql(paste("show partitions",full.tablename))
    return(df)
}
rodps.table.drop <- function(full.tablename, partition=NULL)
{
    .check.init();
    p.t <- rodps.split.ftn( full.tablename )

    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    if( !is.null( projectname )){
        ftn <- paste(projectname, ".", tablename, sep="")
    }
    else{
        ftn <- tablename
    }
    if(is.null(partition)){
        sql <- paste("drop table if exists", ftn)
    }else{
        sql <- paste("alter table", ftn, "drop partition(", partition, ")")
    }
    rodps.sql( sql )
    return(TRUE)
}

rodps.table.desc <- function(full.tablename,partition=NULL)
{
    .check.init();
    p.t <- rodps.split.ftn( full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

	if(is.null(projectname)){
      projectname <- rodps.project.current()
    }
	
    .check.tablename(tablename)
    tableMeta <- odpsOperator$describeTable(.jnew("java/lang/String",projectname), tablename, partition)
    ret<-.change.to.list(tableMeta)
    ret$columns=.column.to.dataframe(ret$columns)
    if(length(ret$partition_keys)>0){
        ret$partition_keys<-.column.to.dataframe(ret$partition_keys)
    }
    if("windows" == .Platform$OS.type){
        ret$comment <- iconv(ret$comment,"utf-8","gbk")
        ret$columns$comments <- iconv(ret$columns$comments,"utf-8","gbk")
    }
    return(ret)
}
#将pt|string| 转成dataframe
.column.to.dataframe <-function(cols){
    len<-length(cols)
    names<-c()
    types<-c()
    comments<-c()
    for(i in 1:len){
        items<-strsplit(cols[i],"|",fixed=TRUE)
        names[i]<-items[[1]][1]
        types[i]<-items[[1]][2]
        if(length(items[[1]])>2){
            comments[i]<-items[[1]][3]
        }else{
            comments[i]<-NA
        }
    }
    return(data.frame(names,types,comments,stringsAsFactors=FALSE))
}
rodps.table.exist <- function(full.tablename, partition=NULL)
{
    .check.init();
    p.t <- rodps.split.ftn( full.tablename )
    projectname <- p.t$projectname
    tablename <- p.t$tablename

	if(is.null(projectname)){
      projectname <- rodps.project.current()
    }
	
    .check.tablename(tablename)
    tableExist <- odpsOperator$isTableExist(.jnew("java/lang/String",projectname), tablename, partition);
    return(tableExist)
}

.rodps.bigSql <- function(query , memsize = 10737518240 ) {
    .check.init();
    if(is.null(query) || query==""){
        stop(error("input_query_error","query is null"))
    }
    postfix <- paste(sample(c(letters[1:6],0:9),30,replace=TRUE),collapse="")
    tmptable <- paste("rodps_result_",postfix,sep="")
    query <- paste("CREATE TABLE ", tmptable," LIFECYCLE 3 AS ", query,sep="")

    
    odpsOperator$runSqlTask(query)
    length <- rodps.table.size(tmptable)
	
	
    if(length > memsize) {
        x <- tmptable
        attr(x,"result:size") <- length
        return(x)
    }else {
        result <- try( rodps.table.read(tmptable, memsize = memsize) )
        odpsOperator$runSqlTask(paste("DROP TABLE ",tmptable,sep=""))
        if("try-error" == class(result)) {
            stop( paste("Exception ocurred when loading table:",tmptable,sep="") )
        }
        else{
            return(result)
        }
    }
}

rodps.project.use<-function(projectname){
    .check.init();
    if(is.null(projectname) || projectname==""){
        stop(error("invalid_project_name"))
    }
    odpsOperator$useProject(projectname)
}

rodps.sql <- function(query)
{
    .check.init();
    if(is.null(query) || query == ""){
        stop(error("input_query_error","query is null"))
    }
    type<-blacklist(query)
    if(as.logical(type) && type==TRUE){
        stop(error("input_query_error",paste("rodps.sql does not support '",query,"' command",sep="")))
    }
    
    #set odps.instance.priority
    if(grepl("set",query)&&grepl("odps.instance.priority",query)&&grepl("[",query,fixed = TRUE)&&grepl("]",query,fixed = TRUE)){
      query_1 <- strsplit(query,'[',fixed=TRUE)
      query_1[[1]][1]
      query_1[[1]][2]
      query_2 <- strsplit(query_1[[1]][2],']',fixed=TRUE)
      if(!is.na(query_2[[1]][2])){
        query <- paste(query_1[[1]][1],query_2[[1]][2],sep="")
      }else{
        query <- query_1[[1]][1]
      }
      if(nchar(query_2)>0){
        odpsOperator$runSqlTask(query_2[[1]])
      }
      if(nchar(query)<1){
        return(TRUE)
      }
    }
    
    
    if(type=="select"){
        return(.rodps.bigSql(query))
    }
    ret <- odpsOperator$runSqlTask(query)
    if(ret==NULL || ret$size()==0){
	    return(NULL)
    }
    if(ret$size()==1){
	    return(strsplit(ret$get(as.integer(0)),"\n"))
    }
    vlist<-list()
    for(i in c(0:(ret$size()-1))){
    	vlist[i+1]<-ret$get(as.integer(i))
    }
    return(vlist)
}
#不支持运行的query
blacklist<-function(query){
    tokens<-strsplit(tolower(query),"\\s+",fixed=FALSE)
    if(length(tokens[[1]])==0){
        return(FALSE)
    }
    if(length(tokens[[1]])>=2 && tokens[[1]][1]==""){
        head<-tokens[[1]][2]
    }else{
        head<-tokens[[1]][1]
    }
    if(head=="use" || head=="read"){
        return(TRUE)
    }
    return(head) 
}

rodps.table.size <- function(full.tablename, partition=NULL)
{	
	.check.init();
    p.t <- rodps.split.ftn( full.tablename )
    projectname <- p.t$projectname
    tablename <- p.t$tablename

	.check.tablename(tablename)
	size<-odpsOperator$getTableSize(.jnew("java/lang/String",projectname), tablename,partition);
	
	return(size)
	
}

#dataframe can be written to a non-exist table or partition
rodps.table.write <- function(dataframe, full.tablename, partition=NULL, tablecomment=NULL,isdebug=FALSE,thread=8)
{
    .check.init();
    p.t <- rodps.split.ftn( full.tablename )
    projectname <- p.t$projectname
    tablename <- p.t$tablename

	if(is.null(projectname)){
      projectname <- rodps.project.current()
    }
	
    .check.tablename(tablename)
    if( !is.data.frame(dataframe)){
        stop('dataframe should be class of data.frame')
    }

    if (length(colnames(dataframe)) == 0) {
        stop('dataframe should have as least one column')
    }

    if(!is.null(partition) && !rodps.table.exist(full.tablename)){
        stop(sprintf("Table not exists,table=%s partition=%s",full.tablename,partition))
    }
    sql <- NULL 
    if(!rodps.table.exist(full.tablename)){
        sql <- .rodps.generate.DDL(full.tablename, dataframe, tablecomment)
    }
    if(!is.null(partition) && !rodps.table.exist(full.tablename,partition)){
        sql <- paste("alter table",full.tablename,"add partition(",odpsOperator$formatPartition(partition,"'",","),")")
    }
    if(!is.null(sql)){
        ret <- try(rodps.sql( sql ) )
        if ('try-error' %in% class(ret)){
            cat('Exception occured when creating table\n')
            cat(sql)
            cat('\n')
        }
    }

    if (nrow(dataframe) == 0) {
        return(TRUE)
    }

    op <- options(digits.secs = 6)
    filename <- paste(as.character(runif(1, min=0, max=as.integer(Sys.time()))), sep=".")
    filename <- paste(tempdir(), filename, sep= .Platform$file.sep)
    options(op)

    actual_thread <- as.integer(thread)
    if (nrow(dataframe) < thread * 100) {
        actual_thread <- as.integer(1)
    }
    if ("windows" == .Platform$OS.type) {
        dataframe <- .dataframe.code.conv(dataframe,"","UTF-8")
    }
    dbNames <- .dataframe.to.sqlite(dataframe, actual_thread, filename, tablename, isdebug)
    odpsOperator$writeTableFromDT(projectname, tablename, partition, filename, NULL, NULL,.jlong(length(dataframe[[1]])),actual_thread)
    if(!isdebug){
        for (i in 1:length(dbNames)) {
            file.remove(dbNames[i])
        }
    }
    return(TRUE)
}

.dataframe.to.sqlite <- function(dataframe, thread, filename, tablename, isdebug) {
    if(!require(DBI,quietly=TRUE)) {
         stop('DBI library not available')
    }
    if(!require(RSQLite,quietly=TRUE)) {
        stop('RSQLite library not available')
    }
    dl <- list()
    recordNumPerThread <- nrow(dataframe) %/% thread
    dbNames = c()
    for (i in (0:(thread-1)))
    {
        startPos <- i * recordNumPerThread + 1
        endPos <- startPos + recordNumPerThread -1
        if (i == thread-1)
        {
            endPos <- nrow(dataframe)
        }
        dl[[length(dl) + 1]] <- as.data.frame(dataframe[startPos:endPos,])
    }

    for (i in (1:length(dl)))
    {
        dbName <- paste(filename, "_", i-1, sep="")
        if (file.exists(dbName)) {
            print(paste("warning:upload middle file",dbName,"already exist, now delete it."))
			file.remove(dbName)
        }

        con <- dbConnect(SQLite(), dbname = dbName)
        dbWriteTable(con, tablename, dl[[i]], row.names=FALSE)
        if(isdebug){
            print(paste("write file",i,":",dbName))
        }
        dbDisconnect(con)
        dbNames <- append(dbNames, dbName)
    }
    return(dbNames)
}

rodps.table.read <- function(full.tablename, partition=NULL, limit = -1,memsize = 10737518240,isdebug=FALSE,thread=8)
{
    .check.init();
    p.t <- rodps.split.ftn( full.tablename )
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    tablesize <- rodps.table.size(full.tablename, partition=partition) 
    if ((tablesize > memsize) && (limit== -1))
    {
        msg <- paste("whole table size (", tablesize, ") is larger than memsize (", memsize, "), can not be loaded.")
        stop(msg)
    }

    op <- options(digits.secs = 6)
    filename <- paste(as.character(runif(1, min=0, max=as.integer(Sys.time()))), sep=".")
    filename <- paste(tempdir(), filename, sep= .Platform$file.sep)
    options(op)
    results <- odpsOperator$loadTableFromDT(projectname, tablename, partition, filename, NULL, NULL, as.integer(limit),as.integer(thread));
    
    if (3 != results$size()) {
        stop("Internal error with load table from dt, please contact kai.xu")
    }
    res <- .sqlite.to.dataframe(results$get(as.integer(2)), results$get(as.integer(1)), tablename, isdebug)
    if ("windows" == .Platform$OS.type) {
        res <- .dataframe.code.conv(res, "UTF-8","")
    }
    return(res)
}

.sqlite.to.dataframe <- function(dbs, coltype, tablename, isdebug)
{
    if(!require(DBI,quietly=TRUE)) {
        stop('DBI library not available')
    }
    if(!require(RSQLite,quietly=TRUE)) {
        stop('RSQLite library not available')
    }

    filenum <- dbs$size()
    if (filenum == 0) {
        stop("Internal error: no middle file return.")
    }
    data <- data.frame()
    for (i in 0:(filenum-1))
    {
        filename <- dbs$get(i)
        if (!file.exists(filename)) {
            stop(paste("file not exists:", filename))
        }
        con <- dbConnect(SQLite(), dbname = filename)
        sql = paste("select * from [", tablename,"]", sep="")
        tmp_data <- dbGetQuery(con, sql)
        data <- rbind(data, tmp_data)
        dbDisconnect(con)
        if (isdebug) {
             print(paste("download temp file:",filename))
        } else {
            file.remove(filename)
        }
    }

    for (i in 0:(coltype$size()-1)) {
        if (coltype$get(i) == 'datetime') {
            data[[i+1]] = as.POSIXct(as.POSIXlt(data[[i+1]], origin="1970-01-01"))
        } else if (coltype$get(i) == 'boolean') {
            data[[i+1]] = as.logical(data[[i+1]])
        }
    }
    return(data)
}

.dataframe.code.conv <- function(dataframe, fromcode, tocode)
{
    collen <- ncol(dataframe)
    for (i in 1:collen) {
        type <- is(dataframe[[i]])[1]
        if (type == "character" || type == "factor") {
            dataframe[[i]] = iconv(dataframe[[i]], fromcode, tocode)
        }
    }
    return(dataframe)
}

.change.to.list <- function(ret){
    lst<-list()
	if(!is.null(ret)){
		data <- .jcast(ret, new.class="java/util/List",check = FALSE, convert.array = FALSE)
		if(!data$isEmpty()){
			vlist <- c(0:(data$size()-1))
			for(pos in vlist){
				dfitem <- data$get(as.integer(pos))
				values <- dfitem$getData()
                if(values$size()>1){
                    vs<-c()
                    for( i in (0:(values$size()-1))){
                        v<-values$get(as.integer(i))
                        if(is.null(v)){
                            vs[i+1]<-NA
                        }else{
                            vs[i+1]<-.change.value(dfitem$getType(),v)
                        }
                    }
                    lst[[dfitem$getName()]]<-vs
                }else{
                    v<-values$get(as.integer(0))
                    if(is.null(v)){
                        lst[[dfitem$getName()]]<-NA
                    }else{
                        lst[[dfitem$getName()]]<-.change.value(dfitem$getType(),values$get(as.integer(0)))
                    }
                }
			}
		}
	}
	return(lst)
}
#将java中返回的List<DataFrameItem>转化成对象
.change.to.obj <- function(ret) {
    obj<-"object"
	if(!is.null(ret)){
		data <- .jcast(ret, new.class="java/util/List",check = FALSE, convert.array = FALSE)
		if(!data$isEmpty()){
			vlist <- c(0:(data$size()-1))
			for(pos in vlist){
				dfitem <- data$get(as.integer(pos))
				values <- dfitem$getData()
                if(values$size()>1){
                    vs<-c()
                    for( i in (0:(values$size()-1))){
                        v<-values$get(as.integer(i))
                        if(is.null(v)){
                            vs[i+1]<-NA
                        }else{
                            vs[i+1]<-.change.value(dfitem$getType(),v)
                        }
                    }
                    attr(obj,dfitem$getName())<-vs
                }else{
                    v<-values$get(as.integer(0))
                    if(is.null(v)){
                        attr(obj,dfitem$getname())<-NA
                    }else{
                        attr(obj,dfitem$getName())<-.change.value(dfitem$getType(),values$get(as.integer(0)))
                    }
                }
			}
		}
	}
	return(obj)
}

.change.data <- function(ret) {
	rdata <- list()
	if(!is.null(ret) && ret$size()>0){
		data <- .jcast(ret, new.class="java/util/List",check = FALSE, convert.array = FALSE)
        vlist <- c(0:(data$size()-1))
        for(pos in vlist){
            dfitem <- data$get(as.integer(pos))
            rdata[[pos + 1]] <- .change.type(dfitem$getType())			
            names(rdata)[pos + 1] <- dfitem$getName()
            values <- dfitem$getData()
            if(values$size()>0){
                for( i in (0:(values$size()-1))){
                    v<-values$get(as.integer(i))
                    if(is.null(v)){
                        v<-NA
                    }
                    rdata[[pos+1]][i+1]<-.change.value(dfitem$getType(),v)
                }
            }
        }
	}
	return(as.data.frame(rdata,stringsAsFactors=FALSE))
}

.change.value <- function(type,value){
    type<-tolower(type)
    if(is.null(type)){
        return(as.character(value))
    }
    rtype <- rodps.type.java2r[type]
    if(!is.null(rtype) && rtype!="character"){
        return(eval(parse(text=paste("as.",rtype,"('",value,"')",sep=""))))
    }else{
        return(as.character(value))
    }
} 
.change.type <- function(type){
    type<-tolower(type)
    rtype <- rodps.type.java2r[type]
    if(is.null(rtype)){
        return(character())
    }
    if(type == "datetime"){
        return(Sys.time())
    }
    if(type == "date"){
        return(date())
    }
    return(eval(parse(text=paste(rtype,"()",sep=""))))
} 

.check.tablename<-function(tablename){
    if(is.null(tablename) || tablename==""){
        stop(error("invalid_value","table name is null"))
    }
    if(!is.character(tablename)){
        stop(error("argument_type_error","tablename must be string type."))
    }
}

.data.frame.get.namelist <- function(dataframe)
{
    if (!is.data.frame(dataframe)) 
        stop("input data is not data frame")
    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum )
    {
        retlist$add(names(dataframe)[i])
    }
    return(retlist)
}

.data.frame.get.typelist <- function(dataframe)
{
    if (!is.data.frame(dataframe)) 
        stop("input data is not data frame")
    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum )
    {
        retlist$add(.get.object.type(dataframe[[i]]))
    }
    return(retlist)
}

.data.frame.to.arraylist <- function(dataframe)
{
    if (!is.data.frame(dataframe)) 
        stop("input data is not data frame")

    retlist <- .jnew("java/util/ArrayList")
    columnnum <- length(dataframe)
    for (i in 1:columnnum )
    {
        name <- names(dataframe)[i]
        type <- .get.object.type(dataframe[[i]])
        dataframeitem <- .jnew("com/aliyun/openservices/odps/roperator/DataFrameItem", name, type)
        for (j in 1:length(dataframe[[i]]))
        {
            dataframeitem$getData()$add(as.character(dataframe[[i]][j]))
        }
        retlist$add(dataframeitem)
    }
    return(retlist)
}


.get.object.type <- function(obj)
{
    type <- is(obj)[1]
    return(rodps.type.r2java[type])
}

.check.column.name <-function(colname)
{
    if( length(grep('[.]|[$]', colname)) >0 || 
        nchar(colname) > 128 || 
        substr(colname,1,1) == '_')
      stop( paste('Invalid column name',colname) )
}

.rodps.generate.DDL <- function( full.tablename, dataframe, tablecomment=NULL)
{
    p.t <- rodps.split.ftn( full.tablename )
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename(tablename)
    if(!is.data.frame( dataframe )){
      stop( 'dataframe should be data.frame type' )
    }

    namelist <- names(dataframe)
    for (n in namelist)
        .check.column.name(n)

    typelist <- sapply(dataframe, .get.object.type )

    sql <- paste(" CREATE TABLE ",full.tablename," (\n",sep="")
    ncol <- length( namelist )
    ntype <- length( typelist ) 

    for( i in seq(1,ncol )){
      if (i != ncol ){
        sql <- paste(sql, " ", namelist[i], "\t", typelist[i] , ",\n", sep=" ")
      }
      else{
        sql <- paste(sql, " ", namelist[i], "\t", typelist[i], ")", sep= " " )
      }
    }
    if( !is.null( tablecomment )) {
        sql <- paste( sql , "\n COMMENT '", tablecomment, "'")
    }
    return( sql )
}

#sample data
#sql sample or sample() over ()
rodps.table.sample.srs <- function(srctable,tgttable, samplerate, cond=NULL, select=NULL)
{
    rv <- round(runif(3)*100)

    .check.tablename(srctable)
    .check.tablename(tgttable)
    if( !is.numeric(samplerate)){
        stop( 'samplerate should be numeric')
    }

    if( is.null(select)){
        sel = "*"
    }
    else{
        if( !is.character(select) ){
            stop('Select should be character')
        }
        sel = paste(select, sep=',', collapse=",")
    }


    if( !rodps.table.exist(srctable) ){
        stop( paste('Table not exists ',srctable) )
    }
    if( rodps.table.exist(tgttable) ){
        stop( paste('Target table already exists', tgttable ))
    }

    sql <- sprintf(' SELECT %s FROM %s', sel, srctable)
    if( !is.null(cond) ){
        if(!is.character(cond)){
            stop('Invalid condition expression')
        }
        else{
            sql <- paste( sql , ' WHERE ' , cond)
        }
    }
    
    distby = sprintf(' DISTRIBUTE BY rand(%d)*10 SORT BY rand(%d)', rv[1], rv[2])
    if( samplerate<1 ){
        #sample by percentage
        sql <- paste( sql, distby )
        sql <- paste( " CREATE TABLE ", tgttable, "  AS \n SELECT * FROM (", sql, " ) sub \n WHERE rand(", rv[3], ")<= ", samplerate )
    }
    else{
        #sample by abs value
        sql <- paste(" CREATE TABLE ",tgttable," AS  \n SELECT * FROM ( ", sql, distby, " ) sub \n LIMIT ", samplerate)
    }
    ret <- try( rodps.sql( sql ) )
    if( 'try-error' %in% class(ret) ){
        cat("Exception occurred when executing sql \n")
        cat( sql )
        cat('\n')
        return(FALSE)
    }
    return(T)
}

#分层抽样
#select
# abc
#from (
#  *,
#  row_number() over( partition by g order by rand()) r_rn,
#  rand() as r_select
#) sub

#1. by percent
#sub
#where r_select < rate

#2. by_number
#sub
#where rn <= rate

rodps.table.sample.strat <- function(srctable, tgttable, samplerate, strat,select=NULL)
{
    .check.tablename(srctable)
    .check.tablename(tgttable)

    if( !is.numeric(samplerate) ){
        stop('sample rate should be numeric ')
    }
    if( !is.character(strat)){
        stop('strat should be character')
    }
    if( !is.null(select) && !is.character(select)){
        stop('select should be character')
    }
    if( rodps.table.exist(tgttable)){
        stop('target table already exists')
    }
    
    rv <- round(runif(3)*100)
    if( is.null(select) ){
        des <- rodps.table.desc( srctable )
        cols <- paste( des$columns$names, collapse=',')
    }else{
        cols <- paste( select, collapse=',')
    }
    pcols <- paste(strat, collapse=',')

    temp <- 'CREATE TABLE %s AS \n SELECT %s FROM ( \n SELECT %s, \n row_number() OVER (PARTITION BY %s ORDER BY rand(%d)) sel_rownumber, \n rand(%d) sel_random  FROM %s) sub'
    sql <- sprintf( temp, tgttable, cols, cols, pcols, rv[1], rv[2] , srctable)

    if( samplerate<1 ){
        sql <- paste(sql, ' WHERE sel_random <= ', samplerate)
    }
    else{
        sql <- paste(sql, ' WHERE sel_rownumber <= ', samplerate)
    }
    
    ret <- try(rodps.sql( sql ) )
    if( 'try-error' %in% class(ret)){
        cat('Exception occurred when executing sql\n')
        cat(sql)
        cat('\n')
    }
    return(TRUE)

}

rodps.table.rows <- function(full.tablename, partition=NULL)
{
    .check.init();
    p.t <- rodps.split.ftn(full.tablename)
    projectname <- p.t$projectname
    tablename <- p.t$tablename

    .check.tablename( tablename )
    sz <- rodps.table.size( full.tablename  )

    if( sz < 10 * 1024 * 1024 * 1024 || !is.null(partition) && partition!=""  ){
        sql <- sprintf(' count %s ', full.tablename )
        if(!is.null(partition) && partition!=""){
            sql <- paste(sql, "partition(", partition, ")")
        }
        v <- rodps.sql( sql ) 
        ret <- as.numeric( v[[2]] )
    }
    else{
        sql <- sprintf( 'select count(*) from %s', full.tablename )
        v <- rodps.sql( sql ) 
        ret <- as.numeric(v[1 , 1])
    }
    return( ret )
}

rodps.project.current <- function()
{
    .check.init();
    return(odpsOperator$getProjectName(""))
}

#split full table name into table name and project name
rodps.split.ftn <- function( ftn )
{
    if( is.null(ftn) || !is.character(ftn) || nchar( ftn ) == 0 || length( ftn ) > 1)
    {
        stop( 'Invalid table name ' )
    }
    p.t <- unlist(strsplit( ftn, '[.]'))
    if( length(p.t) > 2 || length(p.t) < 1){
        stop('Invalid table name ')
    }
    ret <- list()
    if( length(p.t) == 1){
        ret$tablename <- p.t[1]
    }
    else{
        ret$projectname <- p.t[1]
        ret$tablename <- p.t[2]
    }
    return( ret )
}
.check.init<-function(){
    if(length(ls(envir=.GlobalEnv, pattern="odpsOperator"))==0 || is.null(odpsOperator)){
        stop(print("RODPS uninitialized or session timeout, please exectue rodps.init(path), path for the path of odps_config.ini"))
    }
}
