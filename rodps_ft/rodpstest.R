test.dir="testout"
base.dir="testbase"
cur.test=NULL
current.prj=NULL
other.prj=NULL


mark <- function( case.title )
{
  print(paste("#################case:",case.title,"##########################"))
  sink( sprintf('%s/%s.out',test.dir,case.title), type=c("output","message"))
  cat(sprintf('########### RODPS %s###########\n',case.title))
}

check.case <- function()
{
  sink()
}


library('RODPS')
rodps.init("./odps_config.ini_newdailyrun")
current.prj <- rodps.project.current()
rodps.bizid('012345^')
#
mark('listtable')
rodps.table.list()
check.case()

#
mark('list_partition')
rodps.sql("drop table test_partition")
rodps.sql("create table test_partition(id string) partitioned by(pt string)")
rodps.sql("alter table test_partition add partition(pt='20140531')")
rodps.table.partitions('test_partition')
check.case()

#
mark('create_drop_table')
rodps.sql("create table if not exists rodps_drop_table(a string)")
rodps.table.desc('rodps_drop_table')
rodps.table.drop( 'rodps_drop_table' )
rodps.sql("create table if not exists rodps_drop_table(a string)")
rodps.table.drop( paste(current.prj,'.','rodps_drop_table',sep="") )
rodps.table.exist(paste(current.prj,"rodps_drop_table",sep="."))
check.case()

## creat table[dual] for test
#rodps.sql('create table if not exists dual(a string)')
#check.case()

#
mark('desc_table')
rodps.table.desc('dual')
rodps.table.desc(paste(current.prj,".",'dual',sep=""))
check.case()

#
mark('exist_table')
rodps.table.exist('dual')
rodps.table.exist(paste(current.prj,".",'dual',sep=""))
check.case()

#
mark('select_table')
x <- rodps.sql('select * from dual')
x
sapply(x,class)
sapply(x,typeof)
check.case()


#
mark('size_table')
rodps.table.size('dual')
rodps.table.size(paste(current.prj,".","dual",sep=""))
check.case()

#
mark('write_table')
x<-data.frame(c1=1:10,c2=1:10)
rodps.table.write(x,'rodps_write_table')

names(iris) = gsub('\\.','_',names(iris))
rodps.table.drop('iris_test')
rodps.table.write(iris,'iris_test')
check.case()

#
mark('load_table')
rodps.table.read('dual')
rodps.table.read(paste(current.prj,".","dual",sep=""))

rodps.table.drop('rodps_load_write_table')
rodps.sql('create table rodps_load_write_table(c_couble double, c_string string, c_boolean boolean, c_datetime datetime, c_bigint bigint )')
rodps.sql('insert into table rodps_load_write_table select 1.1, "abc",true,to_date("20130101","yyyymmdd"), 10000 from (select count(*) from rodps_load_write_table) a')
rodps.sql('insert into table rodps_load_write_table select -1.1, "ab\\nc",false,to_date("99991231","yyyymmdd"), -10000 from (select count(*) from rodps_load_write_table) a')

x<-rodps.table.read('rodps_load_write_table')
x
check.case()


#
mark('sample_srs')
#test sample.srs
rodps.table.drop('rodps_sample_src')
rodps.table.drop('rodps_sample_tgt')

x<-data.frame(c1=1:1000,c2=rep(1:10,100))

rodps.table.write(x,'rodps_sample_src')
rodps.table.sample.srs('rodps_sample_src','rodps_sample_tgt',0.5)
x<-rodps.table.read('rodps_sample_tgt')

names(x)
nrow(x)

rodps.table.drop('rodps_sample_tgt')
rodps.table.sample.srs('rodps_sample_src','rodps_sample_tgt', 20,select=c("c1"))
x<-rodps.table.read('rodps_sample_tgt')

names(x)
nrow(x)
check.case()

#test sample.strat
mark('sample_strat')
rodps.table.drop('rodps_sample_src')
rodps.table.drop('rodps_sample_tgt')

x<-data.frame(c1=1:1000,c2=rep(1:10,100),c3=rep(1:2,500))
rodps.table.write(x,'rodps_sample_src')

rodps.table.sample.strat('rodps_sample_src','rodps_sample_tgt',0.5,strat=c("c3"))
y<-rodps.table.read('rodps_sample_tgt')
names(y)
nrow(y)
check.case()

#test datetime loading
mark('test_datetime')
d <- rodps.table.read('rodps_load_write_table')
summary(d)
check.case()


#
mark('rodps_predict_rpart')
library(rpart)
names(iris) <- gsub('\\.','_',names(iris))
rodps.table.drop('iris_tbl')
rodps.table.drop('iris_predict')
rodps.table.write(iris,'iris_tbl')
fit=rpart(Species~.,data=iris)
sql=rodps.predict.rpart(fit, 'iris_tbl','iris_predict',run=F)
rodps.sql(sql)
d = rodps.table.read('iris_predict')
v = predict(fit)
max_index<-function(v) {names(v)[which(v==max(v))]}
v1 = apply(v, 1, max_index)
any(d$species_predict != v1)
check.case()

