# RODPS: ODPS Plugin for R

[![Build Status](https://travis-ci.org/aliyun/aliyun-odps-r-plugin.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-odps-r-plugin)


## Features

- Read/write dataframe from/to ODPS.
- Convert some of the R models to SQL command.
- The large data set can be processed by using the distributed algorithm.
- The small data set can be processed directly in R.

## Requirements

- Java: Recommend Java 8 (Java 9 or higher might encounter JAXB issue)
- R: latest release (RODPS itself support R 1.8+, its dependency libraries does not)

R libraries

- rJava
- DBI
- RSQLite

## Installation

1.Download the package:

[Release page](https://github.com/aliyun/aliyun-odps-r-plugin/releases)

2.Install the dependencies:

```
install.packages('DBI')
install.packages('rJava')
install.packages('RSQLite')
```
Or you can use `devtools` to help you resolve dependencies:

```
install.packages('devtools')
```

3.Install RODPS package:

```
install.packages('https://github.com/aliyun/aliyun-odps-r-plugin/releases/download/v2.1.1/RODPS_2.1.1.tar.gz', type="source", repos=NULL)
```

4.Please make sure the environment variable `RODPS_CONFIG` is set to `path/to/odps_config.ini`


```
export RODPS_CONFIG=path/to/odps_config.ini
```

See the configuration template: [odps_config.ini.template](https://github.com/aliyun/aliyun-odps-r-plugin/blob/master/odps_config.ini.template)

### Trouble shooting

- To windows users: when installing R, DO NOT install both 32bit and 64bit on your system, which will introduce compilation trouble in later installation of rJava.

## Getting Started

```R
> library("RODPS")  # Load RODPS
>
> tbl1 <- rodps.table.read("tbl1")  # read dataframe from ODPS
> d <- head(tbl1)
>
> rodps.sql('create table test_table(id bigint);')   # execute sql
>
> names(iris) <- gsub("\\.","_",names(iris))   # rename columns
> rodps.table.write(iris, 'iris')              # write dataframe to ODPS
>
> rodps.table.sample.srs('tbl1','small_tbl1', 100 ) # sampling by raw
>
> rodps.table.hist(tblname='iris', colname='species', col=rainbow(10), freq=F) # create a histogram
>
> library(rpart)
> fit <- rpart(Species~., data=iris)
> rodps.predict.rpart(fit, srctbl='iris',tgttbl='iris_p') # modeling
```

## Type System

**All numeric in R have possibility of precision loss.**

| MaxCompute/ODPS | R | Notes |
|-----------------|---|-------|
| BOOLEAN | logical | |
| BIGINT | numeric | \[-9223372036854774784, 9223372036854774784\] * |
| INT | numeric | |
| TINYINT | numeric | |
| SMALLINT | numeric | |
| DOUBLE | numeric | |
| FLOAT | numeric | |
| DATETIME | numeric | POSIXct POSIXlt, in second |
| DATE | numeric | POSIXct POSIXlt, in second |
| TIMESTAMP | numeric | POSIXct POSIXlt, in second |
| INTERVAL_YEAR_MONTH | numeric | in month |
| INTERVAL_DATE_TIME | numeric | in second |
| DECIMAL | numeric | |
| STRING | character | |
| CHAR | character | |
| VARCHAR | character | |
| BINARY | character | |
| MAP | - | unsupport |
| ARRAY | - | unsupport |
| STRUCT | - | unsupport |

* BIGINT(64bit) from MaxCompute is stored and calculated as double(64bit) in RODPS. Precision loss might happen when casting BIGINT to double, which shrinks the min/max value could be written back to MaxCompute/ODPS.

## Architecture

[![](mindmap-thumb.png)](mindmap.pdf)

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
