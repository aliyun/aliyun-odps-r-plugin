# RODPS: ODPS Plugin for R

[![Build Status](https://travis-ci.org/aliyun/aliyun-odps-r-plugin.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-odps-r-plugin)


## Features

- Read/write dataframe from/to ODPS.
- Convert some of the R models to SQL command.
- The large data set can be processed by using the distributed algorithm.
- The small data set can be processed directly in R.


## Requirements

- Java 6+
- R 1.80+


## Installation

### Build R package

```
./build.sh
```

### Install R package


```
sudo R CMD INSTALL RODPS
```


### Configuration

Please make sure the environment variable `RODPS_CONFIG` is set to `path/to/odps_config.ini`


```
export RODPS_CONFIG=path/to/odps_config.ini
```

See the configuration template: `odps_config.ini.template`


## Getting Started


```R
> library("RODPS")  # Load RODPS
>  
> tbl1 <- rodps.table.read("tbl1")  # read dataframe from ODPS
> d <- head(tbl1)
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


## Authors && Contributors

- [Yang Hongbo](https://github.com/hongbosoftware)
- [Yao Weidong](https://github.com/yaoweidong)


## License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
