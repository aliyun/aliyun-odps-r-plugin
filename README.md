# RODPS: ODPS Plugin for R

[![Building RODPS](https://github.com/aliyun/aliyun-odps-r-plugin/actions/workflows/building.yaml/badge.svg?branch=master)](https://github.com/aliyun/aliyun-odps-r-plugin/actions/workflows/building.yaml)


## Features

- Read/write dataframe from/to ODPS.
- Convert some of the R models to SQL command.
- The large data set can be processed by using the distributed algorithm.
- The small data set can be processed directly in R.

## Requirements

System dependencies:

- Java 8+
- R 1.8+

R libraries:

- [rJava](https://cran.r-project.org/web/packages/rJava/index.html)
- [DBI](https://cran.r-project.org/web/packages/DBI/index.html)
- [RSQLite](https://cran.r-project.org/web/packages/RSQLite/index.html)

## Installation

1. Install the R dependencies:

```R
install.packages('DBI')
install.packages('rJava')
install.packages('RSQLite')
```
2. Install RODPS

    2.1. Install from release package

    Check out the latest version on [release page](https://github.com/aliyun/aliyun-odps-r-plugin/releases). As for version 2.1.3, for example: 

    ```R
    install.packages('https://github.com/aliyun/aliyun-odps-r-plugin/releases/download/v2.1.3/RODPS_2.1.3.tar.gz', type="source", repos=NULL)
    ```

    2.2. Install with `devtools` packages

    This method requires JDK and Maven executables to build java module.

    ```R
    install_github("aliyun/aliyun-odps-r-plugin")
    ```

    2.3 Install from CRAN (**Under development**)

## Getting Started

1. Please make sure the environment variable `RODPS_CONFIG` is set to `/path/to/odps_config.ini`

```bash
export RODPS_CONFIG=/path/to/odps_config.ini
```

See the configuration template: [odps_config.ini.template](examples/odps_config.ini.template)

2. Basic Usage

* [Basic project and SQL functions](https://github.com/aliyun/aliyun-odps-r-plugin/blob/master/tests/test_rodps_basics.R)
* [Basic table functions](https://github.com/aliyun/aliyun-odps-r-plugin/blob/master/tests/test_rodps_table.R)

## Under the Hood

### Design Architecture

For the mind map of related concepts, please refer to the [MindMapDoc](docs/mindmap.pdf)

### Type System

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

### Trouble shooting

- For Windows users: DO NOT install BOTH 32bit and 64bit R on your system, which will introduce compiling issues in the installation of `rJava`.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
