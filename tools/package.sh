#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..

# fresh building java lib
rm -rf $basepath/inst/java/rodps-*.jar >> /dev/null 2>&1

sh $basepath/configure
sh $basepath/tools/format_code.sh
sh $basepath/tools/gendoc.sh

echo "Check RODPS package..."
R --no-save -e "library(devtools);devtools::check()"

echo "Build RODPS package..."
R --no-save -e "library(devtools);devtools::build(path = '.')"
