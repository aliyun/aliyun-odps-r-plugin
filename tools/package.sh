#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..
sh $basepath/configure

sh $basepath/tools/format_code.sh
sh $basepath/tools/gendoc.sh

echo "Check RODPS package..."
R --no-save -e "library(devtools);devtools::check()"

echo "Build RODPS package..."
R --no-save -e "library(devtools);devtools::build(path = '.')"
