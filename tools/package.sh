#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..

sh $basepath/tools/build.sh
sh $basepath/tools/gendoc.sh

echo "Check RODPS package..."
R --no-save -e "library(devtools);devtools::check(cran=FALSE, document=FALSE);devtools::spell_check()"

echo "Build RODPS package..."
R --no-save -e "library(devtools);devtools::build(path = 'build')"