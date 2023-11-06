#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..

sh $basepath/configure

echo "Check RODPS package..."
R --no-save -e "library(devtools);devtools::check(cran = FALSE);devtools::spell_check()"

echo "Regenerate documentations..."
#R --no-save -e "library(devtools);devtools::document()"

echo "Build RODPS package..."
R --no-save -e "library(devtools);devtools::build(path = 'build')"