#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)

sh $basepath/configure

echo "Check RODPS package..."
R CMD check $basepath

#echo "Regenerate documents"
#R --no-save -e "library(devtools);document(roclets=c('collate','namespace','rd'))"

echo "Build RODPS package..."
R CMD build $basepath

#echo "Do release RODPS to CRAN..."
#R --no-save -e "library(devtools);spell_check();release()"