#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..

sh $basepath/tools/package.sh

echo "Release RODPS to CRAN..."
R --no-save -e "library(devtools);devtools::release()"