#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)

sh $basepath/configure

echo "Build RODPS package..."
R --no-save -f $basepath/tools/package.R