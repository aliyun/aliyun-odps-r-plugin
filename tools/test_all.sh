#!/bin/bash
set -e
basepath=$(cd "$(dirname "$0")";pwd)/..
R -f $basepath/tests/test_rodps_basics.R
R -f $basepath/tests/test_rodps_table.R
R -f $basepath/tests/test_rodps_advanced.R

