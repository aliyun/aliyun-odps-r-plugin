#!/bin/bash

source ./config.sh
d=`date +%Y%m%d" "%H:%M:%S`
basepath=$(cd "$(dirname "$0")";pwd)
javasrc=$basepath/java
jarpath=$basepath/java/target
libpath=$basepath/RODPS/inst/lib
RPackageName="RODPS_${RVERSION}.tar.gz"
Log4jPath=$basepath/java/src/main/java
RODPSPath=$basepath/RODPS
ThirdLibPath=$basepath/java/target/lib

echo "Start build R package in $basepath at $d"

#mvn package
cp ./pom.xml $javasrc/
cd $javasrc
mvn clean
mvn package -DskipTests
cd $basepath

#copy jars & log4j config to libpath
rm -f $libpath/*
cp $ThirdLibPath/*.jar $libpath
cp $ThirdLibPath/../*.jar $libpath
cp log4j.properties $libpath

#tar R odps package
tar zcvf $RPackageName RODPS

mkdir -p target
mv $RPackageName target/


echo "Generate $RPackageName $basepath at $d"
