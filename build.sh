#! /bin/bash

d=`date +%Y%m%d" "%H:%M:%S`
basepath=$(cd "$(dirname "$0")";pwd)
javasrc=$basepath/java
jarpath=$basepath/java/target
libpath=$basepath/RODPS/inst/lib
ThirdLibPath=$basepath/java/target/lib
RODPSPath=$basepath/RODPS
RPackageName="RODPS_1.2.tar.gz"

echo "Start build R package in $basepath at $d"

#mvn package
cd $javasrc
mvn clean
mvn package -DskipTests
cd $basepath

#copy jars & log4j config to libpath
rm -f $libpath/*
cp $jarpath/*.jar $libpath
cp $ThirdLibPath/*.jar $libpath
cp log4j.properties $libpath

#tar R odps package
tar zcvf $RPackageName RODPS

mkdir target
mv $RPackageName target/


echo "Generate $RPackageName $basepath at $d"
