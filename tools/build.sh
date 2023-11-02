#! /bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..
source $basepath/config.sh

mkdir -p $basepath/build > /dev/null
rm -rf $basepath/build/*

# Generate documentation
echo "Generating documentation..."
sh -x $basepath/docs/gendoc.sh

javasrc=$basepath/java
libpath=$basepath/RODPS/inst/lib
RPackageName="RODPS_${RVERSION}.tar.gz"
ThirdLibPath=$basepath/java/target/lib

echo "Start build R package in $basepath at $d..."

# mvn package
cd $javasrc
mvn clean
mvn versions:set -DnewVersion=${RVERSION}
mvn package -DskipTests
cd $basepath

# copy jars & log4j config to libpath
rm -f $libpath/*
cp $ThirdLibPath/*.jar $libpath
cp $ThirdLibPath/../*.jar $libpath
cp $javasrc/src/main/resources/log4j.properties $libpath

# tar R odps package
tar zcvf $RPackageName RODPS

mkdir -p target
mv $RPackageName target/

echo "Generate $basepath/target/$RPackageName"
echo "Success"
