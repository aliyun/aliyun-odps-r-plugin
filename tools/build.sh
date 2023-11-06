#!/bin/bash
basepath=$(cd "$(dirname "$0")";pwd)/..

source $basepath/config.version
VERSIONDATE=`date +"%Y-%m-%d %H:%M:%S"`

echo "Configuring RODPS package..."
sed -i '' -E "s|Version: .*|Version: ${RVERSION}|g" $basepath/DESCRIPTION
cat > $basepath/R/odps_version.R <<__EOF__
rodps.version <- function() {
    print("RODPS ${RVERSION}")
    print("BUILDDATE ${VERSIONDATE}")
}
__EOF__

buildpath=${basepath}/build
if [ -e ${buildpath} ]; then rm -rf ${buildpath}; fi
mkdir -p ${buildpath}

libpath=${basepath}/inst/java
if [ -e ${libpath} ]; then rm -rf $libpath; fi
mkdir -p ${libpath}

# mvn package
cd $basepath/java
mvn clean
mvn versions:set -DnewVersion=${RVERSION}
mvn package -DskipTests
cd $basepath

# copy jars & log4j config to libpath
cp $basepath/java/target/lib/*.jar $libpath
cp $basepath/java/target/lib/../*.jar $libpath
cp $basepath/java/src/main/resources/log4j.properties $libpath
rm -rf $basepath/java/target