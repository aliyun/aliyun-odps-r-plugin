#! /bin/bash
source ./config.sh
mkdir -p build
rm -rf build/*
sh -x gendoc.sh
sh -x ./build.sh

#R CMD INSTALL --build RODPS
TAR=`echo RODPS_${RVERSION}_R_*.tar.gz`
#mv $TAR build/
#cd build
#tar xzvf $TAR
#zip -r RODPS_${RVERSION}.zip RODPS
#rm -rf $TAR  RODPS
#cd ..
#tar czvf R3.tar.gz build



tar czvf RODPS_${RVERSION}.tar.gz build
