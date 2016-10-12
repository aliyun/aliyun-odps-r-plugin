#! /bin/bash
source ./config.sh
mkdir -p build
sudo rm -rf build/*
sh -x gendoc.sh
sh -x ./build.sh

sudo R CMD INSTALL --build RODPS
if [ $debug == true ]
then
    sudo R CMD INSTALL target/RODPS_${RVERSION}.tar.gz
fi
sudo mv RODPS_${RVERSION}_R_x86_64-unknown-linux-gnu.tar.gz build/
cd build
sudo tar xzvf RODPS_${RVERSION}_R_x86_64-unknown-linux-gnu.tar.gz
sudo zip -r RODPS_${RVERSION}.zip RODPS
sudo rm -rf RODPS_${RVERSION}_R_x86_64-unknown-linux-gnu.tar.gz  RODPS
cd ..
sudo tar czvf R3.tar.gz build
