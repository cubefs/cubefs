#!/bin/bash
source /etc/profile
RootPath=/root
cd $RootPath
cd ./cubefs
export CPUTYPE=arm64_gcc9 
bash ./build.sh
export CPUTYPE=