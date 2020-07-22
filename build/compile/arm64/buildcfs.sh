#!/bin/bash
source /etc/profile
RootPath=/root
cd $RootPath
cd ./chubaofs
export CPUTYPE=arm64_gcc9 
bash ./build.sh
export CPUTYPE=