#!/bin/bash
source /etc/profile
RootPath=/root
cd $RootPath
cd ./chubaofs
export CPUTYPE=arm64_gcc4
bash ./build.sh
export CPUTYPE=