#!/bin/bash
source /etc/profile
RootPath=/root
cd $RootPath
cd ./cubefs
export CPUTYPE=arm64_gcc4
bash ./build.sh
export CPUTYPE=