#!/bin/bash
RootPath=/root
cd $RootPath
cd ./chubaofs
export CPUTYPE=arm64 
bash ./build.sh
export CPUTYPE=