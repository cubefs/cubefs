#!/usr/bin/env bash
mkdir -p /go/src/github.com/cubefs/cubefs/docker/bin;
failed=0

export GO111MODULE=off

echo -n 'Building CubeFS Server ... ';
cd /go/src/github.com/cubefs/cubefs/cmd;
bash ./build.sh &>> /tmp/cfs_build_output
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-server /go/src/github.com/cubefs/cubefs/docker/bin/cfs-server;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi


echo -n 'Building CubeFS Client ... ' ;
cd /go/src/github.com/cubefs/cubefs/client;
bash ./build.sh &>> /tmp/cfs_build_output
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-client /go/src/github.com/cubefs/cubefs/docker/bin/cfs-client;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi

echo -n 'Building CubeFS CLI    ... ';
cd /go/src/github.com/cubefs/cubefs/cli;
bash ./build.sh &>> /tmp/cfs_build_output;
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-cli /go/src/github.com/cubefs/cubefs/docker/bin/cfs-cli;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi

echo -n 'Building CubeFS libsdk ... ';
cd /go/src/github.com/cubefs/cubefs/libsdk;
bash ./build.sh &>> /tmp/cfs_build_output;
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv libcfs.so /go/src/github.com/cubefs/cubefs/docker/bin/;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi

if [[ ${failed} -eq 1 ]]; then
    echo -e "\nbuild output:"
    cat /tmp/cfs_build_output;
    exit 1
fi

exit 0
