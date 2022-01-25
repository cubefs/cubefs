#!/usr/bin/env bash
mkdir -p /go/src/github.com/cubefs/cubefs/docker/bin;
failed=0

echo -n 'Building ChubaoFS Server ... ';
cd /go/src/github.com/cubefs/cubefs/cmd;
bash ./build.sh &>> /tmp/cfs_build_output
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-server /go/src/github.com/cubefs/cubefs/docker/bin/cfs-server;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi


echo -n 'Building ChubaoFS Client ... ' ;
cd /go/src/github.com/cubefs/cubefs/client;
bash ./build.sh &>> /tmp/cfs_build_output
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-client /go/src/github.com/cubefs/cubefs/docker/bin/cfs-client;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi

echo -n 'Building ChubaoFS CLI    ... ';
cd /go/src/github.com/cubefs/cubefs/cli;
bash ./build.sh &>> /tmp/cfs_build_output;
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m";
    mv cfs-cli /go/src/github.com/cubefs/cubefs/docker/bin/cfs-cli;
else
    echo -e "\033[31mfail\033[0m";
    failed=1
fi

echo -n 'Building ChubaoFS libsdk ... ';
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
