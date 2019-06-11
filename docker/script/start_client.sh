#!/bin/bash

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs

start_client() {
    echo -n "start client "
    nohup /cfs/bin/cfs-client -c /cfs/conf/client.json >/cfs/log/cfs.out 2>&1 &
    sleep 10
    res=$( stat $MntPoint | grep -q "Inode: 1" ; echo $? )
    if [[ $res -ne 0 ]] ; then
        echo "failed"
        print_error_info
        exit $res
    fi

    echo "ok"
}

/cfs/bin/cfs-client -c /cfs/conf/client.json >/cfs/log/cfs.out
