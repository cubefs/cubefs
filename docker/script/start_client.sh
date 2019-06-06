#!/bin/bash

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs

Master1Addr="192.168.0.11:17010"
LeaderAddr=""

getLeaderAddr() {
    echo -n "check Master "
    for i in $(seq 1 300) ; do
        LeaderAddr=$(curl -s "http://$Master1Addr/admin/getCluster" | jq '.data.LeaderAddr' | tr -d '"' )
        if [ "x$LeaderAddr" != "x" ] ; then
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ "x$LeaderAddr" == "x" ] ; then
        echo -n "timeout, exit\n"
        exit 1
    fi
    echo "ok"
}

check_status() {
    node="$1"
    up=0
    echo -n "check $node "
    for i in $(seq 1 300) ; do
        clusterInfo=$(curl -s "http://$LeaderAddr/admin/getCluster")
        #NodeUpCount=$( echo "$clusterInfo" | jq ".data.${node}s | .[].Status" | grep "true" | wc -l)
        NodeTotoalGB=$( echo "$clusterInfo" | jq ".data.${node}StatInfo.TotalGB" )
        if [[ $NodeTotoalGB -gt 0 ]]  ; then
            up=1
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ $up -eq 0 ] ; then
        echo -n "timeout, exit"
        curl "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

create_vol() {
    echo -n "create vol "
    res=$(curl -s "http://$LeaderAddr/admin/createVol?name=ltptest&replicas=2&type=extent&randomWrite=true&capacity=30&owner=ltptest")
    code=$(echo "$res" | jq .code)
    if [[ $code -ne 0 ]] ; then
        echo " failed, exit"
        curl -s "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

create_dp() {
    echo -n "create datapartition "
    res=$(curl -s "http://$LeaderAddr/dataPartition/create?count=20&name=ltptest&type=extent" )
    code=$(echo "$res" | jq .code)
    if [[ $code -ne 0 ]] ; then
        echo " failed, exit"
        curl -s "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

print_error_info() {
    echo "------ err ----"
    cat /cfs/log/cfs.out
    cat /cfs/log/client/client_info.log
    cat /cfs/log/client/client_error.log
    curl -s "http://$LeaderAddr/admin/getCluster" | jq
    mount
    df -h
    stat $MntPoint
    ls -l $MntPoint
    ls -l $LTPTestDir
}

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

getLeaderAddr
check_status "MetaNode"
check_status "DataNode"
create_vol ; sleep 3
create_dp ; sleep 3
/cfs/bin/cfs-client -c /cfs/conf/client.json >/cfs/log/cfs.out
