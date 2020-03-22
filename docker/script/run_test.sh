#!/bin/bash

# Copyright 2018 The Chubao Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs
cli=/cfs/bin/cfs-cli

Master1Addr="192.168.0.11:17010"
LeaderAddr=""
VolName=ltptest
Owner=ltptest
AccessKey=39bEF4RrAQgMj6RV
SecretKey=TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd
AuthKey="0e20229116d5a9a4a9e876806b514a85"

check_cluster() {
    echo -n "check cluster  ... "
    for i in $(seq 1 300) ; do
        ${cli} cluster info &> /tmp/cli_cluster_info
        LeaderAddr=`cat /tmp/cli_cluster_info | grep -i "master leader" | awk '{print$4}'`
        if [[ "x$LeaderAddr" != "x" ]] ; then
            echo -e "\033[32m[success]\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m[timeout]\033[0m"
    exit 1
}

ensure_node_writable() {
    node=$1
    echo -n "check $node ... "
    for i in $(seq 1 300) ; do
        ${cli} ${node} list &> /tmp/cli_${node}_list;
        res=`cat /tmp/cli_${node}_list | grep "Yes" | grep "Active" | wc -l`
        if [[ ${res} -eq 4 ]]; then
            echo -e "\033[32m[success]\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m[timeout]\033[0m"
    cat /tmp/cli_${node}_list
    exit 1
}

create_cluster_user() {
    echo -n "create user    ... "
    # check user exist
    ${cli} user info ${Owner} &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32m[exist]\033[0m"
        return
    fi
    # try create user
    for i in $(seq 1 300) ; do
        ${cli} user create ${Owner} --access-key=${AccessKey} --secret-key=${SecretKey} -y > /tmp/cli_user_create
        if [[ $? -eq 0 ]] ; then
            echo -e "\033[32m[success]\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m[timeout]\033[0m"
    exit 1
}

create_volume() {
    echo -n "create volume  ... "
    # check volume exist
    ${cli} volume info ${VolName} &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32m[exist]\033[0m"
        return
    fi
    ${cli} volume create ${VolName} ${Owner} --capacity=30 -y > /dev/null
    if [[ $? -ne 0 ]]; then
        echo -e "\033[31m[failed]\033[0m"
        exit 1
    fi
    echo -e "\033[32m[success]\033[0m"
}

show_cluster_info() {
    tmp_file=/tmp/collect_cluster_info
    ${cli} cluster info &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} metanode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} datanode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} user info ${Owner} &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} volume info ${Owner} &>> ${tmp_file}
    echo &>> ${tmp_file}
    cat /tmp/collect_cluster_info | grep -v "Master address"
}

add_data_partitions() {
    echo -n "add data partitions ... "
    ${cli} vol add-dp ${VolName} 20 &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32m[success]\033[0m"
        return
    fi
    echo -e "\033[31m[failed]\033[0m"
    exit 1
}

print_error_info() {
    echo "------ err ----"
    cat /cfs/log/cfs.out
    cat /cfs/log/client/client_info.log
    cat /cfs/log/client/client_error.log
    cat /cfs/log/client/client_warn.log
    curl -s "http://$LeaderAddr/admin/getCluster" | jq
    mount
    df -h
    stat $MntPoint
    ls -l $MntPoint
    ls -l $LTPTestDir
}

start_client() {
    echo -n "start client   ... "
    nohup /cfs/bin/cfs-client -c /cfs/conf/client.json >/cfs/log/cfs.out 2>&1 &
    sleep 10
    res=$( stat $MntPoint | grep -q "Inode: 1" ; echo $? )
    if [[ $res -ne 0 ]] ; then
        echo -e "\033[31m[failed]\033[0m"
        print_error_info
        exit $res
    fi
    echo -e "\033[32m[success]\033[0m"
}

wait_proc_done() {
    proc_name=$1
    pid=$( ps -ef | grep "$proc_name" | grep -v "grep" | awk '{print $2}' )
    logfile=$2
    logfile2=${logfile}-2
    logfile3=${logfile}-3
    maxtime=${3:-3000}
    checktime=${4:-60}
    retfile=${5:-"/tmp/ltpret"}
    timeout=1
    pout=0
    lastlog=""
    for i in $(seq 1 $maxtime) ; do
        if ! `ps -ef  | grep -v "grep" | grep -q "$proc_name" ` ; then
            echo "$proc_name run done"
            timeout=0
            break
        fi
        sleep 1
        ((pout+=1))
        if [ $(cat $logfile | wc -l) -gt 0  ] ; then
            pout=0
            cat $logfile > $logfile2 && cat $logfile2 >> $logfile3 && > $logfile
            cat $logfile2 && rm -f $logfile2
        fi
        if [[ $pout -ge $checktime ]] ; then
            echo -n "."
            pout=0
        fi
    done
    if [[ $timeout -eq 1 ]] ;then
        echo "$proc_name run timeout"
        print_error_info
        exit 1
    fi
    ret=$(cat /tmp/ltpret)
    if [[ "-$ret" != "-0" ]] ; then
        exit $ret
    fi
}

run_ltptest() {
    echo "run ltp test"
    LTPTestDir=$MntPoint/ltptest
    LtpLog=/tmp/ltp.log
    mkdir -p $LTPTestDir
    nohup /bin/sh -c " /opt/ltp/runltp  -f fs -d $LTPTestDir > $LtpLog 2>&1; echo $? > /tmp/ltpret " &
    wait_proc_done "runltp" $LtpLog
}

stop_client() {
    echo -n "stop client    ... "
    umount ${MntPoint} && echo -e "\033[32m[success]\033[0m" || { echo -e "\033[31m[failed]\033[0m"; exit 1; }
}

delete_volume() {
    echo -n "delete volume  ... "
    ${cli} volume delete ${VolName} -y &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32m[success]\033[0m"
        return
    fi
    echo -e "\033[31m[timeout]\033[0m"
    exit 1
}

run_s3_test() {
    work_path=/opt/s3tests;
    echo "run s3 compatibility test";

    # install system requirements
    apt-get update && apt-get install -y \
        sudo \
        debianutils \
        python3-pip \
        python3-virtualenv \
        python3-dev \
        libevent-dev \
        libffi-dev \
        libxml2-dev \
        libxslt-dev \
        zlib1g-dev \
        virtualenv

    # download s3-tests project
    mkdir -p ${work_path};
    wget "https://github.com/mervinkid/s3-tests/archive/for-chubaofs-2.0.tar.gz" \
        -O ${work_path}/s3-tests-for-chubaofs-2.0.tar.gz
    tar zxf ${work_path}/s3-tests-for-chubaofs-2.0.tar.gz -C ${work_path}/

    # init s3-tests environment
    cd ${work_path}/s3-tests-for-chubaofs-2.0
    /bin/bash ./bootstrap

    # execute tests
    tests=`cat ${work_path}/s3tests.txt`
    S3TEST_CONF=${work_path}/s3tests.conf ./virtualenv/bin/nosetests ${tests} -v --collect-only
}

check_cluster
create_cluster_user
ensure_node_writable "metanode"
ensure_node_writable "datanode"
create_volume ; sleep 2
add_data_partitions ; sleep 3
show_cluster_info
start_client ; sleep 2
run_ltptest
run_s3_test
stop_client
delete_volume
