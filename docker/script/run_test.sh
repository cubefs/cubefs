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
conf_path=/cfs/conf
cover_path=/cfs/coverage

Master1Addr="192.168.0.11:17010"
LeaderAddr=""
VolName=ltptest
Owner=ltptest
AccessKey=39bEF4RrAQgMj6RV
SecretKey=TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd
AuthKey="0e20229116d5a9a4a9e876806b514a85"

init_cli() {
    cp ${cli} /usr/bin/
    cd ${conf_path}
    ${cli} completion
    echo 'source '${conf_path}'/cfs-cli.sh' >> ~/.bashrc
}
check_cluster() {
    echo -n "Checking cluster  ... "
    for i in $(seq 1 300) ; do
        ${cli} cluster info &> /tmp/cli_cluster_info
        LeaderAddr=`cat /tmp/cli_cluster_info | grep -i "master leader" | awk '{print$4}'`
        if [[ "x$LeaderAddr" != "x" ]] ; then
            echo -e "\033[32mdone\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31mfail\033[0m"
    exit 1
}

ensure_node_writable() {
    node=$1
    echo -n "Checking $node ... "
    for i in $(seq 1 300) ; do
        ${cli} ${node} list &> /tmp/cli_${node}_list;
        res=`cat /tmp/cli_${node}_list | grep "Yes" | grep "Active" | wc -l`
        if [[ ${res} -eq 4 ]]; then
            echo -e "\033[32mdone\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31mfail\033[0m"
    cat /tmp/cli_${node}_list
    exit 1
}

create_cluster_user() {
    echo -n "Creating user     ... "
    # check user exist
    ${cli} user info ${Owner} &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    # try create user
    for i in $(seq 1 300) ; do
        ${cli} user create ${Owner} --access-key=${AccessKey} --secret-key=${SecretKey} -y > /tmp/cli_user_create
        if [[ $? -eq 0 ]] ; then
            echo -e "\033[32mdone\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31mfail\033[0m"
    exit 1
}

create_volume() {
    echo -n "Creating volume   ... "
    # check volume exist
    ${cli} volume info ${VolName} &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    ${cli} volume create ${VolName} ${Owner} --capacity=30 --store-mode=1 -y > /dev/null
    if [[ $? -ne 0 ]]; then
        echo -e "\033[31mfail\033[0m"
        exit 1
    fi
    echo -e "\033[32mdone\033[0m"
}

change_store_mode_to_rocksdb() {
   echo -n "change default store mode to rockdb   ... "
   ${cli} volume set ${VolName} --store-mode=2 -y > /dev/null
   if [[ $? -ne 0 ]]; then
        echo -e "\033[31mfail\033[0m"
        exit 1
   fi
   echo -e "\033[32mdone\033[0m"
}

get_max_meta_partition_id_and_check_leader() {
  max_mpid=`${cli} volume info ${VolName} -m |grep unlimited | awk -F " " '{print $1}'`
  for i in $(seq 1 300) ; do
        ${cli} metapartition info ${max_mpid} &> /tmp/cli_max_mp_info
        while read line;
        do
          result=`echo $line | awk -F " " '{print $2}'`
          if [ "$result" = "true" ]; then
            return
          fi
        done < /tmp/cli_max_mp_info
        sleep 1
   done
   exit 1
}

add_rocksdb_mode_meta_partitions() {
   echo -n "add rockdb store mode meta partitions for volume   ... "
   for i in $(seq 1 5); do
     get_max_meta_partition_id_and_check_leader
     ${cli} volume add-mp ${VolName} > /dev/null
     sleep 60
   done
   echo -e "\033[32mdone\033[0m"
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
    ${cli} volume info ${VolName} &>> ${tmp_file}
    echo &>> ${tmp_file}
    cat /tmp/collect_cluster_info | grep -v "Master address"
}

add_data_partitions() {
    echo -n "Increasing DPs    ... "
    ${cli} vol add-dp ${VolName} 2 &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    echo -e "\033[31mfail\033[0m"
    exit 1
}

print_error_info() {
    echo "------ err ----"
    cat /cfs/log/cfs.out
    cat /cfs/log/ltptest/ltptest_info.log
    cat /cfs/log/ltptest/ltptest_error.log
    cat /cfs/log/ltptest/ltptest_warn.log
    curl -s "http://$LeaderAddr/admin/getCluster" | jq
    mount
    df -h
    stat $MntPoint
    ls -l $MntPoint
    ls -l $LTPTestDir
}

start_client() {
    echo -n "Starting client   ... "
    nohup /cfs/bin/cfs-client -test.coverprofile=client.cov -test.outputdir=${cover_path} -c /cfs/conf/client.json >/cfs/log/cfs.out 2>&1 &
    sleep 10
    res=$( mount | grep -q "$VolName on $MntPoint" ; echo $? )
    if [[ $res -ne 0 ]] ; then
        echo -e "\033[31mfail\033[0m"
        print_error_info
        exit $res
    fi
    echo -e "\033[32mdone\033[0m"
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

run_unit_test() {
    echo "Running unit test"
    echo "************************";
    echo "       unit test       ";
    echo "************************";
    export GO111MODULE="off"
    pushd /go/src/github.com/chubaofs/chubaofs > /dev/null
    packages=`GO111MODULE="off" go list \
            ./master/... \
            ./datanode/... \
            ./metanode/... \
            ./objectnode/... \
            ./schedulenode/... \
            ./storage/... \
            ./sdk/data/... \
            ./sdk/meta/... \
            ./sdk/master/... \
            ./repl/... \
            ./raftstore/rafttest/... \
            ./util/... \
            ./vendor/github.com/tiglabs/raft/...`
    echo "Following packages will be tested and record code coverage:"
    for package in `echo ${packages}`; do
        echo "  * "${package};
    done

    test_output_file=${cover_path}/unittest.out
    echo "Running unit tests ..."
    go test -v -covermode=atomic -coverprofile=${cover_path}/unittest.cov ${packages} > ${test_output_file}
    ret=$?
    popd > /dev/null
    pass_num=`grep "PASS:" ${test_output_file} | wc -l`
    fail_num=`grep "FAIL:" ${test_output_file} | wc -l`
    total_num=`expr ${pass_num} + ${fail_num}`
    echo "Unit test complete returns ${ret}: ${pass_num}/${total_num} passed."
    egrep "FAIL:|PASS:" ${test_output_file}
    if [[ $ret -ne 0 ]]; then
        echo -e "Unit test: \033[32mFAIL\033[0m"
        exit $ret
    fi
    echo -e "Unit test: \033[32mPASS\033[0m"
}

run_ltptest() {
    echo "Running LTP test"
    echo "************************";
    echo "        LTP test        ";
    echo "************************";
    LTPTestDir=$MntPoint/ltptest
    LtpLog=/tmp/ltp.log
    mkdir -p $LTPTestDir
    nohup /bin/sh -c " /opt/ltp/runltp  -f fs -d $LTPTestDir > $LtpLog 2>&1; echo $? > /tmp/ltpret " &
    wait_proc_done "runltp" $LtpLog
}

stop_client() {
    echo -n "Stopping client   ... "
    umount ${MntPoint}
    echo -e "\033[32mdone\033[0m" || { echo -e "\033[31mfail\033[0m"; exit 1; }
}

delete_volume() {
    echo -n "Deleting volume   ... "
    ${cli} volume delete ${VolName} -y &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    echo -e "\033[31mfail\033[0m"
    exit 1
}

run_s3_test() {
    work_path=/opt/s3tests;
    echo "Running S3 compatibility tests"
    echo "******************************";
    echo "    S3 compatibility tests    ";
    echo "******************************";

    python3 -m unittest2 discover ${work_path} "*.py" -v
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
}

set_trash_days() {
   echo -n "set trash days... "
   ${cli} volume set ${VolName} --trash-days=2 -y > /dev/null
   if [[ $? -ne 0 ]]; then
        echo -e "\033[31mfail\033[0m"
        exit 1
   fi
   echo -e "\033[32mdone\033[0m"
}

run_trash_test() {
   echo -n "run trash test... "
   ${cli} trash test --vol ${VolName} > /dev/null
   if [[ $? -ne 0 ]]; then
        echo -e "\033[31mfail\033[0m"
	cp -r /tmp/cfs/cli /cfs/log/
        exit 1
   fi
   cp -r /tmp/cfs/cli /cfs/log/
   echo -e "\033[32mdone\033[0m"
}

init_cli
check_cluster
create_cluster_user
ensure_node_writable "metanode"
ensure_node_writable "datanode"
create_volume ; sleep 2
add_data_partitions ; sleep 3
change_store_mode_to_rocksdb ; sleep 2
add_rocksdb_mode_meta_partitions ; sleep 2
show_cluster_info
start_client ; sleep 2
run_unit_test
run_ltptest
run_s3_test
set_trash_days; sleep 310
run_trash_test; sleep 2
stop_client ; sleep 20
delete_volume
