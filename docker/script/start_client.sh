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

LeaderAddr=""
VolName=ltptest
Owner=ltptest
AccessKey=39bEF4RrAQgMj6RV
SecretKey=TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd
TryTimes=5

init_cli() {
    cp ${cli} /usr/bin/
    cd ${conf_path}
    ${cli} completion &> /dev/null
#    echo 'source '${conf_path}'/cfs-cli.sh' >> ~/.bashrc
    echo -n "Installing CubeFS cli tool  ... "
    echo -e "\033[32mdone\033[0m"
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

create_volume() {
    echo -n "Creating volume   ... "
    # check volume exist
    ${cli} volume info ${VolName} &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    ${cli} volume create ${VolName} ${Owner} --capacity=30 -y > /dev/null
    if [[ $? -ne 0 ]]; then
        echo -e "\033[31mfail\033[0m"
        exit 1
    fi
    echo -e "\033[32mdone\033[0m"
}

create_cold_volume() {
    echo -n "Creating cold volume   ... "
    # check volume exist
    ${cli} volume info ${VolName} &> /dev/null
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32mdone\033[0m"
        return
    fi
    md5=`echo -n ${Owner} | md5sum | cut -d ' ' -f1`
    curl -v "http://192.168.0.11:17010/admin/createVol?name=${VolName}&volType=1&cacheCap=8&cacheAction=2&capacity=10&owner=${Owner}&mpCount=3"
    curl -v "http://192.168.0.11:17010/client/vol?name=${VolName}&authKey=${md5}" | python -m json.tool
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

start_client() {
    echo -n "Starting client   ... "
    /cfs/bin/cfs-client -c  /cfs/conf/client.json
    for((i=0; i<$TryTimes; i++)) ; do
        sleep 2
        sta=$(stat $MntPoint 2>/dev/null | tr ":：" " "  | awk '/Inode/{print $4}')
        if [[ "x$sta" == "x1" ]] ; then
            ok=1
	        echo -e "\033[32mdone\033[0m"
            exit 0
        fi
    done
    echo -e "\033[31mfail\033[0m"
    exit 1
}

init_cli
check_cluster
create_cluster_user
ensure_node_writable "metanode"
ensure_node_writable "datanode"
#create_volume
create_cold_volume
show_cluster_info
start_client

