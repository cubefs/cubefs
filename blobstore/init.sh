#!/bin/bash

function INIT()
{
    # build blobnode
    rootPath=$(cd $(dirname $0); pwd)
    source $rootPath/env.sh
    ./build.sh

    # get consul
    wget https://releases.hashicorp.com/consul/1.11.4/consul_1.11.4_linux_amd64.zip
    unzip consul_1.11.4_linux_amd64.zip; rm -f consul_1.11.4_linux_amd64.zip
    mv consul bin/

    # get kafka
    wget https://ocs-cn-south1.heytapcs.com/blobstore/jdk-8u321-linux-x64.tar.gz
    tar -zxvf jdk-8u321-linux-x64.tar.gz -C bin/
    rm -f jdk-8u321-linux-x64.tar.gz

    wget https://ocs-cn-south1.heytapcs.com/blobstore/kafka_2.13-3.1.0.tgz
    tar -zxvf kafka_2.13-3.1.0.tgz -C bin/
    rm -f kafka_2.13-3.1.0.tgz
    # get mongo
    wget https://ocs-cn-south1.heytapcs.com/blobstore/mongodb-linux-x86_64-rhel70-3.6.23.tgz
    tar -zxvf mongodb-linux-x86_64-rhel70-3.6.23.tgz -C bin/
    rm -f mongodb-linux-x86_64-rhel70-3.6.23.tgz
}