#!/bin/bash

function INIT()
{
    # build blobstore
    rootPath=$(cd $(dirname $0); pwd)
    source $rootPath/env.sh
    ./build.sh
    if [ $? -ne 0 ]; then
      echo "build failed"
      exit 1
    fi

    # get consul
    if [ ! -f bin/consul ]; then
        wget https://ocs-cn-south1.heytapcs.com/blobstore/consul_1.11.4_linux_amd64.zip
        unzip consul_1.11.4_linux_amd64.zip
        rm -f consul_1.11.4_linux_amd64.zip
        mv consul bin/
        if [ $? -ne 0 ]; then
          echo "prepare consul failed"
          exit 1
        fi
    fi

    # get kafka
    grep -q "export JAVA_HOME" /etc/profile
    if [[ $? -ne 0 ]] && [[ ! -d bin/jdk1.8.0_321 ]]; then
         wget https://ocs-cn-south1.heytapcs.com/blobstore/jdk-8u321-linux-x64.tar.gz
         tar -zxvf jdk-8u321-linux-x64.tar.gz -C bin/
         if [ $? -ne 0 ]; then
          echo "prepare kafka failed"
          exit 1
         fi
         rm -f jdk-8u321-linux-x64.tar.gz
    fi
    # init java
    grep -q "export JAVA_HOME" /etc/profile
    if [ $? -ne 0 ]; then
       if [ ! -f ./bin/profile ]; then
         touch ./bin/profile
       fi
       echo "export JAVA_HOME=$rootPath/bin/jdk1.8.0_321" > bin/profile
       echo "export PATH=$JAVA_HOME/bin:$PATH" >> bin/profile
       echo "export CLASSPATH=$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar" >> bin/profile
       source bin/profile
    fi

    if [ ! -d bin/kafka_2.13-3.1.0 ]; then
        wget https://ocs-cn-south1.heytapcs.com/blobstore/kafka_2.13-3.1.0.tgz
        tar -zxvf kafka_2.13-3.1.0.tgz -C bin/
        if [ $? -ne 0 ]; then
          echo "prepare kafka failed"
          exit 1
        fi
        rm -f kafka_2.13-3.1.0.tgz
    fi
}
