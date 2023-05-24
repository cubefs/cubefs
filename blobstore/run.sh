#!/bin/bash

consul=$1
source ./init.sh

# build blobstore and get consul kafka
INIT

if [ ! -d ./run/logs ];then
  mkdir -p ./run/logs
fi
cd build
# start consul
if [ "${consul}" == "--consul" ]; then
  nohup ./bin/blobstore/consul agent -dev -client 0.0.0.0 >> ../blobstore/run/logs/consul.log 2>&1 &
  # check consul running
  sleep 1
  num=`ps -ef | egrep ./bin/blobstore/consul | egrep -v "grep|vi|tail" | wc -l`
  if [ ${num} -lt 1 ];then
    echo "Failed to start consul."
    exit 1
  fi
fi

# start kafka
uuid=`./bin/blobstore/kafka_2.13-3.1.0/bin/kafka-storage.sh random-uuid`
./bin/blobstore/kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $uuid -c bin/blobstore/kafka_2.13-3.1.0/config/kraft/server.properties
./bin/blobstore/kafka_2.13-3.1.0/bin/kafka-server-start.sh -daemon bin/blobstore/kafka_2.13-3.1.0/config/kraft/server.properties
# check kafka running
sleep 1
num=`ps -ef | grep kafka | grep -v "grep|vi|tail" | wc -l`
if [ ${num} -le 1 ];then
	echo "Failed to start kafka."
	exit 1
fi

# Start the clustermgr
nohup ./bin/blobstore/clustermgr -f ../blobstore/cmd/clustermgr/clustermgr1.conf >> ../blobstore/run/logs/clustermgr1.log  2>&1 &
nohup ./bin/blobstore/clustermgr -f ../blobstore/cmd/clustermgr/clustermgr2.conf >> ../blobstore/run/logs/clustermgr2.log  2>&1 &
nohup ./bin/blobstore/clustermgr -f ../blobstore/cmd/clustermgr/clustermgr3.conf >> ../blobstore/run/logs/clustermgr3.log  2>&1 &
sleep 5
num=`ps -ef | egrep "./bin/blobstore/clustermgr" |  egrep -v "vi|tail|grep" | wc -l`
if [ $num -ne 3 ]; then
  echo "Failed to start clustermgr"
  exit 1
fi

sleep 15
echo "start clustermgr ok"

# Start the proxy
nohup ./bin/blobstore/proxy -f ../blobstore/cmd/proxy/proxy.conf >> ../blobstore/run/logs/proxy.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/blobstore/proxy |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The proxy start failed."
	exit 1
fi
echo "start proxy ok"

# Start the scheduler
nohup ./bin/blobstore/scheduler -f ../blobstore/cmd/scheduler/scheduler.conf >> ../blobstore/run/logs/scheduler.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/blobstore/scheduler |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The scheduler start failed."
	exit 1
fi
echo "start scheduler ok"

mkdir -p ./run/disks/disk{1..8}
# Start the blobnode
nohup ./bin/blobstore/blobnode -f ../blobstore/cmd/blobnode/blobnode.conf >> ../blobstore/run/logs/blobnode.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/blobstore/blobnode |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The blobnode start failed."
	exit 1
fi
echo "start blobnode ok"


if [ "${consul}" == "--consul" ]; then
  echo "Wait clustermgr register to consul..."
  sleep 80
fi

# Start the access
nohup ./bin/blobstore/access -f ../blobstore/cmd/access/access.conf >> ../blobstore/run/logs/access.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/blobstore/access |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The access start failed."
	exit 1
fi
echo "start blobstore service successfully, wait minutes for internal state preparation"
