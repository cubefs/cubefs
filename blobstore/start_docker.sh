#!/bin/bash

if [ ! -d ./run/logs ]; then
  mkdir -p ./run/logs
fi

# start kafka
uuid=`./bin/kafka_2.13-3.1.0/bin/kafka-storage.sh random-uuid`
./bin/kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $uuid -c bin/kafka_2.13-3.1.0/config/kraft/server.properties
./bin/kafka_2.13-3.1.0/bin/kafka-server-start.sh -daemon bin/kafka_2.13-3.1.0/config/kraft/server.properties
# check kafka running
sleep 1
num=`ps -ef | grep kafka | grep -v "grep|vi|tail" | wc -l`
if [ ${num} -le 1 ];then
	echo "Failed to start kafka."
	exit 1
fi

# Start the clustermgr
nohup ./bin/clustermgr -f conf/clustermgr1.conf >> ./run/logs/clustermgr1.log  2>&1 &
nohup ./bin/clustermgr -f conf/clustermgr2.conf >> ./run/logs/clustermgr2.log  2>&1 &
nohup ./bin/clustermgr -f conf/clustermgr3.conf >> ./run/logs/clustermgr3.log  2>&1 &
sleep 5
num=`ps -ef | egrep "./bin/clustermgr" |  egrep -v "vi|tail|grep" | wc -l`
if [ $num -ne 3 ]; then
  echo "Failed to start clustermgr"
  exit 1
fi

sleep 15
echo "start clustermgr ok"

# Start the proxy
nohup ./bin/proxy -f conf/proxy.conf >> ./run/logs/proxy.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/proxy |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The proxy start failed."
	exit 1
fi
echo "start proxy ok"

# Start the scheduler
nohup ./bin/scheduler -f conf/scheduler.conf >> ./run/logs/scheduler.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/scheduler |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The scheduler start failed."
	exit 1
fi
echo "start scheduler ok"

mkdir -p ./run/disks/disk{1..8}
# Start the blobnode
nohup ./bin/blobnode -f conf/blobnode.conf >> ./run/logs/blobnode.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/blobnode |  egrep -v "vi|tail|grep" | wc -l`
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
nohup ./bin/access -f conf/access.conf >> ./run/logs/access.log 2>&1 &
sleep 1
num=`ps -ef | egrep ./bin/access |  egrep -v "vi|tail|grep" | wc -l`
if [ ${num} -lt 1 ];then
	echo "The access start failed."
	exit 1
fi
echo "start blobstore service successfully, wait minutes for internal state preparation"

# wait for docker, avoid container exit
while true
do
    sleep 300
done
