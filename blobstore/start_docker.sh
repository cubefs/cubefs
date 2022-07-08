#!/bin/bash

# start consul
nohup ./consul agent -dev -client 0.0.0.0 >> /tmp/consul.log  2>&1 &

uuid=`./kafka_2.13-3.1.0/bin/kafka-storage.sh random-uuid`
./kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $uuid -c kafka_2.13-3.1.0/config/kraft/server.properties
./kafka_2.13-3.1.0/bin/kafka-server-start.sh -daemon kafka_2.13-3.1.0/config/kraft/server.properties
# start mongo
mkdir -p ./mongo/db
./mongodb-linux-x86_64-rhel70-3.6.23/bin/mongod --dbpath ./mongo/db --logpath /tmp/mongod.log --fork
# Start the clustermgr
nohup ./clustermgr -f conf/clustermgr.conf >> /tmp/clustermgr.log  2>&1 &
nohup ./clustermgr -f conf/clustermgr1.conf >> /tmp/clustermgr1.log  2>&1 &
nohup ./clustermgr -f conf/clustermgr2.conf >> /tmp/clustermgr2.log  2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start clustermgr: $status"
  exit $status
fi

sleep 30
mkdir -p ./run/disks/disk{1..6}
# Start the blobnode
nohup ./blobnode -f  conf/blobnode.conf >> /tmp/blobnode.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start blobnode: $status"
  exit $status
fi

# Start the proxy
nohup ./proxy -f conf/proxy.conf >> /tmp/prxoy.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start proxy: $status"
  exit $status
fi

sleep 60
# Start the access
nohup ./access -f conf/access.conf >> /tmp/access.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start access: $status"
  exit $status
fi

# Start the scheduler
nohup ./scheduler -f conf/scheduler.conf >> /tmp/scheduler.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start scheduler: $status"
  exit $status
fi



# Naive check runs checks once a minute to see if either of the processes exited.
# This illustrates part of the heavy lifting you need to do if you want to run
# more than one service in a container. The container exits with an error
# if it detects that either of the processes has exited.
# Otherwise it loops forever, waking up every 60 seconds

processes="clustermgr blobnode access proxy scheduler"
for process in $processes;
do
  ps aux |grep $process |grep -q -v grep
  PROCESS_STATUS=$?
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_STATUS -ne 0 ]; then
    echo "the $process has already exited."
    exit 1
  fi
done


