#!/bin/bash
source ./init.sh

# build blobstore and get consul kafka mongo
INIT

# start consul
nohup ./bin/consul agent -dev -client 0.0.0.0 >> /tmp/consul.log 2>&1 &
# start kafka
uuid=`./bin/kafka_2.13-3.1.0/bin/kafka-storage.sh random-uuid`
./bin/kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $uuid -c kafka_2.13-3.1.0/config/kraft/server.properties
./bin/kafka_2.13-3.1.0/bin/kafka-server-start.sh -daemon kafka_2.13-3.1.0/config/kraft/server.properties
# start mongo
mkdir -p ./mongo/db
./bin/mongodb-linux-x86_64-rhel70-3.6.23/bin/mongod --dbpath ./mongo/db --logpath /tmp/mongod.log --fork

# Start the clustermgr
nohup ./bin/clustermgr -f ./cmd/clustermgr/clustermgr.conf >> /tmp/clustermgr.log  2>&1 &
nohup ./bin/clustermgr -f ./cmd/clustermgr/clustermgr1.conf >> /tmp/clustermgr1.log  2>&1 &
nohup ./bin/clustermgr -f ./cmd/clustermgr/clustermgr2.conf >> /tmp/clustermgr2.log  2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start clustermgr: $status"
  exit $status
fi

sleep 30
mkdir -p ./run/disks/disk{1..8}
# Start the blobnode
nohup ./bin/blobnode -f ./cmd/blobnode/blobnode.conf >> /tmp/blobnode.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start blobnode: $status"
  exit $status
fi

# Start the proxy
nohup ./bin/proxy -f ./cmd/proxy/proxy.conf >> /tmp/proxy.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start proxy: $status"
  exit $status
fi

# Wait clustermgr register to consul
sleep 60

# Start the access
nohup ./bin/access -f ./cmd/access/access.conf >> /tmp/access.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start access: $status"
  exit $status
fi


# Start the scheduler
# must ensure start mqproxy succeed and has mongodb
# mongodb should create 'database.db_name' and 'task_archive_store_db_name'
nohup ./bin/scheduler -f ./cmd/scheduler/scheduler.conf >> /tmp/scheduler.log 2>&1 &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start scheduler: $status"
  exit $status
fi



