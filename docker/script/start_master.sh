#!/bin/sh

mkdir -m 700 /root/.ssh
cat /root/authorized_keys > /root/.ssh/authorized_keys
chown root:root /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

service ssh start

mkdir -p /cfs/conf
cp /root/master.json /cfs/conf/master.json
sed -i 's/\"enableSimpleAuth\": false/\"enableSimpleAuth\": true/' /cfs/conf/master.json

mkdir -p /cfs/log /cfs/data/wal /cfs/data/store /cfs/bin
echo "start master with simple auth enabled"
/cfs/bin/cfs-server -f -c /cfs/conf/master.json

