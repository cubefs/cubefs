#/bin/bash

echo "stop all service"
ps -ef | grep cfs-server | grep -v grep | awk '{print $2}' | xargs kill -9

echo "stop client"
mount | grep cubefs | awk '{print $3}' | xargs umount -l
ps -ef | grep "build/bin/cfs-client" | grep -v grep | awk '{print $2}' | xargs kill -9
