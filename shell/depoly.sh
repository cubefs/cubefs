#/bin/bash

if [ $# -ne 2 ]; then
    echo "usage: start cluster: ./shell/depoly.sh <baseDir>  <bond0>"
    exit 1
fi

# first stop all old service
sh ./shell/stop.sh

# gen net subip 
sh ./shell/genIp.sh $2

# gen config file
sh ./shell/genConf.sh ${1}

confDir=${1}/conf

# start master service
echo "begin start master service"
./build/bin/cfs-server -c ${confDir}/master1.conf
./build/bin/cfs-server -c ${confDir}/master2.conf
./build/bin/cfs-server -c ${confDir}/master3.conf
echo "start master service success"

# start meta service
echo "begin start metanode service"
./build/bin/cfs-server -c ${confDir}/meta1.conf
./build/bin/cfs-server -c ${confDir}/meta2.conf
./build/bin/cfs-server -c ${confDir}/meta3.conf
./build/bin/cfs-server -c ${confDir}/meta4.conf
echo "start metanode service success"

# start data service
echo "begin start datanode service"
./build/bin/cfs-server -c ${confDir}/data1.conf
./build/bin/cfs-server -c ${confDir}/data2.conf
./build/bin/cfs-server -c ${confDir}/data3.conf
./build/bin/cfs-server -c ${confDir}/data4.conf
echo "start datanode service success"

./build/bin/cfs-cli config set --addr 172.16.1.101:17010


