#/bin/bash
set -e

if [ $# -ne 1 ]; then
    echo "useage: genIp.sh <bond0>"
    exit 1
fi

name=$1

processNum=`ps -aux | grep -v grep |grep "${newPid}" | wc -l`

if [ ${processNum} -eq 0 ]; then
    echo "service start fail after 3 seconds"
    exit 2
fi

genIp() {
	id=${1}
	ip=${2}
	cnt=`ifconfig | grep ${ip} | wc -l`
	if [ ${cnt} -eq 0 ]; then
		echo "add sub ip for ${name}, ip ${ip}"
		ifconfig ${name}:${id} ${ip} netmask 255.255.255.255 broadcast 172.16.1.255 up 
	fi
}

genIp 1 172.16.1.101
genIp 2 172.16.1.102
genIp 3 172.16.1.103
genIp 4 172.16.1.104
