#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "useage: genConf.sh <baseDir>"
    exit 1
fi

baseDir=$1
confDir=${baseDir}/conf
tplDir=shell/tpl

if [ ! -d "$confDir" ]; then
    echo "mkdir -p $confDir"
    mkdir -p $confDir
fi

ip1=172.16.1.101
ip2=172.16.1.102
ip3=172.16.1.103
ip4=172.16.1.104

peers="1:$ip1:17010,2:${ip2}:17010,3:${ip3}:17010"
echo "peers $peers"

genMaster()
{
  echo "start gen master$1.conf"
  masterDir="${baseDir}/master${1}"
  confFile="${confDir}/master$1.conf"
  if [ ! -f "$confFile" ]; then
    sed "s/_id_/${1}/g" ${tplDir}/master.tpl | sed "s/_ip_/${2}/g" | sed "s/_peers_/${peers}/g" | sed "s|_dir_|${masterDir}|g" > "$confFile"
    echo "gen master$1.conf success"
  else
    echo "master$1.conf already exists, skipping generation"
  fi
} 

genMaster 1 $ip1
genMaster 2 $ip2
genMaster 3 $ip3


masterAddr="\"${ip1}:17010\",\"${ip2}:17010\",\"${ip3}:17010\""

genData()
{
  echo "start gen data$1.conf"
  dataDir=$baseDir/data$1
  if [ ! -d "$dataDir/disk" ]; then
    echo "mkdir -p $dataDir/disk"
    mkdir -p $dataDir/disk
  fi

  confFile="${confDir}/data$1.conf"
  if [ ! -f "$confFile" ]; then
    sed "s/_ip_/${2}/g" ${tplDir}/data.tpl | sed "s|_dir_|${dataDir}|g" | sed "s|_master_addr_|${masterAddr}|g" > "$confFile"
    echo "gen data$1.conf success"
  else
    echo "data$1.conf already exists, skipping generation"
  fi
}

genData 1 $ip1
genData 2 $ip2
genData 3 $ip3
genData 4 $ip4


genMeta()
{
  echo "start gen meta$1.conf"
  metaDir=$baseDir/meta$1
  confFile="${confDir}/meta$1.conf"
  if [ ! -f "$confFile" ]; then
    sed "s/_ip_/${2}/g" ${tplDir}/meta.tpl | sed "s|_dir_|${metaDir}|g" | sed "s|_master_addr_|${masterAddr}|g" > "$confFile"
    echo "gen meta$1.conf success"
  else
    echo "meta$1.conf already exists, skipping generation"
  fi
}

genMeta 1 $ip1
genMeta 2 $ip2
genMeta 3 $ip3
genMeta 4 $ip4

masterHost="${ip1}:17010,${ip2}:17010,${ip3}:17010"

genClient()
{
  confFile="${confDir}/client.conf"
  echo "start gen client.conf"
  if [ ! -f "$confFile" ]; then
    sed "s|_master_host_|${masterHost}|g" ${tplDir}/client.tpl | sed "s|_dir_|${baseDir}|g" > "$confFile"
    echo "gen client.conf success"
  else
    echo "client.conf already exists, skipping generation"
  fi
}

genClient

genObject()
{
  confFile="${confDir}/object.conf"
  echo "start gen object.conf"
  if [ ! -f "$confFile" ]; then
    sed "s|_master_addr_|${masterAddr}|g" ${tplDir}/object.tpl | sed "s|_dir_|${baseDir}|g" > "$confFile"
    echo "gen object.conf success"
  else
    echo "object.conf already exists, skipping generation"
  fi
}

genObject
