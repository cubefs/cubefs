#!/bin/bash

host_name=perftest-client.$(hostname -i)
hostname ${host_name}

yum install -y salt-minion

salt_master=${SALT_MASTER:-"192.168.0.163"}
salt_minion_conf=/etc/salt/minion

sed -i '/^master:.*/d' ${salt_minion_conf}
echo "master: ${salt_master}" >> ${salt_minion_conf}

killall salt-minion
rm -rf /etc/salt/pki/minion/*

salt-minion start -d

