#!/bin/bash
BASEDIR=$(cd `dirname $0`;pwd)
cd $BASEDIR
BINNAME=prometheus
[ -f $BINNAME ] || {  echo "$BINNAME no such file."; exit 1; }
[ -x $BINNAME ] || chmod a+x $BINNAME
#systemctl start prometheus.service
nohup $BASEDIR/prometheus --config.file=$BASEDIR/prometheus.yml --web.enable-admin-api --web.listen-address=:17090 &> nohup.out  &

