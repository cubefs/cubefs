#!/bin/bash
BASEDIR=$(cd `dirname $0`;pwd)
cd $BASEDIR
BINNAME=cfs-client
[ -f $BINNAME ] || {  echo "$BINNAME no such file."; exit 1; }
[ -x $BINNAME ] || chmod a+x $BINNAME
nohup $BASEDIR/$BINNAME -c $BASEDIR/config.json >nohup.out &

