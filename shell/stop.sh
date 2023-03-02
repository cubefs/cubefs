#/bin/bash

ps -ef | grep cfs-server | grep -v grep | awk '{print $2}' | xargs kill -9
