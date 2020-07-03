#!/bin/bash
#curl -v "http://192.168.0.11:17010/admin/setNodeInfo?batchCount=100"
curl -v "http://192.168.0.11:17010/admin/setNodeInfo?batchCount=100&markDeleteRate=100&deleteWorkerSleepMs=1000"

