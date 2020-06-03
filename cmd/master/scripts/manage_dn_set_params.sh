#!/bin/bash
#curl -v "http://127.0.0.1/dataNode/setParams?markDeleteRate=100"  | python -m json.tool
curl -v "http://127.0.0.1/dataNode/setParams?markDeleteRate=100&hosts=127.0.0.1:9021,127.0.0.2:9021"  | python -m json.tool
