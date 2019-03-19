#!/bin/bash
curl -v "http://127.0.0.1/dataPartition/get?id=100"  | python -m json.tool