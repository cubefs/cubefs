#!/bin/bash
curl -v "http://127.0.0.1/cluster/stat" | python -m json.tool