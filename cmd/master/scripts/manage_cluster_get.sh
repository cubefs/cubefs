#!/bin/bash
curl -v "http://127.0.0.1/admin/getCluster" | python -m json.tool