#!/bin/bash
curl -v "http://127.0.0.1/metaNode/get?addr=127.0.0.1:9021"  | python -m json.tool