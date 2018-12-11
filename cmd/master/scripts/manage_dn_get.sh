#!/bin/bash
curl -v "http://127.0.0.1/dataNode/get?addr=127.0.0.1:5000"  | python -m json.tool