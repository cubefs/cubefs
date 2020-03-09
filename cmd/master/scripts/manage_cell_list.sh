#!/bin/bash
curl -v "http://127.0.0.1/zone/list" | python -m json.tool