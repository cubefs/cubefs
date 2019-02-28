#!/bin/bash
curl -v "http://127.0.0.1/client/vol?name=test&authKey=md5(owner)" | python -m json.tool