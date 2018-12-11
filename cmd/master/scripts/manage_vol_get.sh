#!/bin/bash
curl -v "http://127.0.0.1/client/vol?name=test" | python -m json.tool