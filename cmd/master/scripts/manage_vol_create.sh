#!/bin/bash
curl -v "http://127.0.0.1/admin/createVol?name=test&replicas=3&type=extent&randomWrite=true&capacity=100"