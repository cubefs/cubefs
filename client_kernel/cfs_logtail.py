/*
 * Copyright 2023 The CubeFS Authors.
 */
import os
import select
import signal
import sys

if len(sys.argv) != 2:
    print("Usage: python cfs_logtail.py <file_name>")
    sys.exit(1)

if not os.path.exists(sys.argv[1]):
    print("File not found")
    sys.exit(1)

def signal_handler(signal, frame):
    file.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

file = open(sys.argv[1])

while True:
    ready = select.select([file], [], [], 1)[0]
    if ready:
        data = ready[0].readline()
        if data:
            print(data, end="")
