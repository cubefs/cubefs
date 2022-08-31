#!/bin/bash
# just use for single machine deployment with default config
read -p "Are you sure clean all the data (Y/N)? " res
if [ $res == "Y" ]; then
  echo "cleaning..."
  rm -rf ./run
  echo "done"
fi

