#!/bin/bash
# shellcheck disable=SC2046
{
  kill $(ps -ef | egrep clustermgr |  egrep -v "grep|vi|tail" | awk '{print $2}')
  if [ $? -eq 0 ]; then
      echo "stop clustermgr..."
  fi
  kill $(ps -ef | egrep blobnode |  egrep -v "grep|vi|tail" | awk '{print $2}')
  if [ $? -eq 0 ]; then
    echo "stop blobnode..."
  fi
  kill $(ps -ef | egrep proxy |  egrep -v "grep|vi|tail" | awk '{print $2}')
  if [ $? -eq 0 ]; then
    echo "stop proxy..."
  fi
  kill $(ps -ef | egrep scheduler |  egrep -v "grep|vi|tail" | awk '{print $2}')
  if [ $? -eq 0 ]; then
    echo "stop scheduler..."
  fi
  kill $(ps -ef | egrep access |  egrep -v "grep|vi|tail" | awk '{print $2}')
  if [ $? -eq 0 ]; then
    echo "stop access..."
  fi

  read -p "stop consul (Y/N)? " res
  if [ $res == "Y" ]; then
      kill $(ps -ef | egrep consul |  egrep -v "grep|vi|tail" | awk '{print $2}')
      if [ $? -eq 0 ]; then
        echo "stop consul..."
      fi
  fi

  read -p "stop kafka (Y/N)? " res
  if [ $res == "Y" ]; then
     kill $(ps -ef | grep kafka |  grep -v "grep|vi|tail" | sed -n 1p |awk '{print $2}')
     if [ $? -eq 0 ]; then
        echo "stop kafka..."
      fi
  fi
}

