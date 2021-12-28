#! /bin/bash

for i in {1..10000};do
    go test -gcflags=all='-l' -cover -o t
    if [[ "$?" != "0" ]]; then
       exit 233
    fi
    echo "@@@@@@@@@@@@@  ${i}th run done @@@@@@@@@@@@@@@"
done
