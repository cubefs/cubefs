#! /bin/bash

#write authkey to authnode.json
cd ..
cd ..
cp ./build/bin/cfs-server /home/wuwenjia/gocode/src/github.com/cubefs/cubefs/docker/authnode/.
cp ./build/bin/cfs-authtool /home/wuwenjia/gocode/src/github.com/cubefs/cubefs/docker/authnode/.
cd docker/authnode
./cfs-authtool authkey
authnodeKey=$(sed -n '3p' authservice.json | sed 's/auth_key/authServiceKey/g')
authnodeRootKey=$(sed -n '3p' authroot.json | sed 's/auth_key/authRootKey/g')
line=`expr $(cat authnode1.json | wc -l) - 1`
sed -i "${line}i ${authnodeRootKey}" authnode1.json
sed -i "${line}i ${authnodeRootKey}" authnode2.json
sed -i "${line}i ${authnodeRootKey}" authnode3.json
sed -i "${line}i ${authnodeKey}" authnode1.json
sed -i "${line}i ${authnodeKey}" authnode2.json
sed -i "${line}i ${authnodeKey}" authnode3.json

#start authnode
docker-compose up -d
sleep 2s

#get ticket for auth
./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=./authservice.json -output=./ticket_auth.json getticket AuthService
sleep 2s
#create admin
./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=./ticket_auth.json -data=./data_admin.json -output=./key_admin.json AuthService createkey
#get ticket for admin
./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=./key_admin.json -output=./ticket_admin.json getticket AuthService
#create key for master
./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=./ticket_admin.json -data=./data_master.json -output=./key_master.json AuthService createkey
#create key for client
./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=./ticket_admin.json -data=./data_client.json -output=./key_client.json AuthService createkey
#create key for objectnode
./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=./ticket_admin.json -data=./data_objectnode.json -output=./key_object.json AuthService createkey

#write key to json file
clientKey=$(sed -n '3p' key_client.json | sed 's/auth_key/clientKey/g')
masterKey=$(sed -n '3p' key_master.json | sed 's/auth_key/masterServiceKey/g')
objectKey=$(sed -n '3p' key_object.json | sed 's/auth_key/authKey/g')
cd ..
cd ..
lineClient=`expr $(cat docker/conf/client.json | wc -l) - 1`
sed -i "${lineClient}i ${clientKey}" docker/conf/client.json
lineMaster=`expr $(cat docker/conf/master1.json | wc -l) - 1`
sed -i "${lineMaster}i ${masterKey}" docker/conf/master1.json
sed -i "${lineMaster}i ${masterKey}" docker/conf/master2.json
sed -i "${lineMaster}i ${masterKey}" docker/conf/master3.json
lineObject=`expr $(cat docker/conf/objectnode.json | wc -l) - 1`
sed -i "${lineObject}i ${objectKey}" docker/conf/objectnode.json

#delete temp files
rm -f ./docker/authnode/authservice.json
rm -f ./docker/authnode/authroot.json
rm -f ./docker/authnode/ticket_auth.json
rm -f ./docker/authnode/key_admin.json
rm -f ./docker/authnode/ticket_admin.json
rm -f ./docker/authnode/key_master.json
rm -f ./docker/authnode/key_client.json
rm -f ./docker/authnode/key_object.json