#!/bin/bash
#author:Ulrica Zhang
#test volume basic operation

#basic information
master_addr="192.168.0.11:17010"
master_addrs="192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
Owner='cfs'
cfs_client=./cfs-client

#authKey
printf ${Owner}|md5sum>md5File
read authKey<md5File
authKey=${authKey:0:32}
rm md5File
#echo ${authKey}

#random strings
random_str(){
	length=$1
	(head -c 32 /dev/urandom | base32) > randomStrFile
	read randomStr < randomStrFile
	randomStr=${randomStr:0:${length}}
	rm randomStrFile
	echo ${randomStr}
}

#curl call
parse_curl(){
	operation=$1
	url="http://${master_addr}${operation}"
	#echo $url > return_record
	curl -s ${url}|python -m json.tool > return_record
}
#parse_curl "user/info?user=cfs"

#check message
check_msg(){
	message=$1
	test_num=$2
	test_name=$3
	check_type=$4
	grep "${message}" ./return_record > /dev/null
	if [ $? -ne 0 ]
	then
		echo "a_test_${test_name}_${test_num} ${check_type} fail!"
		cp return_record a_err_record_${test_name}_${test_num}_${check_type}
	else
		echo "a_test_${test_name}_${test_num} ${check_type} success!"
	fi
}

#check create testing volume
check_create(){
	message=$1
	test_num=$2
	test_name=$3
	check_type=$4
	grep "${message}" ./return_record > /dev/null
	if [ $? -ne 0 ]
	then
		echo "b_${test_name}_${test_num} ${check_type} create fail!"
		cp return_record b_err_create_${test_name}_${test_num}_${check_type}
	fi
}

#check delete testing volume
check_delete(){
	message=$1
        test_num=$2
	test_name=$3
	check_type=$4
	grep "${message}" ./return_record > /dev/null
	if [ $? -ne 0 ] 
	then
		echo "c_${test_name}_${test_num} ${check_type} delete fail!"
	        cp return_record c_err_delete_${test_name}_${test_num}_${check_type}
	fi  
}

#get accesskey
accessKey=0
getAccessKey(){
	userID=$1
	parse_curl "/user/info?user=${userID}"
	grep "access_key" ./return_record > accessKeyFile
	read accessKey < accessKeyFile
	accessKey=${accessKey:15:16}
	rm accessKeyFile
	#cp return_record accessKey.file
}

#get secretKey
secretKey=0
getSecretKey(){
	userID=$1
	parse_curl "/user/info?user=${userID}"
	grep "secret_key" ./return_record > secretKeyFile
	read secretKey < secretKeyFile
	secretKey=${secretKey:15:32}
	rm secretKeyFile
	#cp return_record secretKey.file
}

#mount directory
mnt_dir(){
	mkdir -p /cfs/mnt
	mkdir -p /cfs/log
	volName=$1
	getAccessKey "${Owner}"
	getSecretKey "${Owner}"
	#echo "access key: ${accessKey}"
	#echo "secret key: ${secretKey}"
	${cfs_client} --masterAddr=${master_addrs} --mountPoint=/cfs/mnt --volName=${volName} --owner=${Owner} --logDir=/cfs/log --logLevel=info --accessKey=${accessKey} --secretKey=${secretKey} >/dev/null
}

#umount directory
umnt_dir(){
	umount /cfs/mnt 
	rm -rf /cfs
}

#write file
write_file(){
	fileName=$1
	fileSize=$2
	dd if=/dev/zero of=${fileName} bs=1M count=${fileSize} 
}
#write_file "writeFile.txt" 1024

#covered write file
cover_write_file(){
	fileName=$1
	fileSize=$2
	dd if=/dev/zero of=${fileName} bs=1M count=${fileSize}
	#random_str ${fileSize}>${fileName}
}
#cover_write_file "writeFile.txt" 1024

#appended write file
append_write_file(){
	fileName=$1
	fileSize=$2
	random_str ${fileSize}>>${fileName}
}

#create volume
create_vol(){
	volName=$1
	capacity=$2
	mpCount=$3
	parameter=$4
	operation="/admin/createVol?name=${volName}&capacity=${capacity}&owner=${Owner}&mpCount=${mpCount}${parameter}"
	parse_curl ${operation}
}

#create specific number of volumes
create_vol_specific(){
	num=$1
	volName=$2
	capacity=$3
	mpCount=$4
	for i in $(seq 1 ${num})
	do
		name="${volName}_${i}"
		create_vol ${name} ${capacity} ${mpCount}
	done
}

#delete volume
del_vol(){
	volName=$1
	Key=$2
	operation="/vol/delete?name=${volName}&authKey=${Key}"
#	echo ${operation}
	parse_curl ${operation}
}

#delete specific number of volumes
del_vol_specific(){
	num=$1
	volName=$2
	Key=$3

	for i in $(seq 1 ${num})
	do
		name="${volName}_${i}"
	        del_vol ${name} ${Key}
	done
}

#get volume
get_vol(){
	volName=$1
	Key=$2
	operation="/client/vol?name=${volName}&authKey=${Key}"
	parse_curl ${operation}
}

#stat volume
stat_vol(){
	volName=$1
	operation="/client/volStat?name=${volName}"
	parse_curl ${operation}
}

#update volume
update_vol(){
	volName=$1
	updateCapacity=$2
	Key=$3
	parameter=$4
	operation="/vol/update?name=${volName}&capacity=${updateCapacity}&authKey=${Key}${parameter}"
	#echo ${operation}
	parse_curl ${operation}
}

#get volume list
list_vol(){
	keywords=$1
	operation="/vol/list?keywords=${keywords}"
	parse_curl ${operation}
}

#update zone
update_zone(){
	Name=$1
	Enable=$2
	operation="zone/update?name=${name}&enable=${Enable}"
	parse_curl ${operation}
}

