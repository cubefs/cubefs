#!/bin/bash
#testcases for vol interfaces of CFS resource management with simple auth enabled

. ../lib/libTestAuth.sh

cp ../../build/bin/cfs-authtool ./
cfs_authtool=./cfs-authtool

prepare_ltptest_user

#test01 create vol
test_operation "/admin/createVol?name=test1&capacity=1&owner=newuser&mpCount=3" ltptest
check_msg "no permission" 1 1
test_operation "/admin/createVol?name=test1&capacity=1&owner=root&mpCount=3" ltptest
check_msg "no permission" 1 2
test_operation "/admin/createVol?name=test1&capacity=1&owner=ltptest&mpCount=3" ltptest
check_msg "success" 1 3
test_operation "/admin/createVol?name=test2&capacity=1&owner=newuser&mpCount=3" root
check_msg "success" 1 4
test_operation "/user/info?user=newuser" root
check_msg "success" 1 5
add_dict_user newuser

#test02 list vol
test_operation "/vol/list?keywords="
check_msg "success" 2 2
check_msg "test1" 2 3
check_msg "test2" 2 4

#test03 stat vol
test_operation "/client/volStat?name=test1"
check_msg "success" 3 1
check_msg "test1" 3 2
test_operation "/client/volStat?name=test2"
check_msg "success" 3 3
check_msg "test2" 3 4

#test04 get vol
cal_md5 ltptest
test_operation "/client/vol?name=test1&authKey=$md5"
check_msg "success" 4 1
check_msg "PartitionID" 4 2
check_msg "Status" 4 3
test_operation "/client/vol?name=test2&authKey=$md5"
check_msg "client and server auth key do not match" 4 4
cal_md5 newuser
test_operation "/client/vol?name=test2&authKey=$md5"
check_msg "success" 4 5
check_msg "PartitionID" 4 6
check_msg "Status" 4 7

#test05 update vol
cal_md5 ltptest
test_operation "/vol/update?name=test1&capacity=2&authKey=$md5" newuser
check_msg "no permission" 5 1
test_operation "/vol/update?name=test1&capacity=2&authKey=$md5" ltptest
check_msg "success" 5 2
test_operation "/vol/update?name=test1&capacity=1&authKey=$md5" root
check_msg "success" 5 3

#test06 delete vol
cal_md5 ltptest
test_operation "/vol/delete?name=test1&authKey=$md5" newuser
check_msg "no permission" 6 1
test_operation "/vol/delete?name=test1&authKey=$md5" root
check_msg "success" 6 2
cal_md5 newuser
test_operation "/vol/delete?name=test2&authKey=$md5" newuser
check_msg "success" 6 3
test_operation "/user/delete?user=newuser" root
check_msg "success" 6 4

rm return_Info.json
rm $cfs_authtool
