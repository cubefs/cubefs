#!/bin/bash
#author:Ulrica Zhang
#delete volume test

source ./test_basic.sh

#sleep to wait for delete volume
#sleep 60s

#1.name&authKey true
create_vol "test_delete_1" "1" "1"
check_create "success" "1" "delete" 

del_vol "test_delete_1" ${authKey}
check_msg "success" "1" "delete" 

#2.name wrong
create_vol "test_delete_2" "1" "1"
check_create "success" "2" "delete" 

randomName_2=$(random_str "10")
del_vol ${randomName_2} ${authKey}
check_msg "\"msg\": \"vol not exists\"" "2" "delete" 

del_vol "test_delete_2" ${authKey}
check_delete "success" "2" "delete" 

#3.authKey wrong
create_vol "test_delete_3" "1" "1"
check_create "success" "3" "delete" 

randomKey_3=$(random_str "32")
del_vol "test_delete_3" ${randomKey_3}
check_msg "\"msg\": \"client and server auth key do not match\"" "3" "delete" 

del_vol "test_delete_3" ${authKey}
check_delete "success" "3" "delete" 

#4.name&authKey wrong
create_vol "test_delete_4" "1" "1" 
check_create "success" "4" "delete" 

randomName_4=$(random_str "10")
randomKey_4=$(random_str "32")
del_vol ${randomName_4} ${randomKey_4}
check_msg "\"msg\": \"vol not exists\"" "4" "delete" 

del_vol "test_delete_4" ${authKey}
check_delete "success" "4" "delete" 

#5.name&authKey empty
create_vol "test_delete_5" "1" "1"
check_create "success" "5" "delete" 

del_vol " " " "
check_msg "parameter name not found" "5" "delete" 

del_vol "test_delete_5" ${authKey}
check_delete "success" "5" "delete" 




