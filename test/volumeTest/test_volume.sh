#!/bin/bash
#author:Ulrica Zhang
#volume operation test

echo "---------------1.delete operation-----------------"
./deleteVol.sh
echo "---------------2.get operation--------------------"
./getVol.sh
echo "---------------3.stat operation-------------------"
./statVol.sh
echo "---------------4.update volume operation----------"
./updateVol.sh
echo "---------------5.list volume operation------------"
./listVol.sh

rm return_record
echo "---------------test end---------------------------"
