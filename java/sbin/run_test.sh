#!/bin/env sh
test_dir=/export/test/lib
junit_jar=$test_dir/junit-4.12.jar
sdk_jar=$test_dir/libchubaofs-1.0-SNAPSHOT-jar-with-dependencies.jar
test_jar=$test_dir/libchubaofs-1.0-SNAPSHOT-tests.jar
hamcrest_jar=$test_dir/hamcrest-core-1.3.jar

export CLASSPATH=$junit_jar:$sdk_jar:$test_jar:$hamcrest_jar:$CLASSPATH

export cfs_masters="11.51.28.251:8080"
export cfs_volume="cfstest"
export cfs_libsdk="/export/zhenshan/gocode/src/github.com/chubaofs/chubaofs/libsdk/libcfs.so"
package="io.chubao.fs.sdk."
names="ConfigTest CFSClientTest MkdirTest ListTest RenameTest RmdirTest \
UnlinkTest CreateFileTest AppendFileTest CFSFileTest StreamTest \
ChownTest SetTimesTest ChmodTest"

for name in $names
do
  cases=$cases" "$package$name
done
echo "TestCases:"$cases
java org.junit.runner.JUnitCore $cases