import json
import os
import time
import urllib2

path = "/cfs/mnt/"

master_addr = "http://192.168.0.11:17010"


def httpGet(url):
    req = urllib2.Request(url)
    content = urllib2.urlopen(req).read()
    value = json.loads(content)
    return value


for v in range(100):
    f = open(path + str(v) + ".txt", "w");
    f.writelines("test write file")
    f.flush()

for v in range(100):
    os.remove(path + str(v) + ".txt")

time.sleep(700)

assert len(os.listdir(path)) == 0, 'delete file has err '

volInfo = httpGet(master_addr + "/client/vol?name=ltptest&authKey=0e20229116d5a9a4a9e876806b514a85")
assert volInfo["code"] == 0, 'get vol info api has err:' + content

for mp in volInfo['data']['MetaPartitions']:
    result = httpGet("http://" + mp["LeaderAddr"].split(":")[0] + ":9500/cursorReset?vol=ltptest&pid=" + str(mp["PartitionID"]))
    assert result["code"] == 200, str(result)

# curl "http://192.168.0.21:9500/cursorReset?vol=ltptest&pid=4"
# curl "http://192.168.0.24:9500/cursorReset?vol=ltptest&pid=5"
# curl "http://192.168.0.22:9500/cursorReset?vol=ltptest&pid=6"


# curl "http://192.168.0.21:9500/getPartitionById?pid=4"
# curl "http://192.168.0.24:9500/getPartitionById?pid=5"
# curl "http://192.168.0.22:9500/getPartitionById?pid=6"


# curl "http://192.168.0.11:17010/client/vol?name=ltptest&authKey=0e20229116d5a9a4a9e876806b514a85"
