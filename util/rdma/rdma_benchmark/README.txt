分到拷贝到两台机器上，之后分别进到client和server，执行go build，之后就是如下命令：

[service@server-machine server]$ ./server -h
Usage of ./server:
  -deep int
        io deep (default 1)
  -ip string
        127.0.0.1 (default "127.0.0.1")
  -port string
        9000 (default "9000")
  -protocol string
        rdma/tcp (default "rdma")
  -size int
        io size (default 4096)

[service@client-machine client]$ ./client -h
Usage of ./client:
  -deep int
        io deep (default 1)
  -ip string
        127.0.0.1 (default "127.0.0.1")
  -port string
        9000 (default "9000")
  -protocol string
        rdma/tcp (default "rdma")
  -size int
        io size (default 4096)


-----------------------------------------------------------------------------------------------------------------------

[service@client-machine client]$ ./client -ip 192.168.12.100 -protocol tcp -port 17370 -ip 192.168.12.100
param {1 4096 tcp 192.168.12.100 17370}
IOPS=[10792.00], TOTAL_BPS=[84.31M], AVG_TM=[92.55us]
IOPS=[10316.00], TOTAL_BPS=[80.59M], AVG_TM=[96.83us]
IOPS=[9843.33], TOTAL_BPS=[76.90M], AVG_TM=[101.49us]
IOPS=[9784.50], TOTAL_BPS=[76.44M], AVG_TM=[102.10us]
IOPS=[9676.60], TOTAL_BPS=[75.60M], AVG_TM=[103.24us]
^C
[service@client-machine client]$ ./client -ip 192.168.12.100 -protocol rdma -port 17370 -ip 192.168.12.100
param {1 4096 rdma 192.168.12.100 17370}
IOPS=[54937.00], TOTAL_BPS=[429.20M], AVG_TM=[16.69us]
IOPS=[59708.50], TOTAL_BPS=[466.48M], AVG_TM=[16.03us]
IOPS=[61561.00], TOTAL_BPS=[480.95M], AVG_TM=[15.76us]

-----------------------------------------------------------------------------------------------------------------------
[service@server-machine server]$ ./server -protocol tcp -port 17370 -ip 192.168.12.100
param {1 4096 tcp 192.168.12.100 17370}
IOPS=[0.00], TOTAL_BPS=[0.00M], AVG_TM=[NaNus]
IOPS=[0.00], TOTAL_BPS=[0.00M], AVG_TM=[NaNus]
IOPS=[0.00], TOTAL_BPS=[0.00M], AVG_TM=[NaNus]
IOPS=[10017.00], TOTAL_BPS=[78.26M], AVG_TM=[92.40us]
IOPS=[10007.00], TOTAL_BPS=[78.18M], AVG_TM=[96.18us]
IOPS=[9610.33], TOTAL_BPS=[75.08M], AVG_TM=[101.43us]
IOPS=[9617.75], TOTAL_BPS=[75.14M], AVG_TM=[102.00us]
IOPS=[9543.20], TOTAL_BPS=[74.56M], AVG_TM=[103.18us]
^C
[service@server-machine server]$ 
[service@server-machine server]$ ./server -protocol rdma -port 17370 -ip 192.168.12.100
param {1 4096 rdma 192.168.12.100 17370}
IOPS=[0.00], TOTAL_BPS=[0.00M], AVG_TM=[NaNus]
IOPS=[0.00], TOTAL_BPS=[0.00M], AVG_TM=[NaNus]
IOPS=[59072.00], TOTAL_BPS=[461.50M], AVG_TM=[16.69us]
IOPS=[62272.50], TOTAL_BPS=[486.50M], AVG_TM=[15.91us]
IOPS=[62930.33], TOTAL_BPS=[491.64M], AVG_TM=[15.78us]

-----------------------------------------------------------------------------------------------------------------------