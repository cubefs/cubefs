# Performance comparison

Server cluster：dgtest01-kerneltest1

Client OS：3.10.0-957.27.2.el7.x86_64

Client CPU：Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz (80C)

Client Mem：256GB

Client network：Intel Corporation I350 Gigabit Network Connection (rev 01)

Client network mode：IEEE 802.3ad Dynamic link aggregation

Fuse client mount command：

```less
cd /root/tmp
./cfs-client -c ./client.conf
```
Kernel client mount command：
```less
mount -t cubefs -o owner=wuhc,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 //{master ip}:17010/wuhc /mnt/cubefs
```
|    |kern|fuse|
|:----|:----|:----|
|direct io Sequential write (Single file) |110MB/s|120MB/s|
|direct io Sequential write (Multiple files) |1063MB/s|1387MB/s|
|direct io Sequential read (Single file) |386MB/s|257MB/s|
|direct io Sequential read (Multiple files)|1641MB/s|2344MB/s|
|page io Sequential write (Single file) |2745MB/s|120MB/s|
|page io Sequential write (Multiple files)|25.7GB/s|1379MB/s|
|page io Sequential read (Single file) |1246MB/s|638MB/s|
|page io Sequential read (Multiple files)|3085MB/s|1851MB/s|

# 
# kern

## direct io Sequential write (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=write --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1 

  lat (nsec)   : 1000=62.56%
  lat (usec)   : 2=37.26%, 4=0.14%, 10=0.05%

Run status group 0 (all jobs):
  WRITE: bw=105MiB/s (110MB/s), 105MiB/s-105MiB/s (110MB/s-110MB/s), io=10.0GiB (10.7GB), run=97945-97945msec
```
## direct io Sequential write (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=write --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32 

  lat (nsec)   : 1000=1.27%
  lat (usec)   : 2=92.77%, 4=4.98%, 10=0.88%, 20=0.10%

Run status group 0 (all jobs):
  WRITE: bw=1014MiB/s (1063MB/s), 31.7MiB/s-35.7MiB/s (33.2MB/s-37.4MB/s), io=32.0GiB (34.4GB), run=28713-32326msec
```
## direct io Sequential read (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=read --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1 

  lat (nsec)   : 750=21.52%, 1000=69.42%
  lat (usec)   : 2=8.81%, 4=0.14%, 10=0.09%, 20=0.01%, 50=0.01%

Run status group 0 (all jobs):
   READ: bw=368MiB/s (386MB/s), 368MiB/s-368MiB/s (386MB/s-386MB/s), io=10.0GiB (10.7GB), run=27798-27798msec
```
## direct io Sequential read (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=read --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32  

  lat (nsec)   : 1000=2.25%
  lat (usec)   : 2=87.99%, 4=9.08%, 10=0.49%, 20=0.10%, 50=0.10%

Run status group 0 (all jobs):
   READ: bw=1565MiB/s (1641MB/s), 48.9MiB/s-76.5MiB/s (51.3MB/s-80.2MB/s), io=32.0GiB (34.4GB), run=13383-20939msec
```
## page io Sequential write (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=write --ioengine=libaio --bs=1M --size=50G -name=mytest --numjobs=1 

  lat (nsec)   : 750=97.41%, 1000=2.20%
  lat (usec)   : 2=0.30%, 4=0.07%, 10=0.02%, 50=0.01%

Run status group 0 (all jobs):
  WRITE: bw=2618MiB/s (2745MB/s), 2618MiB/s-2618MiB/s (2745MB/s-2745MB/s), io=50.0GiB (53.7GB), run=19556-19556msec
```
## page io Sequential write (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=write --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32 

  lat (nsec)   : 750=28.22%, 1000=65.72%
  lat (usec)   : 2=5.66%, 4=0.39%

Run status group 0 (all jobs):
  WRITE: bw=23.9GiB/s (25.7GB/s), 766MiB/s-1208MiB/s (803MB/s-1266MB/s), io=32.0GiB (34.4GB), run=848-1337msec
```
## page io Sequential read (Single file) 

```shell
fio --directory=/mnt/cubefs/ --direct=0 --thread --rw=read --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1  

  lat (nsec)   : 750=83.03%, 1000=15.90%
  lat (usec)   : 2=0.84%, 4=0.17%, 10=0.07%

Run status group 0 (all jobs):
   READ: bw=1188MiB/s (1246MB/s), 1188MiB/s-1188MiB/s (1246MB/s-1246MB/s), io=10.0GiB (10.7GB), run=8616-8616msec
```
## page io Sequential read (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=read --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32  

  lat (nsec)   : 750=91.11%, 1000=7.03%
  lat (usec)   : 2=1.46%, 4=0.10%, 10=0.20%, 20=0.10%

Run status group 0 (all jobs):
   READ: bw=2942MiB/s (3085MB/s), 91.9MiB/s-139MiB/s (96.4MB/s-145MB/s), io=32.0GiB (34.4GB), run=7388-11137msec
```
# fuse

## direct io  Sequential write (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=write --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1 

  lat (usec)   : 2=82.81%, 4=17.01%, 10=0.18%
Run status group 0 (all jobs):
  WRITE: bw=114MiB/s (120MB/s), 114MiB/s-114MiB/s (120MB/s-120MB/s), io=10.0GiB (10.7GB), run=89585-89585msec
```
## direct io  Sequential write (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=write --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32

  lat (usec)   : 2=3.22%, 4=93.65%, 10=3.12%
Run status group 0 (all jobs):
  WRITE: bw=1323MiB/s (1387MB/s), 41.3MiB/s-48.2MiB/s (43.4MB/s-50.5MB/s), io=32.0GiB (34.4GB), run=21260-24766msec
```
## direct io Sequential read (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=read --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1      

  lat (usec)   : 2=91.85%, 4=7.83%, 10=0.19%, 20=0.03%
Run status group 0 (all jobs):
   READ: bw=245MiB/s (257MB/s), 245MiB/s-245MiB/s (257MB/s-257MB/s), io=10.0GiB (10.7GB), run=41778-41778msec
```
## direct io Sequential read (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=1 --thread --rw=read --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32 

  lat (usec)   : 2=50.78%, 4=46.19%, 10=2.54%, 20=0.29%, 50=0.10%
Run status group 0 (all jobs):
   READ: bw=2235MiB/s (2344MB/s), 69.8MiB/s-73.2MiB/s (73.2MB/s-76.8MB/s), io=32.0GiB (34.4GB), run=13985-14661msec
```
## page io Sequential write (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=write --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1 

  lat (usec)   : 2=85.16%, 4=14.62%, 10=0.20%, 20=0.03%
Run status group 0 (all jobs):
  WRITE: bw=114MiB/s (120MB/s), 114MiB/s-114MiB/s (120MB/s-120MB/s), io=10.0GiB (10.7GB), run=89437-89437msec
```
## page io Sequential write (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=write --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32

  lat (usec)   : 2=11.82%, 4=85.94%, 10=1.86%, 20=0.39%
Run status group 0 (all jobs):
  WRITE: bw=1315MiB/s (1379MB/s), 41.1MiB/s-47.8MiB/s (43.1MB/s-50.1MB/s), io=32.0GiB (34.4GB), run=21441-24923msec
```
## page io Sequential read (Single file) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=read --ioengine=libaio --bs=1M --size=10G -name=mytest --numjobs=1  

  lat (usec)   : 2=90.93%, 4=8.87%, 10=0.20%, 20=0.01%
Run status group 0 (all jobs):
   READ: bw=608MiB/s (638MB/s), 608MiB/s-608MiB/s (638MB/s-638MB/s), io=10.0GiB (10.7GB), run=16842-16842msec
```
## page io Sequential read (Multiple files) 

```shell
fio --directory=/mnt/cubefs --direct=0 --thread --rw=read --ioengine=libaio --bs=1M --size=1G -name=mytest --numjobs=32  

  lat (usec)   : 2=3.52%, 4=76.37%, 10=19.34%, 20=0.59%, 50=0.10%
Run status group 0 (all jobs):
   READ: bw=1765MiB/s (1851MB/s), 55.2MiB/s-68.8MiB/s (57.8MB/s-72.2MB/s), io=32.0GiB (34.4GB), run=14881-18566msec
```

