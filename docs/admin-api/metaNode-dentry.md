## getDentry
```bash
curl -v "http://127.0.0.1:9092/getDentry?pid=100&ino=1024"
```

Get dentry information

|parameter | type | desc|
|:---:|:---:|:---:|
|pid | integer| meta partition id | 
    
## getDirectory
```bash
curl -v "http://127.0.0.1:9092/getDirectory?pid=100&parentIno=1024
```

Get all files of the parent inode is 1024

|parameter | type | desc|
|:---:|:---:|:---:|
| pid | integer |partition id |
| ino | integer |inode id | 

## getAllDentry
```bash
curl -v "http://127.0.0.1:9092/getAllDentry?pid=100
```

Get all dentry

|parameter | type | desc|
|:---:|:---:|:---:|
| pid | integer |partition id |