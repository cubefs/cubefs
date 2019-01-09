## getInode
```bash
curl -v http://127.0.0.1:9092/getInode?pid=100&ino=1024
```
Get inode information
    
|parameter | type | desc|
|:---:|:---:|:---:|
| pid   | integer|meta-partition id |
| ino   | integer|inode id |

## getExtentsByInode
```bash
curl -v http://127.0.0.1:9092/getExtentsByInode?pid=100&ino=1024
```
Get inode all extents information
    
|parameter | type | desc|
|:---:|:---:|:---:|
| pid   | integer|meta-partition id |
| ino   | integer|inode id |
    
## getAllInodes
```bash
curl -v http://127.0.0.1:9092/getAllInodes?pid=100
```
Get all inodes of the specified partition

|parameter | type | desc|
|:---:|:---:|:---:|
 pid |integer| meta-partition id |
    
