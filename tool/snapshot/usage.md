### Command examples

```example bash
./snapshot clean evict --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./snapshot clean inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./snapshot clean inode --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./snapshot clean dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./snapshot clean dentry --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
```
