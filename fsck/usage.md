### Command examples

```example bash
./fsck check inode ----master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck check dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck check both --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck check both --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./fsck clean evict --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck clean inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck clean inode --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./fsck clean dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck clean dentry --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./fsck get locations --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck get path --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck get path --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./fsck get summary --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
```
