### Command examples

```example bash
./cfs-fsck check inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck check dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck check both --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck check both --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220" --mp 1
./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1
./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1 --check-apply-id true
./cfs-fsck clean evict --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck clean inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck clean inode --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./cfs-fsck clean dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck clean dentry --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./cfs-fsck get locations --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck get path --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck get path --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
./cfs-fsck get summary --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
```
