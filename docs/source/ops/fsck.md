# File System Consistency Check

File System Consistency Check (fsck) is mainly used to check and repair inconsistencies and errors in the Cubefs file system. This tool is used to solve potential file system problems, such as inconsistencies in mp on multiple copies. Using fsck can quickly locate the problem, thereby reducing the manpower cost of daily cluster operation and maintenance.

## Compile fsck 
Compile the fsck tool in the cubefs directory. The cfs-fsck executable program will be generated in the ./build/bin/ directory.

```bash
$ make fsck
```

## fsck command

### Detect the specified volume inode and dentry command
Detect obsolete inodes and dentry that need to be cleaned up.

``` bash
$ ./cfs-fsck check inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check both --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check both --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
``` 

The command will generate an `_export_volname` folder in the execution directory according to the volume name. The directory structure is as follows. Among them, `inode.dump` and `dentry.dump` save all inode and dentry information in the volume, and the `.obsolete` suffix file saves the obsolete inode and dentry information. You can obtain the inode and dentry information by sending a request to the master, or analyze it based on the existing files.

``` bash
.
├── dentry.dump
├── dentry.dump.obsolete
├── inode.dump
└── inode.dump.obsolete
``` 

### Check the consistency of mp in multiple copies
v3.5.0 Hybrid Cloud version newly added function, used to detect whether the inode and dentry in mp are consistent between different copies.
Optional parameters: At least one of vol and mp must be specified
* vol：specifying the volume name will compare all mp under the volume to see if they are consistent
* mp：Check whether the specified mp is consistent
* check-apply-id：Select whether to skip copies with different applyids. Default is false

``` bash
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220" --mp 1
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1 --check-apply-id true
```

Execute the command to generate the `mpCheck.log` file in the `_export_volname` or `_export_mpID` folder, and the detection information will be entered into the file.

### clean inode command
``` bash
$ ./cfs-fsck clean inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
```
Cleaning requirements:
1. inode.Nlink=0
2. 24 hours have passed since the modified time
3. The type is a regular file, not a directory

To delete junk data with Nlink!=0, you can add the -f parameter to force deletion

### Get information command
``` bash
$ ./cfs-fsck get locations --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
# Example
Inode: 1
Generation: 1
Size: 0

MetaPartition:
 Id:1
 Leader:172.16.1.103:17210
 Hosts:[172.16.1.102:17210 172.16.1.103:17210 172.16.1.101:17210]
```

``` bash
$./cfs-fsck get path --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
# Example
Inode: 8388610, Valid: true
Path: /111.test
```

``` bash
$ ./cfs-fsck get summary --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
# Example
Inode: 8388610, Valid: true
Path(is file): /111.test, Bytes: 0
```