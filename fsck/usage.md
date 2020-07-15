### Command examples

```bash
./fsck check inode -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck check dentry -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck check both -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck check both -vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./fsck clean evict -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck clean inode -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck clean inode -vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
./fsck clean dentry -master "<masterAddr>" -vol "<volName>" -mport "<metanode prof port>"
./fsck clean dentry -vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
```
