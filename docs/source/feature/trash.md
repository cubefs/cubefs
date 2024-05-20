# Trash

The default setting for CubeFS volumes does not enable the trash feature. However, you can enable the trash feature for a volume through the master service interface.
- name，volume name.
- authKey，calculate the 32-bit MD5 value of the owner field of vol as authentication information.
- trashInterval，trash cleanup period for deleted files is specified in minutes. The default value of 0 means that the trash feature is not enabled.。
``` bash
curl -v "http://127.0.0.1:17010/vol/setTrashInterval?name=test&authKey=&trashInterval=7200" | jq .
```
After enabling the trash feature, the client will create a hidden folder named ".Trash" in the root directory of the mount point. The "Current" folder within ".Trash" retains the currently mistakenly deleted files or folders along with their complete file paths.

::: tip Note
In order to improve the efficiency of the trash, deleted files are displayed with their parent directories flattened in the file names. The background coroutine constructs the parent directory paths for the files. Therefore, when there are a large number of files in the deleted, it is possible to briefly encounter situations where deleted files do not have their parent directories. This is considered a normal occurrence.
:::

The "Current" directory is periodically renamed to **Expired_ExpirationTimestamp**. When the expiration timestamp of the "Expired" folder is reached, all contents within that folder are deleted.

## Recover deleted files

As mentioned earlier, to recover a mistakenly deleted file, you simply need to locate the file in either the "Current" or "Expired" directory within the ".Trash" folder. Using the complete parent directory path, you can restore the deleted file/folder to its original location using the "mv" operation.

## Clean up files in the trash

It is important to note that the contents of the trash rely on the client's background coroutine for periodic deletion. Therefore, if there is no online client for the respective volume, the contents of the trash will be retained until a client for the respective volume.

To free up space in the trash as quickly as possible, you can also directly perform the "rm" operation on the ".Trash" folder through the client.