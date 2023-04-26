# User Management

## Create User

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"id":"testuser","pwd":"12345","type":3}' "http://10.196.59.198:17010/user/create"
```

Creates a user in the cluster for accessing object storage.

::: tip Note
When the cluster starts, the root user (type value is `0x1`) is automatically created.
:::

CubeFS regards the **Owner** field of a volume as a user ID. For example, if the Owner value is *testuser* when creating a volume, the volume is automatically assigned to the user *testuser*.

If there is no user ID with the same value as Owner when creating a volume, a user ID with the value of Owner is automatically created when creating the volume.

Parameter List

| Parameter | Type   | Description                   | Value Range                                                                      | Required | Default Value    |
|-----------|--------|-------------------------------|----------------------------------------------------------------------------------|----------|------------------|
| id        | string | User ID                       | Consists of letters, numbers, and underscores, and does not exceed 21 characters | Yes      | None             |
| pwd       | string | User password                 | No restrictions                                                                  | No       | `CubeFSUser`     |
| ak        | string | Access Key for object storage | Consists of 16 letters and numbers                                               | No       | System-generated |
| sk        | string | Secret Key for object storage | Consists of 32 letters and numbers                                               | No       | System-generated |
| type      | int    | User type                     | 2 (administrator)/3 (ordinary user)                                              | Yes      | None             |

## Delete User

``` bash
curl -v "http://10.196.59.198:17010/user/delete?user=testuser"
```

Deletes the specified user in the cluster.

Parameter List

| Parameter | Type   | Description |
| --------- | ------ | ----------- |
| user      | string | User ID     |

## Query User Information

Displays basic user information, including user `ID`, `Access Key`, `Secret Key`, list of volumes owned by the user, list of permissions granted by other users, user type, creation time, etc.

The **policy** field in the user information indicates the volumes for which the user has permissions. The **own_vols** field indicates the volumes whose owner is the user, and the **authorized_vols** field indicates the volumes authorized by other users and the permission restrictions they have.

There are two ways to obtain user information:

### Query by User ID

``` bash
curl -v "http://10.196.59.198:17010/user/info?user=testuser" | python -m json.tool
```

Parameter List

| Parameter | Type   | Description |
|-----------|--------|-------------|
| user      | string | User ID     |

### Query by Access Key

``` bash
curl -v "http://10.196.59.198:17010/user/akInfo?ak=0123456789123456" | python -m json.tool
```

Parameter List

| Parameter | Type   | Description                     |
|-----------|--------|---------------------------------|
| ak        | string | 16-digit Access Key of the user |

Response Example

``` json
{
     "user_id": "testuser",
     "access_key": "gDcKaBvqky4g8StT",
     "secret_key": "ZVY5RHlrnOrCjImW9S3MajtYZyxSegcf",
     "policy": {
         "own_vols": ["vol1"],
         "authorized_vols": {
             "ltptest": [
                 "perm:builtin:ReadOnly",
                 "perm:custom:PutObjectAction"
             ]
         }
     },
     "user_type": 3,
     "create_time": "2020-05-11 09:25:04"
}
```

## Query User List

``` bash
curl -v "http://10.196.59.198:17010/user/list?keywords=test" | python -m json.tool
```

Queries information about all users in the cluster whose user ID contains a certain keyword.

Parameter List

| Parameter | Type   | Description                       |
|-----------|--------|-----------------------------------|
| keywords  | string | Keyword to search for in user IDs |

## Update User Information

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","access_key":"KzuIVYCFqvu0b3Rd","secret_key":"iaawlCchJeeuGSnmFW72J2oDqLlSqvA5","type":3}' "http://10.196.59.198:17010/user/update"
```

Updates the information of the specified user ID, including `Access Key`, `Secret Key`, and user type.

Parameter List

| Parameter  | Type   | Description          | Required |
|------------|--------|----------------------|----------|
| user_id    | string | User ID to update    | Yes      |
| access_key | string | New Access Key value | No       |
| secret_key | string | New Secret Key value | No       |
| type       | int    | New user type        | No       |

## User Authorization

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol","policy":["perm:builtin:ReadOnly","perm:custom:PutObjectAction"]}' "http://10.196.59.198:17010/user/updatePolicy"
```

Updates the access permissions of a specified user for a certain volume. The value of **policy** can be one of the following:

-   Read-only or read-write permission, with a value of `perm:builtin:ReadOnly` or `perm:builtin:Writable`
-   Permission for a specified operation, in the format of `action:oss:XXX`. For example, for the *GetObject* operation, the policy value is **action:oss:GetObject**
-   Custom permission, in the format of `perm:custom:XXX`, where *XXX* is defined by the user.

After specifying the permissions, the user can only access the volume within the specified permission range when using the object storage function.

::: danger Warning
If the user already has permission settings for this volume, this operation will overwrite the original permissions.
:::

Parameter List

| Parameter | Type         | Description                               | Required |
|-----------|--------------|-------------------------------------------|----------|
| user_id   | string       | User ID to set permissions for            | Yes      |
| volume    | string       | Name of the volume to set permissions for | Yes      |
| policy    | string slice | Permissions to set                        | Yes      |

## Remove User Permissions

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol"}' "http://10.196.59.198:17010/user/removePolicy"
```

Removes all permissions of a specified user for a certain volume.

Parameter List

| Parameter | Type   | Description                                  | Required |
|-----------|--------|----------------------------------------------|----------|
| user_id   | string | User ID to remove permissions for            | Yes      |
| volume    | string | Name of the volume to remove permissions for | Yes      |

## Transfer Volume

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"volume":"vol","user_src":"user1","user_dst":"user2","force":true}' "http://10.196.59.198:17010/user/transferVol"
```

Transfers ownership of a specified volume. This operation removes the specified volume from the source user and adds it to the target user. At the same time, the value of the Owner field in the volume structure is updated to the user ID of the target user.

Parameter List

| Parameter | Type   | Description                                                                                                                                                                                             | Required |
|-----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| volume    | string | Name of the volume to transfer ownership of                                                                                                                                                             | Yes      |
| user_src  | string | Original owner of the volume, which must be the same as the original value of the Owner field of the volume                                                                                             | Yes      |
| user_dst  | string | Target user ID to transfer ownership to                                                                                                                                                                 | Yes      |
| force     | bool   | Whether to force the transfer of the volume. If set to true, the volume will be transferred to the target user even if the value of user_src is not equal to the value of the Owner field of the volume | No       |