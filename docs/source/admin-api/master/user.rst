User
==========

Create
----------

.. code-block:: bash

   curl -H "Content-Type:application/json" -X POST --data '{"id":"testuser","pwd":"12345","type":3}' "http://10.196.59.198:17010/user/create"

Create a user in the cluster to access object storage service. When the cluster starts, the ``root`` user is automatically created (the value of ``type`` is ``0x1``).

CubeFS treats every ``Owner`` of volume as a ``user``. For example, if the value of **Owner** is ``testuser`` when creating a volume, the volume is owned by user ``testuser``.

If there is no user ID with the same value as the **Owner** when creating the volume, the user named the value of **Owner** will be automatically created when creating the volume.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Range", "Mandatory", "Default"
   
   "id", "string", "user ID", "Consists of letters, numbers and underscores, no more than 21 characters", "Yes", "None"
   "pwd", "string", "user's password", "Unlimited", "No", "``CubeFSUser``"
   "ak", "string", "Access Key", "Consists of 16-bits letters and numbers", "No", "Random value"
   "sk", "string","Secret Key", "Consists of 32-bits letters and numbers", "No", "Random value"
   "type", "int", "user type", "2: [admin] / 3: [normal user]", "Yes", "None"

Delete
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/user/delete?user=testuser"

Delete the specified user in the cluster.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "user", "string", "user ID"

Get
-----------

Show basic user information, including user ID, Access Key, Secret Key, list of owned volumes, list of permissions granted by other users, user type, creation time.

The field ``policy`` shows the volumes which the user has permission, of which ``own_vols`` indicates that volumes owned by the user, and ``authorized_vols`` indicates the volume authorized by other users to the user with restrictions.

There are two ways to obtain:

Query by User ID
>>>>>>>>>>>>>>>>>

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/user/info?user=testuser" | python -m json.tool

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "user", "string", "user ID"

Query by Access Key
>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/user/akInfo?ak=0123456789123456" | python -m json.tool

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "ak", "string", "Access Key"

response

.. code-block:: json

   {
        "user_id": "testuser",
        "access_key": "0123456789123456",
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

List Users
-----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/user/list?keywords=test" | python -m json.tool

Query information about all users in a cluster whose user ID contains the keyword.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "keywords", "string", "check user ID contains this or not"

Update
-----------

.. code-block:: bash

   curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","access_key":"KzuIVYCFqvu0b3Rd","secret_key":"iaawlCchJeeuGSnmFW72J2oDqLlSqvA5","type":3}' "http://10.196.59.198:17010/user/update"

Update the specified user's information, including access key, secret key and user type.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Mandatory"

   "user_id", "string", "user ID value after updating", "Yes"
   "access_key", "string", "Access Key value after updating", "No"
   "secret_key", "string", "Secret Key value after updating", "No"
   "type", "int", "user type value after updating", "No"

Update Permission
------------------

.. code-block:: bash

   curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol","policy":["perm:builtin:ReadOnly","perm:custom:PutObjectAction"]}' "http://10.196.59.198:17010/user/updatePolicy"

Update the specified user's permission to a volume. There are three types of values for ``policy``:

- Grant read-only or read-write permission, the value is ``perm:builtin:ReadOnly`` or ``perm:builtin:Writable``.
- Grant a permission of the specified action, the format is ``action:oss:XXX``, take *GetObject* action as an example, the value of policy is ``action:oss:GetObject``.
- Grant a custom permission, the format is ``perm:custom:XXX``, where *XXX* is customized by the user.

After the permissions are specified, the user can only access the volume within the specified permissions when using the object storage. If the user already has permissions for this volume, this operation will overwrite the original permissions.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Mandatory"

   "user_id", "string", "user ID to be set", "Yes"
   "volume", "string", "volume name to be set", "Yes"
   "policy", "string slice", "policy to be set", "Yes"

Remove Permission
------------------

.. code-block:: bash

   curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol"}' "http://10.196.59.198:17010/user/removePolicy"

Remove all permissions of a specified user for a volume.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Mandatory"

   "user_id", "string", "user ID to be deleted", "Yes"
   "volume", "string", "volume name to be deleted", "Yes"

Transfer Volume
----------------

.. code-block:: bash

   curl -H "Content-Type:application/json" -X POST --data '{"volume":"vol","user_src":"user1","user_dst":"user2","force":true}' "http://10.196.59.198:17010/user/transferVol"

Transfer the ownership of the specified volume. This operation removes the specified volume from the ``owner_vols`` of source user name and adds it to the ``owner_vols`` of target user name; At the same time, the value of the field ``Owner`` in the volume structure will also be updated to the target user ID.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Mandatory"

   "volume", "string", "Volume name to be transferred", "Yes"
   "user_src", "string", "Original owner of the volume, and must be the same as the ``Owner`` of the volume", "Yes"
   "user_dst", "string", "Target user ID after transferring", "Yes"
   "force", "bool", "Force to transfer the volume. If the value is set to true, even if the value of ``user_src`` is different from the value of the owner of the volume, the volume will also be transferred to the target user, but the original volume owner will not be modified", "No"

Get Users Of Volume
-------------------

.. code-block:: bash

    curl -v "http://10.196.59.198:17010/vol/users?name=vol" | python -m json.tool

Get all users of volume.

.. csv-table:: body key
   :header: "Key", "Type", "Description", "Mandatory"

   "name", "string", "volume name", "Yes"