# User Management

## Create User

Create user [USER ID].

``` bash
cfs-cli user create [USER ID] [flags]
```

```bash
Flags:
    --access-key string                     # Specify the access key for the user to use the object storage function.
    --secret-key string                     # Specify the secret key for the user to use the object storage function.
    --password string                       # Specify the user password.
    --user-type string                      # Specify the user type, optional values are normal or admin (default is normal).
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

## Delete User

Delete user [USER ID].

``` bash
cfs-cli user delete [USER ID] [flags]
```

```bash
Flags:
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

## Show User

Get information of user [USER ID].

```bash
cfs-cli user info [USER ID]
```

## List Users

Get a list of all current users.

```bash
cfs-cli user list
```

## Update User Permission

Update the permission [PERM] of user [USER ID] for volume [VOLUME].

[PERM] can be "READONLY/RO", "READWRITE/RW", or "NONE".

```bash
cfs-cli user perm [USER ID] [VOLUME] [PERM]
```

## Update User Information

Update the information of user [USER ID].

```bash
cfs-cli user update [USER ID] [flags]
```

```bash
Flags:
    --access-key string                     # The updated access key value.
    --secret-key string                     # The updated secret key value.
    --user-type string                      # The updated user type, optional values are normal or admin.
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

