# BlobStore
- [Overview](#overview)
- [Documents](#documents)
- [Build BlobStore](#build-blobstore)
- [Deploy BlobStore](#deploy-blobstore)
- [Manage BlobStore](#manage-blobstore)
- [License](#license)

## Overview
BlobStore is a highly reliable,highly available and  ultra-large scale  distributed storage system. The system adopts Reed-Solomon code, which provides higher data durability with less storage cost than use three copies  backup technology, and supports multiple erasure code modes multiple availability zones,and optimizes for small file to meet the storage needs of different scenarios.
Some key features of BlobStore include:
- ultra-large scale
- high reliability
- flexible deployment
- low cost


## Documents

- English version: https://cubefs.io/docs/master/overview/introduction.html
- Chinese version: https://cubefs.io/zh/docs/master/overview/introduction.html

## Build BlobStore

```
$ git clone http://github.com/cubefs/cubefs.git
$ cd cubefs/blobstore
$ source env.sh
$ ./build.sh
```

## Deploy BlobStore
For more details please refer to [documentation](https://cubefs.io/docs/master/user-guide/blobstore.html).

## Manage BlobStore
For more details please refer to [documentation](https://cubefs.io/docs/master/maintenance/admin-api/blobstore/base.html).

## License
BlobStore is licensed under the Apache License, Version 2.0. For detail see LICENSE and NOTICE.
