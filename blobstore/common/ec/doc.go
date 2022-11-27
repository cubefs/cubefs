// Copyright 2022 The CubeFS Authors.
//
// Note:
//   1. Do not use after releasing it.
//   2. You need to ensure its safety yourself before releasing it.
//   3. You should know that its pointer would have changed after Resize.
//   4. Manually manage the DataBuf in Ranged mode, do not Split it.
//
// MinShardSize min size per shard, fill data into shards 0-N continuously,
// align with zero bytes if data size less than MinShardSize*N
//
// Length of real data less than MinShardSize*N, ShardSize = MinShardSize.
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  data  |     align bytes     |        partiy          | local |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  0  |  1  | ....       |  N  |  N+1  | ...      | N+M | N+M+L |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// Length more than MinShardSize*N, ShardSize = ceil(len(data)/N)
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |          data        |padding|        partiy          | local |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  0  |  1  | ....       |  N  |  N+1  | ...      | N+M | N+M+L |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
//
// Example:
//
// import (
//     "github.com/cubefs/cubefs/blobstore/common/codemode"
//     "github.com/cubefs/cubefs/blobstore/common/resourcepool"
// )
// func xxx() {
//     pool := resourcepool.NewMemPool(nil)
//     codeModeInfo := codemode.GetTactic(codemode.EC15p12)
//     buffer, err := NewBuffer(1024, codeModeInfo, pool)
//     if err != nil {
//         // ...
//     }
//     // release
//     defer buffer.Release()
//
//     // release it in an anonymous func if you will resize
//     defer func() {
//         buffer.Release()
//     }()
//
//     io.Copy(buffer.DataBuf, io.Reader)
//     shards := ec.Split(buffer.ECDataBuf)
//     // ...
//
//     buffer.Resize(10240)
//     // ...
// }
package ec
