// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// To run this command you need protoc.
//go:generate go get -u google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate protoc --go_out=./ Cell.proto Client.proto ClusterId.proto ClusterStatus.proto Comparator.proto ErrorHandling.proto FS.proto Filter.proto HBase.proto Master.proto Procedure.proto Quota.proto RPC.proto Tracing.proto ZooKeeper.proto

package pb
