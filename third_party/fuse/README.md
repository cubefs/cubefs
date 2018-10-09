github.com/tiglabs/containerfs/third_party/fuse -- Filesystems in Go
===================================

`github.com/tiglabs/containerfs/third_party/fuse` is a Go library for writing FUSE userspace
filesystems.

It is a from-scratch implementation of the kernel-userspace
communication protocol, and does not use the C library from the
project called FUSE. `github.com/tiglabs/containerfs/third_party/fuse` embraces Go fully for safety and
ease of programming.

Here’s how to get going:

    go get github.com/tiglabs/containerfs/third_party/fuse

Website: http://github.com/tiglabs/containerfs/third_party/fuse/

Github repository: https://github.com/bazil/fuse

API docs: http://godoc.org/github.com/tiglabs/containerfs/third_party/fuse

Our thanks to Russ Cox for his fuse library, which this project is
based on.
