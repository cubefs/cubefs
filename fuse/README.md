github.com/tiglabs/baudstorage/fuse -- Filesystems in Go
===================================

`github.com/tiglabs/baudstorage/fuse` is a Go library for writing FUSE userspace
filesystems.

It is a from-scratch implementation of the kernel-userspace
communication protocol, and does not use the C library from the
project called FUSE. `github.com/tiglabs/baudstorage/fuse` embraces Go fully for safety and
ease of programming.

Here’s how to get going:

    go get github.com/tiglabs/baudstorage/fuse

Website: http://github.com/tiglabs/baudstorage/fuse/

Github repository: https://github.com/bazil/fuse

API docs: http://godoc.org/github.com/tiglabs/baudstorage/fuse

Our thanks to Russ Cox for his fuse library, which this project is
based on.
