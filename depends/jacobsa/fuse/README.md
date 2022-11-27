[![GoDoc](https://godoc.org/github.com/jacobsa/ogletest?status.svg)](https://godoc.org/github.com/jacobsa/fuse)

This package allows for writing and mounting user-space file systems from Go.
Install it as follows:

    go get -u github.com/jacobsa/fuse

Afterward, see the documentation for the following three packages:

 *  Package [fuse][] provides support for mounting a new file system and
    reading requests from the kernel.

 *  Package [fuseops][] enumerates the supported requests from the kernel, and
    provides documentation on their semantics.

 *  Package [fuseutil][], in particular the `FileSystem` interface, provides a
    convenient way to create a file system type and export it to the kernel via
    `fuse.Mount`.

Make sure to also see the sub-packages of the [samples][] package for examples
and tests.

This package owes its inspiration and most of its kernel-related code to
[bazil.org/fuse][bazil].

[fuse]: http://godoc.org/github.com/jacobsa/fuse
[fuseops]: http://godoc.org/github.com/jacobsa/fuse/fuseops
[fuseutil]: http://godoc.org/github.com/jacobsa/fuse/fuseutil
[samples]: http://godoc.org/github.com/jacobsa/fuse/samples
[bazil]: http://godoc.org/bazil.org/fuse
