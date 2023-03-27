# Contributing to ChubaoFS

## Bug Reports

Please make sure the bug is not already reported by [searching the repository](https://github.com/cubefs/cubefs/search?q=&type=Issues&utf8=%E2%9C%93) with reasonable keywords. Then, [open an issue](https://github.com/cubefs/cubefs/issues) with steps to reproduce.

## Patch Guidelines

In order for the patch to be accepted with higher possibility, there are a few things you might want to pay attention to:

* [Filesystem stress tests](https://github.com/linux-test-project/ltp/blob/master/runtest/fs) is required before opening a pull request by `runltp -f fs -d [MOUNTPOINT]`.
* A good commit message describing the bug fix or the new feature is preferred.
* [DCO](https://github.com/apps/dco) is required, so please add `Signed-off-by` to the commit.

## Credits

Sections of this documents have been borrowed from [CoreDNS](https://github.com/coredns/coredns/blob/master/CONTRIBUTING.md) and [Fluentd](https://github.com/fluent/fluentd/blob/master/CONTRIBUTING.md)