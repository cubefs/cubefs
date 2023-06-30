# CubeFS

[![CNCF Status](https://img.shields.io/badge/cncf%20status-incubating-blue.svg)](https://www.cncf.io/projects)
[![Build Status](https://github.com/cubefs/cubefs/actions/workflows/ci.yml/badge.svg)](https://github.com/cubefs/cubefs/actions/workflows/ci.yml)
[![LICENSE](https://img.shields.io/github/license/cubefs/cubefs.svg)](https://github.com/cubefs/cubefs/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/cubefs/cubefs)](https://goreportcard.com/report/github.com/cubefs/cubefs)
[![Docs](https://img.shields.io/badge/docs-latest-green.svg)](https://cubefs.io/docs/master/overview/introduction.html)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2761/badge)](https://bestpractices.coreinfrastructure.org/projects/2761)
[![Codecov](https://img.shields.io/codecov/c/github/cubefs/cubefs?style=flat-square&logo=codecov)](https://codecov.io/gh/cubefs/cubefs)
[![Release](https://img.shields.io/github/v/release/cubefs/cubefs.svg?color=161823&style=flat-square&logo=smartthings)](https://github.com/cubefs/cubefs/releases)
[![Tag](https://img.shields.io/github/v/tag/cubefs/cubefs.svg?color=ee8936&logo=fitbit&style=flat-square)](https://github.com/cubefs/cubefs/tags)

|<img src="https://user-images.githubusercontent.com/5708406/91202310-31eaab80-e734-11ea-84fc-c1b1882ae71c.png" height="24"/>&nbsp;Community Meeting|
|------------------|
| The CubeFS Project holds bi-weekly community online meeting. To join or watch previous meeting notes and recordings, please see [meeting schedule](https://github.com/cubefs/community/wiki/Meeting-Schedule) and [meeting minutes](https://github.com/cubefs/community/wiki/Meeting-Agenda-and-Notes). |


<div width="100%" style="text-align:center;"><img alt="CubeFS" src="https://user-images.githubusercontent.com/12113219/178886968-9513e51e-393b-4af1-bd90-189593ee2012.png" height="200"/></div>


## Overview

CubeFS ("储宝" in Chinese) is an open-source cloud-native file storage system, hosted by the [Cloud Native Computing Foundation](https://cncf.io) (CNCF) as an [incubating](https://www.cncf.io/projects/) project.

First, CubeFS has been commonly used as the datacenter filesystem for online applications, database or data processing services and machine learning jobs orchestrated by Kubernetes to separate storage from compute. Second, CubeFS also works as a high-performance object store compatible with the S3 API. Moreover, CubeFS offers personal cloud storage on all your devices. 

Some key features of CubeFS include:

- Multiple access protocols such as POSIX, HDFS, S3, and its own REST API
- Highly scalable metadata service with strong consistency  
- Performance optimization of large/small files and sequential/random writes
- Multi-tenancy support with better resource utilization and tenant isolation
- Hybrid cloud I/O acceleration through multi-level caching
- Flexible storage policies, high-performance replication or low-cost erasure coding


<div width="100%" style="text-align:center;"><img alt="CubeFS Architecture" src="https://raw.githubusercontent.com/cubefs/cubefs/master/docs/source/pic/cfs-arch-ec.png"/></div>

## Documents

- English version: https://cubefs.io/docs/master/overview/introduction.html
- Chinese version: https://cubefs.io/zh/docs/master/overview/introduction.html

## Community

- Homepage: [cubefs.io](https://cubefs.io/)
- Mailing list: users@cubefs.groups.io
- Slack: [cubefs.slack.com](https://cubefs.slack.com/)
- WeChat: detail see [here](https://github.com/cubefs/cubefs/issues/604).

## Partners and Users

There is the list of users and success stories [ADOPTERS.md](ADOPTERS.md).

## Reference

Haifeng Liu, et al., CFS: A Distributed File System for Large Scale Container Platforms. SIGMOD‘19, June 30-July 5, 2019, Amsterdam, Netherlands. 

For more information, please refer to https://dl.acm.org/citation.cfm?doid=3299869.3314046 and https://arxiv.org/abs/1911.03001


## License

CubeFS is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Note

The master branch may be in an unstable or even broken state during development. Please use releases instead of the master branch in order to get a stable set of binaries.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=cubefs/cubefs&type=Date)](https://star-history.com/#cubefs/cubefs&Date)
