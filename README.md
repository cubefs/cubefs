# CubeFS

[![CNCF Status](https://img.shields.io/badge/cncf%20status-graduated-blue.svg)](https://www.cncf.io/projects)
[![Build Status](https://github.com/cubefs/cubefs/actions/workflows/ci.yml/badge.svg)](https://github.com/cubefs/cubefs/actions/workflows/ci.yml)
[![LICENSE](https://img.shields.io/github/license/cubefs/cubefs.svg)](https://github.com/cubefs/cubefs/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/cubefs/cubefs)](https://goreportcard.com/report/github.com/cubefs/cubefs)
[![Docs](https://img.shields.io/badge/docs-latest-green.svg)](https://cubefs.io/docs/master/overview/introduction.html)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/6232/badge)](https://www.bestpractices.dev/projects/6232)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/cubefs/cubefs/badge)](https://securityscorecards.dev/viewer/?uri=github.com/cubefs/cubefs)
[![Codecov](https://img.shields.io/codecov/c/github/cubefs/cubefs?style=flat-square&logo=codecov)](https://codecov.io/gh/cubefs/cubefs)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/cubefs)](https://artifacthub.io/packages/helm/cubefs/cubefs)
[![CLOMonitor](https://img.shields.io/endpoint?url=https://clomonitor.io/api/projects/cncf/chubao-fs/badge)](https://clomonitor.io/projects/cncf/chubao-fs)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fcubefs%2Fcubefs.svg?type=shield&issueType=security)](https://app.fossa.com/projects/git%2Bgithub.com%2Fcubefs%2Fcubefs?ref=badge_shield)
[![Release](https://img.shields.io/github/v/release/cubefs/cubefs.svg?color=161823&style=flat-square&logo=smartthings)](https://github.com/cubefs/cubefs/releases)
[![Tag](https://img.shields.io/github/v/tag/cubefs/cubefs.svg?color=ee8936&logo=fitbit&style=flat-square)](https://github.com/cubefs/cubefs/tags)
[![Gurubase](https://img.shields.io/badge/Gurubase-Ask%20CubeFS%20Guru-006BFF)](https://gurubase.io/g/cubefs)

|<img src="https://user-images.githubusercontent.com/5708406/91202310-31eaab80-e734-11ea-84fc-c1b1882ae71c.png" height="24"/>&nbsp;Community Meeting|
|------------------|
| The CubeFS Project holds bi-weekly community online meeting. To join or watch previous meeting notes and recordings, please see [meeting schedule](https://github.com/cubefs/community/wiki/Meeting-Schedule) and [meeting minutes](https://github.com/cubefs/community/wiki/Meeting-Agenda-and-Notes). |


<div width="100%" style="text-align:center;"><img alt="CubeFS" src="https://user-images.githubusercontent.com/12113219/178886968-9513e51e-393b-4af1-bd90-189593ee2012.png" height="200"/></div>


## Overview

CubeFS ("储宝" in Chinese) is an open-source cloud-native distributed file & object storage system, hosted by the [Cloud Native Computing Foundation](https://cncf.io) (CNCF) as a [graduated](https://www.cncf.io/projects/) project.

## What can you build with CubeFS

* As an open-source distributed storage, CubeFS can serve as your datacenter filesystem, data lake storage infra, and private or hybrid cloud storage. 
* Moreover, it can be run in public cloud services, providing cache acceleration and file system semantics on top of public cloud storage such as S3.

* In particular, CubeFS enables the separation of storage/compute architecture for databases, search systems, and AI/ML applications.

Some key features of CubeFS include:

- Multiple access protocols such as POSIX, HDFS, S3, and its own REST API
- Highly scalable metadata service with strong consistency  
- Performance optimization of large/small files and sequential/random writes
- Multi-tenancy support with better resource utilization and tenant isolation
- Hybrid cloud I/O acceleration through multi-level caching
- Flexible storage policies, high-performance replication or low-cost erasure coding


<div width="100%" style="text-align:center;"><img alt="CubeFS Architecture" src="https://raw.githubusercontent.com/cubefs/cubefs/master/docs/source/overview/pic/cfs-arch-ec.png"/></div>

## Documents

- English version: https://cubefs.io/docs/master/overview/introduction.html
- Chinese version: https://cubefs.io/zh/docs/master/overview/introduction.html

## Community

- Homepage: [cubefs.io](https://cubefs.io/)
- Mailing list: users@cubefs.groups.io. 
	- Please subscribe on the page https://groups.io/g/cubefs-users/ or send your email to cubefs-users+subscribe@groups.io to apply.
- Slack: [cubefs.slack.com](https://cubefs.slack.com/)
- WeChat: detail see [here](https://github.com/cubefs/cubefs/issues/604)
- Twitter: [cubefs_storage](https://twitter.com/cubefs_storage)

## Governance

[Governance documentation](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md) plays a crucial role in establishing clear guidelines, procedures, and structures within an organization or project

## Contribute
[Contributing to CubeFS](https://github.com/cubefs/cubefs/blob/master/CONTRIBUTING.md)

There is a clear definition of roles and their promotion paths.
- [Becoming a Maintainer](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-maintainer)
- [Becoming a committer](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-committer)
- [Becoming a TSC Member](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-tsc-member)


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
