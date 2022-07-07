# Contributing to BlobStore

## Bug Reports

Please make sure the bug is not already reported by [searching the repository](https://github.com/cubefs/cubefs/search?q=&type=Issues&utf8=%E2%9C%93) with reasonable keywords. Then, [open an issue](https://github.com/cubefs/cubefs/issues) with steps to reproduce.

## Contribution flow

Recommend the standard GitHub flow based on forking and pull requests.

This is a rough outline of what a contributor's workflow looks like:
- Create a topic branch from where to base the contribution. This is usually main.
- Make commits of logical units.
- Make sure commit messages are in the proper format (see below).
- Push changes in a topic branch to a personal fork of the repository.
- Submit a pull request to BlobStore.
- The PR must receive a LGTM from two maintainers found in the MAINTAINERS file.

## Code style

The coding style suggested by the Golang community is used in BlobStore. See the style [doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make BlobStore easy to review, maintain and develop.

## Format of the commit message
We follow a rough convention for commit messages that is designed to answer two questions: what changed and why. The subject line should feature the what and the body of the commit should describe the why.
```
fix(clustermgr): fix truncate wal log error 
    
fix truncate wal log print error log when restart clustermgr.

fixes #38

Signed-off-by: xiangcai1215 <505892459@qq.com>
```
The format can be described more formally as follows:

```
<Type><package>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>
<BLANK LINE>
<sign-off>
```

The type should be one of the following:
- feat: A new feature
- fix: A bug fix
- perf: A code change that improves performance
- refactor: A code change that neither fixes a bug nor adds a feature
- style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- test: Adding missing tests or correcting existing tests


**Note 1:**<br>
If your pull request solves an existing issue or implements a feature request with an existing issue. 
Please use the fixes keyword in the pull request to associate the pull request with the relevant issue.

**Note 2:**<br>
Every pull request that merges code to the master branch needs to be approved by at least two maintainer for code review and pass all checks (including the DCO check) before it can be merged.