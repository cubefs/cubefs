# Contributing to CubeFS

## Bug Reports

Please make sure the bug is not already reported by [searching the repository](https://github.com/cubefs/cubefs/search?q=&type=Issues&utf8=%E2%9C%93) with reasonable keywords. Then, [open an issue](https://github.com/cubefs/cubefs/issues) with steps to reproduce.

## Workflow

Recommend the standard GitHub flow based on forking and pull requests.

The following diagram and practice steps show the basic process of contributing code to CubeFS:

<img src="https://ocs-cn-north1.heytapcs.com/cubefs/github/workflow.png" height="520" alt="How to make contributing to CubeFS"></img>

1. Fork CubeFS to your repository.
2. Add remote for your forked repository.<br>(Example: `$ git remote add me https://github.com/your/cubefs`)
3. Make sure your local master branch synchronized with the master branch of main repository. <br>(Example: `$ git checkout master && git pull`)
4. Create local new branch from your up-to-dated local master branch, then checkout to it and leaves your changes. <br>(Example: `$ git branch your-branch && git checkout your-branch`)
5. Commit and push to your forked remote repository.<br>(Example: `$ git commit -s && git push me`)
6. Make a pull request that request merging your own branch on your forked repository into the master branch of the main repository.<br>(Example: merge `your/cubefs:your-branch` into `cubefs/cubefs:master`)

**Note 1:**<br>
The [DOC Check](https://github.com/apps/dco) is enabled and required. Please make sign your commit by using `-s` argument to add a valid `Signed-off-by` line at bottom of your commit message.<br>
Example:
```bash
$ git commit -s
```

**Note 2:**<br>
If your pull request solves an existing issue or implements a feature request with an existing issue. 
Please use the fixes keyword in the pull request to associate the pull request with the relevant issue.

**Note 3:**<br>
Every pull request that merges code to the master branch needs to be approved by at least one core maintainer for code review and pass all checks (including the DCO check) before it can be merged.

## Development Guide

### Coding style

- Follow the [coding standards](https://go.dev/doc/effective_go) of Go.
- Ensure that your code is formatted using `go fmt` or `gofumpt` before submitting it.
- Ensure that each new source file starts with a license header.
- Ensure that there are enough unit tests.
- Ensure that there are enough comments.

### Commit message guidelines

- Follow the [Angular commit guidelines](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit).
- Commits must be signed and the signature must match the author.
- If there is an issue, you need to link to related issues.

Example:

> For example, the author information must match the Signed-off-by information.

```shell
Author: users <users@cubefs.groups.io>
Date:   Thu Apr 27 09:40:02 2023 +0800

    feat(cubefs): this is an example
    
    close: #1
    
    Signed-off-by: users <users@cubefs.groups.io>
```