# Welcome

Welcome to CubeFS!

-   [Contributing to CubeFS](#contributing-to-cubefs)
    -   [Bug Reports](#bug-reports)
    -   [Workflow](#workflow)
    -   [Contributor Recruitment](#contributor-recruitment)
-   [Development Guide](#development-guide)
    -   [Coding Style](#coding-style)
    -   [Commit Message Guidelines](#commit-message-guidelines)
    -   [Github Workflows](#github-workflows)

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

## Contributor Recruitment
- Current Recruitment：
	- Developer activity 2024 ：https://github.com/cubefs/cubefs/issues/3105
- History：
  - Summer of Open Source ：https://www.we2shopping.com/blog/2829327/
  - Developer activity 2023 ：https://github.com/cubefs/cubefs/issues/1920

# Development Guide

## Coding Style

- Prepare some code checking tools.
    ``` shell
    # gofumpt
    > go install mvdan.cc/gofumpt@latest

    # golangci-lint
    > go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.43.0

    ```
- Follow the [coding standards](https://go.dev/doc/effective_go) of Go.
- Ensure that your code is formatted using `gofumpt` before submitting it.
    ``` shell
    > gofumpt -l -w .
    ```
- Run some indispensable go lints on the project with your new code.
    ``` shell
    > go generate .

    # OR run in local docker
    > ./docker/run_docker.sh --format
    ```
- Ensure that each new source file starts with a license header.
- Ensure that there are enough unit tests.
- Ensure that there are well-documented comments.

## Commit Message Guidelines

- Follow the [Angular commit guidelines](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit).
    ``` text
    <type>(<scope>): <subject>
    <BLANK LINE>
    <body>
    <BLANK LINE>
    "close: #<issue_id>"
    <BLANK LINE>
    <footer>
    "Signed-off-by: <name> <email>"
    ```
- Commits must be signed and the signature must match the author `git commit --signoff`.
- If there is an issue, you need to link to related issues.
- The `subject` and `line of body` should not exceed 100 bytes.

Example:

> For example, the author information must match the Signed-off-by information.

```shell
Author: users <users@cubefs.groups.io>
Date:   Thu Apr 27 09:40:02 2023 +0800

    feat(doc): this is an example

    body's first line, explain more details of this commit,
    new line if the body is more than 100 bytes.

    close: #1

    empty or anything else
    Signed-off-by: users <users@cubefs.groups.io>
```

## Github Workflows

- `codeql`: Basic security code scanning.
- `ci`: Run code format, unit test and coverage, s3 service testing and gosec.
- `check_pull_request`: Check pull request title and new commit messages.
- `goreleaser`: Build release package.
- `slsa-releaser`: Build release package with [SLSA](https://slsa.dev/).
- `release_test`: Run ltp tests before released.

## Development, Testing, and Troubleshooting Guidelines

- Develop Guide [link](https://cubefs.io/docs/master/dev-guide/code.html)
- Testing Guide [link](https://cubefs.io/docs/master/evaluation/env.html)
- Best Practice Series
	- [Common Troubleshooting Methods](https://cubefs.io/docs/master/faq/troubleshoot/strategy.html)
	- [Common Troubleshooting Scenarios](https://cubefs.io/docs/master/faq/troubleshoot/case.html#common-troubleshooting-scenarios)
	- [Common Operational Issues Handling](https://mp.weixin.qq.com/s/cH9xw5sK80RIkkZWpyd4qA)
