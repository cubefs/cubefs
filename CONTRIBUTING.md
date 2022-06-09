# Contributing to CubeFS

## Bug Reports

Please make sure the bug is not already reported by [searching the repository](https://github.com/cubefs/cubefs/search?q=&type=Issues&utf8=%E2%9C%93) with reasonable keywords. Then, [open an issue](https://github.com/cubefs/cubefs/issues) with steps to reproduce.

## Workflow

Recommend the standard GitHub flow based on forking and pull requests.

The following diagram and practice steps show the basic process of contributing code to CubeFS:

<img src="https://user-images.githubusercontent.com/5708406/87878246-9ddf4300-ca15-11ea-97a4-cffd1febffa1.png" height="520" alt="How to make contributing to CubeFS"></img>

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
