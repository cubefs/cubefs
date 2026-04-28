---
name: incremental-go-coverage
description: Checks and improves incremental Go test coverage for current uncommitted changes or changes since a specified commit, using changed-line coverprofile analysis. Use when the user asks to raise incremental coverage, auto-add tests for changed code, or validate changed-code coverage against a threshold such as 80%.
---

# Incremental Go Coverage

## When to use

Use this skill when the user wants to:

- raise coverage for current uncommitted Go changes
- check incremental coverage since a specific commit
- auto-add or extend tests until changed-code coverage reaches a threshold
- reach a threshold such as 80% before commit or merge

## Workflow

1. Decide the diff base:
   - current uncommitted changes: no base argument
   - a specific commit up to the current working tree: pass `--base <commit>`
2. Prefer a targeted coverprofile for the changed Go packages instead of full-repo `testcover`, unless the scope is unclear.
3. Run the incremental coverage checker script.
4. If coverage is below the threshold, inspect uncovered changed lines, add or update focused tests, and rerun until the threshold is met.

## Coverage generation

Keep the test log separate from the coverprofile file. Do not redirect stdout or `tee` into `coverage.txt`.

Preferred targeted workflow in this repository:

```bash
base=<commit>
. build/cgo_env.sh
pkgs=$(
  {
    git diff --name-only "$base" -- '*.go'
    git ls-files --others --exclude-standard -- '*.go'
  } | while read -r f; do
        [ -n "$f" ] || continue
        d=$(dirname "$f")
        [ "$d" = "." ] && echo "./" || echo "./$d"
      done | sort -u | xargs -r go list | sort -u
)
coverpkg=$(printf '%s\n' "$pkgs" | paste -sd, -)
go test -covermode=count -coverprofile coverage.txt -coverpkg="$coverpkg" $pkgs
python3 .cursor/skills/incremental-go-coverage/scripts/check_incremental_go_coverage.py --repo . --coverprofile coverage.txt --base "$base" --threshold 80
```

Fallback full-repo commands when package scope is broad or uncertain:

```bash
bash build/build.sh testcover > /tmp/testcover.log 2>&1
```

```bash
bash build/build.sh testcovercubefs > /tmp/testcover.log 2>&1
```

Focused package coverage is acceptable only when the coverprofile definitely includes every changed Go package.

## Commands

Current uncommitted Go changes:

```bash
python3 .cursor/skills/incremental-go-coverage/scripts/check_incremental_go_coverage.py --coverprofile coverage.txt --threshold 80
```

Changes since a commit, including committed, staged, unstaged, and untracked changes:

```bash
python3 .cursor/skills/incremental-go-coverage/scripts/check_incremental_go_coverage.py --coverprofile coverage.txt --base <commit> --threshold 80
```

## What the script reports

- overall incremental coverage percentage
- covered versus relevant changed lines
- uncovered changed lines grouped by file
- changed files missing from the coverprofile
- changed lines ignored because they do not map to statement blocks

## Agent guidance

- Do not count `_test.go` changes in the changed-code denominator.
- Treat files missing from the coverprofile as a real gap to fix first; usually the package was not included in the coverage run.
- Prefer appending to existing `_test.go` files before creating new test files.
- Prefer targeted package coverage when the changed package set is small. Prefer `build/build.sh testcover` or `testcovercubefs` only when the scope is broad or uncertain.
- Stop only when the reported threshold is met or when you have a concrete blocker to report.
