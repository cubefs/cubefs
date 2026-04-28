---
description: 自动补齐测试用例并校验 Go 增量覆盖率
---

# 增量覆盖率自动补测

用于“只针对增量改动补测试并校验覆盖率”的场景。

命令参数：

- `/incremental-coverage-autofix <base_commit>`
  - `base_commit` 可选
  - 传入时：检查 `<base_commit>..工作区(含未提交)` 的增量覆盖率
  - 不传时：默认只检查当前未提交改动（相对 `HEAD`）

示例：

```bash
/incremental-coverage-autofix 6101cc536350bac8ba84c57e45b01dc9731da805
/incremental-coverage-autofix
```

执行要求：

1. 先确定 diff 基线：
   - 如果用户给了 commit，就用它作为 `--base`
   - 否则默认检查当前未提交代码
2. 优先按**最小变更包范围**生成 `coverage.txt`，不要默认整仓 `bash build/build.sh testcover`
3. 用现有脚本检查增量覆盖率：
   - `python3 .cursor/skills/incremental-go-coverage/scripts/check_incremental_go_coverage.py ...`
4. 如果低于目标阈值：
   - 根据未覆盖 changed lines 补测试
   - 优先追加到已有 `_test.go`，不要无意义拆很多新测试文件
   - 优先覆盖关键逻辑、关键分支、配置透传和并发/状态变化路径
5. 每补一轮测试都重新执行：
   - 定向 `go test -coverprofile`
   - 增量覆盖率脚本
6. 结束时明确给出：
   - 当前增量覆盖率
   - 运行过的测试命令
   - 还没覆盖到的关键文件/关键路径

推荐命令模板：

```bash
base="${1:-}"
. build/cgo_env.sh
if [ -n "$base" ]; then
  changed_go_files=$(git diff --name-only "$base" -- '*.go')
else
  changed_go_files=$(
    {
      git diff --name-only HEAD -- '*.go'
      git ls-files --others --exclude-standard -- '*.go'
    }
  )
fi

pkgs=$(printf '%s\n' "$changed_go_files" | while read -r f; do
        [ -n "$f" ] || continue
        d=$(dirname "$f")
        [ "$d" = "." ] && echo "./" || echo "./$d"
      done | sort -u | xargs -r go list | sort -u
)
coverpkg=$(printf '%s\n' "$pkgs" | paste -sd, -)
go test -covermode=count -coverprofile coverage.txt -coverpkg="$coverpkg" $pkgs
check_cmd=(python3 .cursor/skills/incremental-go-coverage/scripts/check_incremental_go_coverage.py --repo . --coverprofile coverage.txt --threshold 80)
[ -n "$base" ] && check_cmd+=(--base "$base")
"${check_cmd[@]}"
```
