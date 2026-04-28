#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class CoverBlock:
    start_line: int
    end_line: int
    count: int


def run_git(repo: Path, args: List[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout


def read_module_path(repo: Path) -> Optional[str]:
    go_mod = repo / "go.mod"
    if not go_mod.exists():
        return None
    for line in go_mod.read_text(encoding="utf-8").splitlines():
        if line.startswith("module "):
            return line.split(None, 1)[1].strip()
    return None


def normalize_path(raw: str, repo: Path, module_path: Optional[str]) -> str:
    value = raw.strip()
    if not value:
        return value
    if os.path.isabs(value):
        try:
            return Path(value).resolve().relative_to(repo.resolve()).as_posix()
        except ValueError:
            return Path(value).as_posix()
    if module_path and value.startswith(module_path + "/"):
        return value[len(module_path) + 1 :]
    if value.startswith("./"):
        return value[2:]
    return value


def parse_hunk_new_lines(header: str) -> List[int]:
    plus_index = header.find("+")
    if plus_index == -1:
        return []
    end = header.find(" @@", plus_index)
    if end == -1:
        return []
    section = header[plus_index + 1 : end]
    if "," in section:
        start_s, count_s = section.split(",", 1)
    else:
        start_s, count_s = section, "1"
    start = int(start_s)
    count = int(count_s)
    if count <= 0:
        return []
    return list(range(start, start + count))


def collect_changed_lines_from_diff(repo: Path, base: Optional[str]) -> Dict[str, Set[int]]:
    diff_target = base if base else "HEAD"
    output = run_git(repo, ["diff", "--unified=0", "--no-color", diff_target, "--", "*.go"])
    changed: DefaultDict[str, Set[int]] = defaultdict(set)
    current_file: Optional[str] = None
    for line in output.splitlines():
        if line.startswith("+++ "):
            path = line[4:].strip()
            if path == "/dev/null":
                current_file = None
            elif path.startswith("b/"):
                current_file = path[2:]
            else:
                current_file = path
            if current_file and current_file.endswith("_test.go"):
                current_file = None
            continue
        if line.startswith("@@") and current_file:
            changed[current_file].update(parse_hunk_new_lines(line))
    return changed


def collect_untracked_go_files(repo: Path) -> Dict[str, Set[int]]:
    output = run_git(repo, ["ls-files", "--others", "--exclude-standard", "--", "*.go"])
    changed: Dict[str, Set[int]] = {}
    for rel in output.splitlines():
        if not rel:
            continue
        if rel.endswith("_test.go"):
            continue
        path = repo / rel
        if not path.exists():
            continue
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        changed[rel] = set(range(1, line_count + 1))
    return changed


def merge_changed_lines(*maps: Dict[str, Set[int]]) -> Dict[str, Set[int]]:
    merged: DefaultDict[str, Set[int]] = defaultdict(set)
    for mapping in maps:
        for path, lines in mapping.items():
            merged[path].update(lines)
    return dict(merged)


def parse_coverprofile(path: Path, repo: Path, module_path: Optional[str]) -> Dict[str, List[CoverBlock]]:
    if not path.exists():
        raise RuntimeError(f"coverprofile not found: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or not lines[0].startswith("mode: "):
        raise RuntimeError(f"{path} is not a Go coverprofile (expected first line to start with 'mode: ')")
    blocks: DefaultDict[str, List[CoverBlock]] = defaultdict(list)
    for line in lines[1:]:
        if not line.strip():
            continue
        try:
            file_part, rest = line.split(":", 1)
            span_part, _num_stmts, count_part = rest.split()
            start_part, end_part = span_part.split(",")
            start_line = int(start_part.split(".")[0])
            end_line = int(end_part.split(".")[0])
            count = int(count_part)
        except ValueError as exc:
            raise RuntimeError(f"invalid coverprofile line: {line}") from exc
        normalized = normalize_path(file_part, repo, module_path)
        blocks[normalized].append(CoverBlock(start_line, end_line, count))
    return dict(blocks)


def lines_covered_by_blocks(lines: Set[int], blocks: List[CoverBlock]) -> Tuple[Set[int], Set[int]]:
    relevant: Set[int] = set()
    covered: Set[int] = set()
    for line in lines:
        line_relevant = False
        line_covered = False
        for block in blocks:
            if block.start_line <= line <= block.end_line:
                line_relevant = True
                if block.count > 0:
                    line_covered = True
        if line_relevant:
            relevant.add(line)
        if line_covered:
            covered.add(line)
    return relevant, covered


def compress_lines(lines: List[int]) -> str:
    if not lines:
        return ""
    ranges: List[str] = []
    start = prev = lines[0]
    for value in lines[1:]:
        if value == prev + 1:
            prev = value
            continue
        ranges.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = value
    ranges.append(str(start) if start == prev else f"{start}-{prev}")
    return ",".join(ranges)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check incremental Go coverage for uncommitted changes or changes since a commit."
    )
    parser.add_argument("--repo", default=".", help="repository root, defaults to current directory")
    parser.add_argument("--coverprofile", required=True, help="path to a Go coverprofile file")
    parser.add_argument("--base", help="git revision to diff against; omit for current uncommitted changes")
    parser.add_argument("--threshold", type=float, default=80.0, help="minimum acceptable incremental coverage percentage")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    coverprofile = Path(args.coverprofile)
    if not coverprofile.is_absolute():
        coverprofile = (repo / coverprofile).resolve()

    module_path = read_module_path(repo)
    changed_lines = merge_changed_lines(
        collect_changed_lines_from_diff(repo, args.base),
        collect_untracked_go_files(repo),
    )
    if not changed_lines:
        print("No changed Go files found for the selected diff scope.")
        return 0

    cover_blocks = parse_coverprofile(coverprofile, repo, module_path)

    total_relevant = 0
    total_covered = 0
    uncovered_by_file: Dict[str, List[int]] = {}
    ignored_by_file: Dict[str, List[int]] = {}
    missing_profile_files: Dict[str, List[int]] = {}

    for path in sorted(changed_lines):
        lines = set(changed_lines[path])
        blocks = cover_blocks.get(path)
        if not blocks:
            missing_profile_files[path] = sorted(lines)
            total_relevant += len(lines)
            continue

        relevant, covered = lines_covered_by_blocks(lines, blocks)
        ignored = sorted(lines - relevant)
        uncovered = sorted(relevant - covered)

        total_relevant += len(relevant)
        total_covered += len(covered)

        if uncovered:
            uncovered_by_file[path] = uncovered
        if ignored:
            ignored_by_file[path] = ignored

    coverage = 100.0 if total_relevant == 0 else (total_covered / total_relevant) * 100.0
    scope = f"changes since {args.base}" if args.base else "current uncommitted changes"

    print(f"Incremental Go coverage: {coverage:.2f}% ({total_covered}/{total_relevant})")
    print(f"Threshold: {args.threshold:.2f}%")
    print(f"Scope: {scope}")
    print(f"Coverprofile: {coverprofile}")
    if module_path:
        print(f"Module path: {module_path}")

    if missing_profile_files:
        print("\nFiles missing from the coverprofile (counted as uncovered):")
        for path, lines in missing_profile_files.items():
            print(f"- {path}: {compress_lines(lines)}")

    if uncovered_by_file:
        print("\nUncovered changed lines:")
        for path, lines in uncovered_by_file.items():
            print(f"- {path}: {compress_lines(lines)}")

    if ignored_by_file:
        print("\nChanged lines ignored because they did not map to statement blocks:")
        for path, lines in ignored_by_file.items():
            print(f"- {path}: {compress_lines(lines)}")

    if coverage + 1e-9 < args.threshold:
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
