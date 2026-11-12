#!/usr/bin/env python3
"""Fail when changed Go, Python, or SQL source files exceed the project limit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SOURCE_SUFFIXES = {".go", ".py", ".sql"}
DEFAULT_LIMIT = 500


def count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as source:
        return sum(1 for _ in source)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", help="source files to inspect")
    parser.add_argument("--all", action="store_true", help="inspect every production source file in the repository")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args()

    if args.all:
        paths = [str(path) for path in Path(".").rglob("*") if path.is_file() and path.suffix in SOURCE_SUFFIXES]
    else:
        paths = args.paths
    if not paths:
        parser.error("provide source paths or use --all")

    violations: list[tuple[str, int]] = []
    for raw_path in paths:
        path = Path(raw_path)
        # The 500-line rule is for production source modules. Test suites
        # intentionally group many scenarios and are governed by test quality,
        # not production-module cohesion.
        if (
            path.suffix not in SOURCE_SUFFIXES
            or not path.is_file()
            or "tests" in path.parts
            or path.name.startswith("test_")
            or path.stem.endswith("_test")
        ):
            continue
        lines = count_lines(path)
        if lines > args.limit:
            violations.append((str(path), lines))

    if violations:
        for path, lines in violations:
            print(f"{path}: {lines} lines (limit: {args.limit})", file=sys.stderr)
        return 1

    print(f"File length check passed (limit: {args.limit} lines).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
