from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from app.batch.cli import main


def parse_java_major_version(version_output: str) -> int | None:
    match = re.search(r'"(\d+)(?:\.(\d+))?', version_output)
    if not match:
        return None

    first = int(match.group(1))
    second = match.group(2)

    if first == 1 and second is not None:
        return int(second)
    return first


def has_compatible_java_runtime(min_major: int = 17) -> bool:
    command = ["java", "-version"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return False

        version_output = f"{result.stdout}\n{result.stderr}".strip()
        major_version = parse_java_major_version(version_output)
        if major_version is None:
            return False

        return major_version >= min_major
    except OSError:
        return False


def _run_in_spark_container(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parent.parent
    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "spark",
        "python",
        "scripts/run_batch.py",
        *argv,
    ]
    result = subprocess.run(command, cwd=repo_root, check=False)
    return result.returncode


if __name__ == "__main__":
    args = sys.argv[1:]
    force_container = os.getenv("FORCE_SPARK_CONTAINER", "false").lower() == "true"
    if not Path("/.dockerenv").exists() and (
        force_container or not has_compatible_java_runtime()
    ):
        raise SystemExit(_run_in_spark_container(args))
    raise SystemExit(main(args))
