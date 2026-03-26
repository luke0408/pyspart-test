import os
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from app.batch.cli import main


def _has_java_runtime() -> bool:
    command = ["java", "-version"]
    try:
        result = subprocess.run(command, capture_output=True, check=False)
        return result.returncode == 0
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
        force_container or not _has_java_runtime()
    ):
        raise SystemExit(_run_in_spark_container(args))
    raise SystemExit(main(args))
