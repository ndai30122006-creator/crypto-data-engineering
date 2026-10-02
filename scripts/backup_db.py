"""Save a binary PostgreSQL backup locally before migrations/cutover."""

import argparse
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from docker_cli import docker_executable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path(".recovery"))
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    target = args.directory / f"crypto_db-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.dump"
    with target.open("xb") as output:
        subprocess.run(
            [docker_executable(), "exec", "crypto-postgres", "sh", "-c",
             'exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc'],
            stdout=output, check=True,
        )
    print(f"backup saved: {target} ({target.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
