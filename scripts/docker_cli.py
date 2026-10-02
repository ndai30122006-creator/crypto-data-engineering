"""Resolve Docker Desktop's CLI without changing machine-wide PATH."""

import os
import shutil
from pathlib import Path


def docker_executable() -> str:
    configured = os.getenv("DOCKER_EXE")
    if configured:
        return configured
    found = shutil.which("docker")
    if found:
        return found
    for root, relative in (
        (os.getenv("LOCALAPPDATA"), "Programs/DockerDesktop/resources/bin/docker.exe"),
        (os.getenv("PROGRAMFILES"), "Docker/Docker/resources/bin/docker.exe"),
    ):
        if root:
            candidate = Path(root) / relative
            if candidate.is_file():
                return str(candidate)
    return "docker"
