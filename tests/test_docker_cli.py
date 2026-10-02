from scripts.docker_cli import docker_executable


def test_finds_user_docker_desktop_without_path(monkeypatch, tmp_path):
    executable = tmp_path / "Programs/DockerDesktop/resources/bin/docker.exe"
    executable.parent.mkdir(parents=True)
    executable.touch()
    monkeypatch.delenv("DOCKER_EXE", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.setattr("scripts.docker_cli.shutil.which", lambda _: None)
    assert docker_executable() == str(executable)


def test_explicit_cli_override(monkeypatch):
    monkeypatch.setenv("DOCKER_EXE", "custom-docker")
    assert docker_executable() == "custom-docker"
def test_unreadable_path_falls_back_to_desktop(monkeypatch, tmp_path):
    from scripts import docker_cli

    executable = tmp_path / "Programs/DockerDesktop/resources/bin/docker.exe"
    executable.parent.mkdir(parents=True)
    executable.touch()
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.delenv("DOCKER_EXE", raising=False)

    def denied(_name):
        raise PermissionError("restricted PATH directory")

    monkeypatch.setattr(docker_cli.shutil, "which", denied)
    assert docker_cli.docker_executable() == str(executable)
