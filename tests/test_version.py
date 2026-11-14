from importlib.metadata import version
from pathlib import Path
import tomllib

import streambus


def test_package_version_matches_project_and_lockfile() -> None:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)
    with (root / "uv.lock").open("rb") as file:
        lock = tomllib.load(file)

    package = next(pkg for pkg in lock["package"] if pkg["name"] == "streambus")
    assert streambus.__version__ == project["project"]["version"]
    assert streambus.__version__ == package["version"]
    assert streambus.__version__ == version("streambus")
