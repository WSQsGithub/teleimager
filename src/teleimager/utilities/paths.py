from pathlib import Path


def find_project_root(start_file: Path) -> Path:
    """Return the nearest ancestor containing pyproject.toml, or the start directory if none is found."""
    current = start_file.parent
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return current
