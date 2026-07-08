"""
setup.py -- makes `pip install -r requirements.txt` install the project
itself in editable mode (via the `-e .` line) plus all dependencies.

The template pattern:
- requirements.txt lists deps AND ends with `-e .`
- get_requirements() reads that file but STRIPS `-e .` so setuptools
  never tries to install the package as a dependency of itself.
"""

from pathlib import Path

from setuptools import find_packages, setup

HYPHEN_E_DOT = "-e ."


def get_requirements(file_path: str = "requirements.txt") -> list[str]:
    """Read requirements.txt -> clean list (no comments, no blank, no -e .)."""
    reqs: list[str] = []
    for line in Path(file_path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line == HYPHEN_E_DOT:
            continue
        reqs.append(line)
    return reqs


setup(
    name="fraud-signal-lakehouse",
    version="0.1.0",
    author="Vedant Kale",
    description=(
        "Production fraud-signal lakehouse on AWS -- batch + streaming "
        "Iceberg platform feeding agentic-AI blockchain fraud research"
    ),
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=get_requirements(),
)
