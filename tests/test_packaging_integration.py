from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest


@pytest.mark.integration
def test_wheel_metadata_marks_pyspark_as_spark_extra_only() -> None:
    result = subprocess.run(
        ["poetry", "build", "-f", "wheel"],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    dist_dir = Path(__file__).resolve().parents[1] / "dist"
    wheel = sorted(dist_dir.glob("spindle_token-2.0.0-*.whl"))[-1]
    with zipfile.ZipFile(wheel) as zf:
        metadata = zf.read("spindle_token-2.0.0.dist-info/METADATA").decode()

    assert "Provides-Extra: spark" in metadata
    assert 'Requires-Dist: pyspark (>=3.5.0,<4.2.0) ; extra == "spark"' in metadata
    assert "Requires-Dist: pyspark (>=3.5.0,<4.2.0)\n" not in metadata
