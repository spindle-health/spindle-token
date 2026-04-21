from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import zipfile
from pathlib import Path


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
    wheel = sorted(dist_dir.glob("spindle_token-2.1.0-*.whl"))[-1]
    with zipfile.ZipFile(wheel) as zf:
        metadata = zf.read("spindle_token-2.1.0.dist-info/METADATA").decode()

    assert "Provides-Extra: spark" in metadata
    assert "Requires-Dist: pyspark (>=3.5.0,<4.2.0) ; extra == \"spark\"" in metadata
    assert "Requires-Dist: pyspark (>=3.5.0,<4.2.0)\n" not in metadata


def test_cli_help_imports_without_pyspark() -> None:
    project_src = Path(__file__).resolve().parents[1] / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        str(project_src)
        if not env.get("PYTHONPATH")
        else str(project_src) + os.pathsep + env["PYTHONPATH"]
    )

    code = textwrap.dedent(
        """
        import importlib.abc
        import sys

        from click.testing import CliRunner

        class BlockPySpark(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "pyspark" or fullname.startswith("pyspark."):
                    raise ModuleNotFoundError("No module named 'pyspark'")
                return None

        sys.meta_path.insert(0, BlockPySpark())

        from spindle_token._cli import cli

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0, result.output
        assert "tokenize" in result.output
        assert "transcode" in result.output
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
