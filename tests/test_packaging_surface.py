from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def test_pyproject_marks_pyspark_as_spark_extra_only() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    text = pyproject.read_text()

    assert "[project.optional-dependencies]" in text
    assert 'spark = [\n    "pyspark (>=3.5.0,<4.2.0)",\n]' in text


def test_cli_help_imports_without_pyspark() -> None:
    project_src = Path(__file__).resolve().parents[1] / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        str(project_src)
        if not env.get("PYTHONPATH")
        else str(project_src) + os.pathsep + env["PYTHONPATH"]
    )

    code = textwrap.dedent("""
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
        """)

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
