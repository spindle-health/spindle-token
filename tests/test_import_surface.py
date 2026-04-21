from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def test_base_package_imports_without_pyspark() -> None:
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

        class BlockPySpark(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "pyspark" or fullname.startswith("pyspark."):
                    raise ModuleNotFoundError("No module named 'pyspark'")
                return None

        sys.meta_path.insert(0, BlockPySpark())

        import spindle_token
        import spindle_token._cli
        import spindle_token.opprl

        assert spindle_token.__version__ == "2.0.0"
        assert spindle_token.TokenProtocol is not None
        assert spindle_token.__all__ == [
            "PiiAttribute",
            "Token",
            "TokenProtocol",
            "tokenize",
            "transcode_out",
            "transcode_in",
            "generate_pem_keys",
        ]

        private_key, public_key = spindle_token.generate_pem_keys()
        assert private_key.startswith(b"-----BEGIN PRIVATE KEY-----")
        assert public_key.startswith(b"-----BEGIN PUBLIC KEY-----")

        try:
            spindle_token.tokenize(None, {}, [])
        except ImportError as exc:
            assert "optional 'spark' extra" in str(exc)
        else:
            raise AssertionError("expected ImportError")

        try:
            spindle_token.opprl.OpprlV2
        except ImportError as exc:
            assert "optional 'spark' extra" in str(exc)
        else:
            raise AssertionError("expected ImportError")
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


def test_opprl_import_form_works_with_pyspark() -> None:
    project_src = Path(__file__).resolve().parents[1] / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        str(project_src)
        if not env.get("PYTHONPATH")
        else str(project_src) + os.pathsep + env["PYTHONPATH"]
    )

    code = textwrap.dedent(
        """
        from spindle_token.opprl import OpprlV2, IdentityAttribute

        assert OpprlV2.token1.name == "opprl_token_1v2"
        assert IdentityAttribute is not None
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
