from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

from spindle_token.opprl.metadata import (
    get_opprl_v2_token_by_name,
    get_opprl_v2_tokens,
)
from spindle_token.opprl.v2 import OpprlV2


def test_opprl_v2_metadata_imports_without_pyspark() -> None:
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

        class BlockPySpark(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "pyspark" or fullname.startswith("pyspark."):
                    raise ModuleNotFoundError("No module named 'pyspark'")
                return None

        sys.meta_path.insert(0, BlockPySpark())

        from spindle_token.opprl.metadata import (
            get_opprl_v2_token_by_name,
            get_opprl_v2_tokens,
        )

        tokens = get_opprl_v2_tokens()
        assert len(tokens) == 13
        assert get_opprl_v2_token_by_name("opprl_token_11v2").token_id == "token11"
        assert "pyspark" not in sys.modules
        assert "pyspark.sql" not in sys.modules
        assert "spindle_token._spark" not in sys.modules
        assert "spindle_token.opprl.v2" not in sys.modules
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


def test_get_opprl_v2_token_by_name_returns_none_for_unknown_name() -> None:
    assert get_opprl_v2_token_by_name("unknown") is None


def test_opprl_v2_metadata_matches_runtime_tokens_in_order() -> None:
    runtime_tokens = {
        "token1": OpprlV2.token1,
        "token2": OpprlV2.token2,
        "token3": OpprlV2.token3,
        "token4": OpprlV2.token4,
        "token5": OpprlV2.token5,
        "token6": OpprlV2.token6,
        "token7": OpprlV2.token7,
        "token8": OpprlV2.token8,
        "token9": OpprlV2.token9,
        "token10": OpprlV2.token10,
        "token11": OpprlV2.token11,
        "token12": OpprlV2.token12,
        "token13": OpprlV2.token13,
    }

    metadata = get_opprl_v2_tokens()

    assert tuple(token.token_id for token in metadata) == tuple(runtime_tokens)
    for token_metadata in metadata:
        runtime_token = runtime_tokens[token_metadata.token_id]
        assert token_metadata.protocol == "opprl"
        assert token_metadata.version == "v2"
        assert token_metadata.name == runtime_token.name
        assert token_metadata.attribute_ids == tuple(runtime_token.attribute_ids)
