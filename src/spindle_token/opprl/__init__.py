"""Standard configurations for different versions of the OPPRL protocol.

This package is Spark-backed. Importing the package itself is safe, but accessing
OPPRL version objects requires the optional `spark` extra.
"""

from __future__ import annotations

from importlib import import_module

from spindle_token._compat import _is_missing_pyspark

__all__ = ["OpprlV0", "OpprlV1", "OpprlV2", "IdentityAttribute"]

_EXPORTS = {
    "OpprlV0": ("spindle_token.opprl.v0", "OpprlV0"),
    "OpprlV1": ("spindle_token.opprl.v1", "OpprlV1"),
    "OpprlV2": ("spindle_token.opprl.v2", "OpprlV2"),
    "IdentityAttribute": ("spindle_token.opprl._common", "IdentityAttribute"),
}


def _raise_spark_import_error(exc: ModuleNotFoundError) -> None:
    raise ImportError(
        "spindle_token.opprl requires the optional 'spark' extra. "
        "Install with `pip install spindle-token[spark]`."
    ) from exc


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        if _is_missing_pyspark(exc):
            _raise_spark_import_error(exc)
        raise

    value = getattr(module, attr_name)
    globals()[name] = value
    return value
