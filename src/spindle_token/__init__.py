"""A module containing the main API of spindle-token.

Most users will only need to use the 3 main functions in this top-level module along with the provided
configuration objects corresponding to OPPRL tokenization.

The 3 main functions provide tokenization and transcoding capabilities for data senders and recipients
respectively.

"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import lru_cache
from importlib import import_module
from typing import TYPE_CHECKING

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from spindle_token._compat import _is_missing_pyspark
from spindle_token.core import PiiAttribute, Token, TokenProtocol

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

__version__ = "2.2.0"

__all__ = [
    "PiiAttribute",
    "Token",
    "TokenProtocol",
    "tokenize",
    "transcode_out",
    "transcode_in",
    "generate_pem_keys",
]


@lru_cache(maxsize=1)
def _spark_api():
    try:
        return import_module("spindle_token._spark")
    except ModuleNotFoundError as exc:
        if _is_missing_pyspark(exc):
            raise ImportError(
                "spindle-token Spark functionality requires the optional 'spark' extra. "
                "Install with `pip install spindle-token[spark]`."
            ) from exc
        raise


def tokenize(
    df: DataFrame,
    col_mapping: Mapping[PiiAttribute, str],
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    return _spark_api().tokenize(df, col_mapping, tokens, private_key)


def transcode_out(
    df: DataFrame,
    tokens: Iterable[Token],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    return _spark_api().transcode_out(df, tokens, recipient_public_key, private_key)


def transcode_in(
    df: DataFrame,
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    return _spark_api().transcode_in(df, tokens, private_key)


def generate_pem_keys(key_size: int = 2048) -> tuple[bytes, bytes]:
    """Generates a fresh RSA key pair.

    Arguments:
        key_size:
            The size (in bits) of the key.

    Returns:
        A tuple containing the private key and public key bytes. Both in the PEM encoding.

    """
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )
    private = key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    public = key.public_key().public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )
    return private, public
