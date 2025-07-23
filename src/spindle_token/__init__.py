from collections.abc import Mapping, Iterable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)
from spindle_token._crypto import private_key_from_env, public_key_from_env
from spindle_token.core import PiiAttribute, Token, TokenProtocol


def _bound_protocols(tokens: Iterable[Token], private_key: bytes, public_key: bytes | None):
    protocols: dict[str, TokenProtocol] = {}
    for token in tokens:
        if token.protocol.id not in protocols:
            protocols[token.protocol.id] = token.protocol.bind(private_key, public_key)
    return protocols


def tokenize(
    df: DataFrame,
    col_mapping: Mapping[PiiAttribute, str],
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)

    protocols = _bound_protocols(tokens, private_key, public_key=None)

    token_columns = []
    for token in tokens:
        protocol = protocols[token.protocol.id]
        token_column = protocol.tokenize(df, col_mapping, token.attributes).alias(token.name)
        token_columns.append(token_column)

    return df.select(col("*"), *token_columns)


def transcrypt_out(
    df: DataFrame,
    tokens: Iterable[Token],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    if not recipient_public_key:
        recipient_public_key = public_key_from_env()
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    load_pem_public_key(recipient_public_key)

    protocols = _bound_protocols(tokens, private_key, recipient_public_key)
    return df.withColumns(
        {
            token.name: protocols[token.protocol.id].transcode_out(col(token.name))
            for token in tokens
        }
    )


def transcrypt_in(
    df: DataFrame,
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    protocols = _bound_protocols(tokens, private_key, public_key=None)
    return df.withColumns(
        {
            token.name: protocols[token.protocol.id].transcode_in(col(token.name))
            for token in tokens
        }
    )
