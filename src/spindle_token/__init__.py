"""A module containing the main API of spindle-token.

Most users will only need to use the 3 main functions in this top-level module along with the provided
configuration objects corresponding to OPPRL tokenization.

The 3 main functions provide tokenization and transcoding capabilities for data senders and recipients 
respectively.

"""

__version__ = "1.0.0"

from collections.abc import Mapping, Iterable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
    Encoding,
    PrivateFormat,
    PublicFormat,
    NoEncryption,
)
from spindle_token._crypto import private_key_from_env, public_key_from_env
from spindle_token.core import PiiAttribute, Token, TokenProtocol


__all__ = ["tokenize", "transcode_out", "transcode_in", "generate_pem_keys"]


def _bound_protocols(tokens: Iterable[Token], private_key: bytes, public_key: bytes | None):
    protocols: dict[str, TokenProtocol] = {}
    for token in tokens:
        if token.protocol.factory_id not in protocols:
            protocols[token.protocol.factory_id] = token.protocol.bind(private_key, public_key)
    return protocols


def tokenize(
    df: DataFrame,
    col_mapping: Mapping[PiiAttribute, str],
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    """Adds encrypted token columns based on PII.

    All PII columns found in the `DataFrame` are normalized according and transformed as needed according to the `col_mapping`.
    The PII attributes that make up of each [`Token`][spindle_token.core.Token] objects in `tokens` are then hashed and
    encrypted together according to their respective protocol versions.

    Arguments:
        df:
            The pyspark `DataFrame` containing all PII attributes.
        col_mapping:
            A dictionary that maps instances of [`PiiAttribute`][spindle_token.core.PiiAttribute] to the corresponding
            column name of `df`.
        tokens:
            A collection of [`Token`][spindle_token.core.Token] objects that denotes which tokens (from which PII attributes)
            should be added to the dataframe.
        private_key:
            Your private RSA key. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.

    Returns:
        The input `DataFrame` with by encrypted tokens added.
    """
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)

    protocols = _bound_protocols(tokens, private_key, public_key=None)

    token_columns = []
    for token in tokens:
        protocol = protocols[token.protocol.factory_id]
        token_column = protocol.tokenize(df, col_mapping, token.attributes).alias(token.name)
        token_columns.append(token_column)

    return df.select(col("*"), *token_columns)


def transcode_out(
    df: DataFrame,
    tokens: Iterable[Token],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    """Transcodes token columns of a dataframe into ephemeral tokens.

    Arguments:
        df:
            The pyspark `DataFrame` containing token columns.
        tokens:
            A collection of [`Token`][spindle_token.core.Token] objects that denote which columns of the input dataframe
            will be transcoded into ephemeral tokens.
        recipient_public_key:
            The public RSA key of the recipient who will be receiving the dataset with ephemeral tokens. Can also be supplied
            the SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY environment variable.
        private_key:
            Your private RSA key. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.
    Returns:
        The `DataFrame` with the tokens replaced by ephemeral tokens for sending to the recipient.
    """
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
            token.name: protocols[token.protocol.factory_id].transcode_out(col(token.name))
            for token in tokens
        }
    )


def transcode_in(
    df: DataFrame,
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    """Transcodes ephemeral token columns into normal tokens.

    Used by the data recipient of a dataset containing ephemeral tokens produced by [`transcode_out`][spindle_token.transcode_out]
    to transcode the ephemeral tokens such that they will match other datasets tokenized with the same private key.

    Arguments:
        df:
            Spark `DataFrame` with ephemeral token columns to transcode.
        tokens:
            A collection of [`Token`][spindle_token.core.Token] objects that denote which columns of the input dataframe
            will be transcoded from ephemeral tokens into normal tokens.
        private_key:
            Your private RSA key. Must be the corresponding private key for the public key given to the sender when calling
            `transcode_out`. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.

    Returns:
        The `DataFrame` with the ephemeral tokens replaced with normal tokens.
    """
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    protocols = _bound_protocols(tokens, private_key, public_key=None)
    return df.withColumns(
        {
            token.name: protocols[token.protocol.factory_id].transcode_in(col(token.name))
            for token in tokens
        }
    )


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
