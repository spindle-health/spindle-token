from collections.abc import Iterable, Mapping
from warnings import warn

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)

from spindle_token._crypto import private_key_from_env, public_key_from_env
from spindle_token._utils import attr_id_to_col_name
from spindle_token.core import PiiAttribute, Token, TokenProtocol


def _bound_protocols(tokens: Iterable[Token], private_key: bytes, public_key: bytes | None):
    protocols: dict[str, TokenProtocol] = {}
    for token in tokens:
        if token.protocol.factory_id not in protocols:
            protocols[token.protocol.factory_id] = token.protocol.bind(private_key, public_key)
    return protocols


def _uses_mlkem_keys(tokens: Iterable[Token]) -> bool:
    protocol_ids = {token.protocol.factory_id for token in tokens}
    if "opprl.v3" not in protocol_ids:
        return False
    if protocol_ids != {"opprl.v3"}:
        raise ValueError(
            "OPPRL v3 ML-KEM-768 tokens cannot mix with other protocol versions "
            "in one operation; process them separately."
        )
    return True


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
            The private key bytes required by the selected protocol version. For v1/v2 this is an RSA PEM key;
            for v3 this is a raw 64-byte ML-KEM-768 seed. When omitted, v3 reads a base64-encoded seed from
            SPINDLE_TOKEN_PRIVATE_KEY while v1/v2 read RSA PEM from the same variable.

    Returns:
        The input `DataFrame` with by encrypted tokens added.
    """
    tokens = list(tokens)
    use_mlkem = _uses_mlkem_keys(tokens)
    # Treat empty bytes the same as a missing key so callers fall back to env config.
    if not private_key:
        private_key = private_key_from_env(raw_base64=use_mlkem)

    # Raise clear error message if key is invalid.
    if not use_mlkem:
        load_pem_private_key(private_key, None)

    protocols = _bound_protocols(tokens, private_key, public_key=None)

    # A collection of intermediate columns to drop before returning.
    to_drop: set[str] = set()

    # We create temporary normalized columns for each attribute used in any token to encourage
    # Spark to reuse columns in the query plan when attributes are used by multiple tokens.
    # This is particularly important when using Python UDFs because Spark struggles to detect
    # reusable expressions and collapse multiple `BatchEvalPython` into a single pass, which
    # adds to communication overhead. Materializing Python UDF inputs as column on the JVM before
    # invoking the Python UDFs eliminates this overhead.

    normalized_cols = []
    for attr, input_col in col_mapping.items():
        if input_col not in df.columns:
            raise ValueError(
                f"Input dataframe does not contain column {input_col} for attribute {attr}"
            )
        norm_name = attr_id_to_col_name(attr.attr_id)
        normalized_col = attr.transform(col(input_col), df.schema[input_col].dataType).alias(
            norm_name
        )
        normalized_cols.append(normalized_col)
        to_drop.add(norm_name)
    normalized = df.select(col("*"), *normalized_cols)

    # All available attributes, including derivatives. Initialized to just the directly provided attributes.
    all_attrs: dict[str, Column] = {
        attr.attr_id: col(attr_id_to_col_name(attr.attr_id)) for attr in col_mapping.keys()
    }

    for attr in col_mapping.keys():
        attr_name = attr_id_to_col_name(attr.attr_id)
        for derivative_id, derivate_attr in attr.derivatives().items():
            if derivative_id in all_attrs:
                # If the derivative attribute is provided directly, use the user-provided column.
                continue
            derivative_name = attr_id_to_col_name(derivative_id)
            all_attrs[derivative_id] = derivate_attr.transform(
                col(attr_name), normalized.schema[attr_name].dataType
            ).alias(derivative_name)
            to_drop.add(derivative_name)

    required_attrs = set()
    token_columns = []
    for token in tokens:
        required_attrs |= set(token.attribute_ids)
        protocol = protocols[token.protocol.factory_id]
        token_column = protocol.tokenize(token.attribute_ids).alias(token.name)
        token_columns.append(token_column)

    used = {attr_id: col for attr_id, col in all_attrs.items() if attr_id in required_attrs}
    all_pii = normalized.select(col("*"), *used.values())

    for attr_id in required_attrs:
        if attr_id_to_col_name(attr_id) not in all_pii.columns:
            raise ValueError(
                f"Required attribute {attr_id} not found in input data or derived attributes."
            )

    with_tokens = all_pii.select(col("*"), *token_columns)
    return with_tokens.drop(*to_drop)


def transcrypt_out(
    df: DataFrame,
    tokens: Iterable[Token],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    """Transcrypts token columns of a dataframe into ephemeral tokens.

    Arguments:
        df:
            The pyspark `DataFrame` containing token columns.
        tokens:
            A collection of [`Token`][spindle_token.core.Token] objects that denote which columns of the input dataframe
            will be transcrypted into ephemeral tokens.
        recipient_public_key:
            The recipient public key bytes for the selected protocol version: RSA PEM for v1/v2 or raw
            1,184-byte ML-KEM-768 data for v3. The v3 environment value is base64-encoded.
        private_key:
            The private key bytes for the selected protocol version: RSA PEM for v1/v2 or the raw 64-byte
            ML-KEM-768 seed for v3. If omitted, the environment value is interpreted according to the version.
    Returns:
        The `DataFrame` with the tokens replaced by ephemeral tokens for sending to the recipient.
    """
    tokens = list(tokens)
    use_mlkem = _uses_mlkem_keys(tokens)
    # Treat empty bytes the same as a missing key so callers fall back to env config.
    if not recipient_public_key:
        recipient_public_key = public_key_from_env(raw_base64=use_mlkem)
    # Treat empty bytes the same as a missing key so callers fall back to env config.
    if not private_key:
        private_key = private_key_from_env(raw_base64=use_mlkem)

    # Raise clear error message if key is invalid.
    if not use_mlkem:
        load_pem_private_key(private_key, None)
        load_pem_public_key(recipient_public_key)

    protocols = _bound_protocols(tokens, private_key, recipient_public_key)
    return df.withColumns(
        {
            token.name: protocols[token.protocol.factory_id].transcrypt_out(col(token.name))
            for token in tokens
        }
    )


def transcrypt_in(
    df: DataFrame,
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    """Transcrypts ephemeral token columns into normal tokens.

    Used by the data recipient of a dataset containing ephemeral tokens produced by [`transcrypt_out`][spindle_token.transcrypt_out]
    to transcrypt the ephemeral tokens such that they will match other datasets tokenized with the same private key.

    Arguments:
        df:
            Spark `DataFrame` with ephemeral token columns to transcrypt.
        tokens:
            A collection of [`Token`][spindle_token.core.Token] objects that denote which columns of the input dataframe
            will be transcrypted from ephemeral tokens into normal tokens.
        private_key:
            The private key bytes for the selected protocol version, corresponding to the public key given
            to the sender: RSA PEM for v1/v2 or the raw 64-byte ML-KEM-768 seed for v3. If omitted, the
            v3 environment value is base64-decoded.

    Returns:
        The `DataFrame` with the ephemeral tokens replaced with normal tokens.
    """
    tokens = list(tokens)
    use_mlkem = _uses_mlkem_keys(tokens)
    # Treat empty bytes the same as a missing key so callers fall back to env config.
    if not private_key:
        private_key = private_key_from_env(raw_base64=use_mlkem)

    # Raise clear error message if key is invalid.
    if not use_mlkem:
        load_pem_private_key(private_key, None)
    protocols = _bound_protocols(tokens, private_key, public_key=None)
    return df.withColumns(
        {
            token.name: protocols[token.protocol.factory_id].transcrypt_in(col(token.name))
            for token in tokens
        }
    )


def transcode_out(
    df: DataFrame,
    tokens: Iterable[Token],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    warn(
        "transcode_out() is deprecated; use transcrypt_out() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return transcrypt_out(df, tokens, recipient_public_key, private_key)


def transcode_in(
    df: DataFrame,
    tokens: Iterable[Token],
    private_key: bytes | None = None,
) -> DataFrame:
    warn(
        "transcode_in() is deprecated; use transcrypt_in() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return transcrypt_in(df, tokens, private_key)
