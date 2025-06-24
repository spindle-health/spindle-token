import os
from collections.abc import Iterable, Sequence, Mapping
from dataclasses import dataclass
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    sha2,
    udf,
    to_binary,
)
from pyspark.sql.types import BinaryType
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)
from carduus.token.pii import (
    normalize_pii,
    enhance_pii,
    join_pii,
    PiiTransform,
    NameTransform,
    GenderTransform,
    DateTransform,
)
import carduus.token.crypto as crypto
from carduus.token._impl import base64_no_newline
from carduus.keys import derive_aes_key, private_key_from_env, public_key_from_env


__all__ = [
    "tokenize",
    "transcrypt_out",
    "transcrypt_in",
    "OpprlPii",
    "OpprlToken",
    "PiiTransform",
    "TokenSpec",
]


class OpprlPii(Enum):
    """Enum of [PiiTransform][carduus.token.PiiTransform] objects for the PII fields supported by the Open Privacy Preserving
    Record Linkage specification.

    Attributes:
        first_name:
            [PiiTransform][carduus.token.PiiTransform] implementation for a subject's first name according to the OPPRL standard.
        middle_name:
            [PiiTransform][carduus.token.PiiTransform] implementation for a subject's middle name according to the OPPRL standard.
        last_name:
            [PiiTransform][carduus.token.PiiTransform] implementation for a subject's last (aka family) name according to the OPPRL standard.
        gender:
            [PiiTransform][carduus.token.PiiTransform] implementation for a subject's gender according to the OPPRL standard.
        birth_date:
            [PiiTransform][carduus.token.PiiTransform] implementation for a subject's date of birth according to the OPPRL standard.

    """

    first_name: NameTransform = NameTransform("first")
    middle_name: NameTransform = NameTransform("middle")
    last_name: NameTransform = NameTransform("last")
    gender: GenderTransform = GenderTransform()
    birth_date: DateTransform = DateTransform("birth")  # @TODO: Parameterize date format


@dataclass(frozen=True)
class TokenSpec:
    """An collection of PII fields that will be encrypted together to create a token.

    For an enum of standard `TokenSpec` instances that comply with the Open Privacy Preserving Record Linkage protocol
    see [`OpprlToken`][carduus.token.OpprlToken].

    Attributes:
        name:
            The name of the column that holds these tokens.
        fields:
            The PII fields to encrypt together to create token values.
    """

    name: str
    fields: Iterable[str]


class OpprlToken(Enum):
    """Enum of [`TokenSpec`][carduus.token.TokenSpec] objects that meet the Open Privacy Preserving
    Record Linkage tokenization specification.

    Attributes:
        opprl_token_1:
            Token #1 from the OPPRL specification. Creates tokens based on `first_initial`, `last_name`, `gender`, and `birth_date`.
        opprl_token_2:
            Token #2 from the OPPRL specification. Creates tokens based on `first_soundex`, `last_soundex`, `gender`, and `birth_date`.
        orrpl_token_3:
            Token #3 from the OPPRL specification. Creates tokens based on the `first_metaphone`, `last_metaphone`, `gender`, and `birth_date`.

    """

    token1: TokenSpec = TokenSpec(
        "opprl_token_1",
        (
            "first_initial",
            OpprlPii.last_name.name,
            OpprlPii.gender.name,
            OpprlPii.birth_date.name,
        ),
    )

    token2: TokenSpec = TokenSpec(
        "opprl_token_2",
        (
            "first_soundex",
            "last_soundex",
            OpprlPii.gender.name,
            OpprlPii.birth_date.name,
        ),
    )

    token3: TokenSpec = TokenSpec(
        "opprl_token_3",
        (
            "first_metaphone",
            "last_metaphone",
            OpprlPii.gender.name,
            OpprlPii.birth_date.name,
        ),
    )


def tokenize(
    df: DataFrame,
    pii_transforms: Mapping[str, PiiTransform | OpprlPii],
    tokens: Sequence[TokenSpec | OpprlToken],
    private_key: bytes | None = None,
) -> DataFrame:
    """Adds encrypted token columns based on PII.

    All PII columns found in the `DataFrame` are normalized using the provided `pii_transforms`.
    All PII attributes provided by the enhancements of the `pii_transforms` are added if they
    are not already present in the `DataFrame`. The fields of each [`TokenSpec`][carduus.token.TokenSpec]
    from `tokens` are hashed and encrypted together according to the OPPRL specification.

    Arguments:
        df:
            The pyspark `DataFrame` containing all PII attributes.
        pii_transforms:
            A dictionary that maps column names of `df` to [PiiTransform][carduus.token.PiiTransform]
            objects to specify how each raw PII column is normalized and enhanced into derived PII attributes.
            Values can also be a member of the [OpprlPii][carduus.token.OpprlPii] enum if using the
            standard OPPRL tokens.
        tokens:
            A collection of [`TokenSpec`][carduus.token.TokenSpec] objects that denotes which PII attributes
            are encrypted into each token. Elements can also be a member of the [OpprlToken][carduus.token.OpprlToken]
            enum if using the standard OPPRL tokens.
        private_key:
            Your private RSA key. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.

    Returns:
        The `DataFrame` with PII columns replaced by encrypted tokens.

    """
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    pii_transforms_ = {
        c: tr.value if isinstance(tr, OpprlPii) else tr for c, tr in pii_transforms.items()
    }
    tokens_ = [t.value if isinstance(t, OpprlToken) else t for t in tokens]
    if len(tokens) == 0:
        return df
    token_columns = [token.name for token in tokens_]
    encrypt = udf(
        crypto.make_deterministic_encrypter(derive_aes_key(private_key)),
        returnType=BinaryType(),
    )

    pii = normalize_pii(df, pii_transforms_)
    pii, new_pii_columns = enhance_pii(pii, pii_transforms_)

    return (
        pii.withColumns(
            {t.name: join_pii(*[col(f) for f in sorted(t.fields)]) for t in tokens_}
        )
        .withColumns(
            {
                column: base64_no_newline(
                    encrypt(to_binary(sha2(col(column), 512), lit("hex")))
                )
                for column in token_columns
            }
        )
        .drop(*new_pii_columns)
    )


def transcrypt_out(
    df: DataFrame,
    token_columns: Iterable[str],
    recipient_public_key: bytes | None = None,
    private_key: bytes | None = None,
) -> DataFrame:
    """Prepares a `DataFrame` containing encrypted tokens to be sent to a specific trusted party by re-encrypting
    the tokens using the recipient's public key without exposing the original PII.

    Output tokens will be unmatchable to any dataset or within the given dataset until the intended recipient
    processes the data with [`transcrypt_in`][carduus.token.transcrypt_in].

    Arguments:
        df:
            Spark `DataFrame` with token columns to transcrypt.
        token_columns:
            The collection of column names that correspond to tokens.
        recipient_public_key:
            The public RSA key of the recipient who will be receiving the dataset with ephemeral tokens. Can also be supplied
            the SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY environment variable.
        private_key:
            Your private RSA key. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.

    Returns:
        The `DataFrame` with the original encrypted tokens re-encrypted for sending to the recipient.
    """
    if not recipient_public_key:
        recipient_public_key = public_key_from_env()
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    load_pem_public_key(recipient_public_key)
    decrypt = udf(
        crypto.make_deterministic_decrypter(derive_aes_key(private_key)),
        returnType=BinaryType(),
    )
    encrypt = udf(
        crypto.make_asymmetric_encrypter(recipient_public_key),
        returnType=BinaryType(),
    )
    return df.withColumns(
        {
            column: base64_no_newline(encrypt(decrypt(to_binary(col(column), lit("base64")))))
            for column in token_columns
        }
    )


def transcrypt_in(
    df: DataFrame,
    token_columns: Iterable[str],
    private_key: bytes | None = None,
) -> DataFrame:
    """Used by the recipient of a `DataFrame` containing tokens in the intermediate representation produced by
    [`transcrypt_out`][carduus.token.transcrypt_out] to re-encrypt the tokens such that they will match with
    other datasets

    Arguments:
        df:
            Spark `DataFrame` with token columns to transcrypt.
        token_columns:
            The collection of column names that correspond to tokens.
        private_key:
            Your private RSA key. This argument should only be set when reading from a secrets manager or testing, otherwise it is
            recommended to set the SPINDLE_TOKEN_PRIVATE_KEY environment variable with your private key.

    Returns:
        The `DataFrame` with the original encrypted tokens re-encrypted for sending to the destination.

    """
    if not private_key:
        private_key = private_key_from_env()

    # Raise clear error message if key is invalid.
    load_pem_private_key(private_key, None)
    decrypt = udf(crypto.make_asymmetric_decrypter(private_key), returnType=BinaryType())
    encrypt = udf(
        crypto.make_deterministic_encrypter(derive_aes_key(private_key)),
        returnType=BinaryType(),
    )
    return df.withColumns(
        {
            column: base64_no_newline(encrypt(decrypt(to_binary(col(column), lit("base64")))))
            for column in token_columns
        }
    )
