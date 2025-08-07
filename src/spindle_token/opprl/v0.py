from copy import copy
from typing import ClassVar
from collections.abc import Callable, Iterable, Mapping
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    array,
    array_join,
    col,
    lit,
    sha2,
    udf,
    to_binary,
)
from pyspark.sql.types import BinaryType
from cryptography.hazmat.primitives.hashes import Hash, SHAKE256

from spindle_token.core import PiiAttribute, Token, TokenProtocol, TokenProtocolFactory
from spindle_token._crypto import (
    make_deterministic_encrypter,
    make_deterministic_decrypter,
    make_asymmetric_encrypter,
    make_asymmetric_decrypter,
)
from spindle_token._utils import null_propagating, base64_no_newline
from spindle_token.opprl._common import NameAttribute, GenderAttribute, DateAttribute


def _derive_aes_key(rsa_key: bytes) -> bytes:
    """Derives an AES key from the given RSA private key using SHAKE

    This choice of key derivation function was replaced in OPPRL v1 by HKDF.
    """
    digest = Hash(SHAKE256(32))
    digest.update(rsa_key)
    return digest.finalize()


def _tokenize_impl(
    df: DataFrame,
    col_mapping: Mapping[PiiAttribute, str],
    attributes: Iterable[PiiAttribute],
    encrypt_aes: Callable[[Column], Column],
):
    """The implementation logic for `tokenize` introduced by OPPRL v0."""
    all_attr_input_columns: dict[str, str] = {}
    for attr, column_name in col_mapping.items():
        attr_and_derivatives = copy(attr.derivatives())
        attr_and_derivatives[attr.attr_id] = attr
        for attr_id, _ in attr_and_derivatives.items():
            all_attr_input_columns[attr_id] = column_name

    attributes = sorted(attributes, key=lambda f: f.attr_id)

    attr_columns = []
    for attr in attributes:
        input_column = all_attr_input_columns[attr.attr_id]
        attr_column = attr.transform(col(input_column), df.schema[input_column].dataType)
        attr_columns.append(attr_column)

    plaintext_str = array_join(null_propagating(array)(*attr_columns), delimiter=":")
    return base64_no_newline(encrypt_aes(to_binary(sha2(plaintext_str, 512), lit("hex"))))


def _transcrypt_out_impl(
    token: Column,
    decrypt_aes: Callable[[Column], Column],
    encrypt_rsa: Callable[[Column], Column],
):
    """The implementation logic for `tokenize_out` introduced by OPPRL v0."""
    return base64_no_newline(encrypt_rsa(decrypt_aes(to_binary(token, lit("base64")))))


def _transcrypt_in_impl(
    ephemeral_token: Column,
    decrypt_rsa: Callable[[Column], Column],
    encrypt_aes: Callable[[Column], Column],
):
    """The implementation logic for `tokenize_in` introduced by OPPRL v0."""
    return base64_no_newline(
        encrypt_aes(decrypt_rsa(to_binary(ephemeral_token, lit("base64"))))
    )


class _ProtocolV0(TokenProtocol):

    def __init__(self, private_key: bytes, recipient_public_key: bytes | None):
        aes_key = _derive_aes_key(private_key)

        self.encrypt_aes = udf(make_deterministic_encrypter(aes_key), returnType=BinaryType())
        self.decrypt_aes = udf(make_deterministic_decrypter(aes_key), returnType=BinaryType())
        self.encrypt_rsa = None
        if recipient_public_key:
            self.encrypt_rsa = udf(
                make_asymmetric_encrypter(recipient_public_key), returnType=BinaryType()
            )
        self.decrypt_rsa = udf(make_asymmetric_decrypter(private_key), returnType=BinaryType())

    def tokenize(
        self,
        df: DataFrame,
        col_mapping: Mapping[PiiAttribute, str],
        attributes: Iterable[PiiAttribute],
    ) -> Column:
        return _tokenize_impl(df, col_mapping, attributes, self.encrypt_aes)

    def transcode_out(self, token: Column) -> Column:
        if not self.encrypt_rsa:
            raise ValueError("No recipient public key provided")
        return _transcrypt_out_impl(token, self.decrypt_aes, self.encrypt_rsa)

    def transcode_in(self, ephemeral_token: Column) -> Column:
        return _transcrypt_in_impl(ephemeral_token, self.decrypt_rsa, self.encrypt_aes)


class _ProtocolFactoryV0(TokenProtocolFactory[_ProtocolV0]):

    def __init__(self, id: str):
        super().__init__(id)

    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> _ProtocolV0:
        return _ProtocolV0(private_key, recipient_public_key)


class OpprlV0():

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v0.first")

    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v0.last")

    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v0.gender")

    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v0.birth_date", "yyyy-MM-dd")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV0("opprl.v0")

    token1: ClassVar[Token] = Token(
        "opprl_token_1v0",
        protocol,
        (
            first_name.initial,
            last_name,
            gender,
            birth_date,
        ),
    )

    token2: ClassVar[Token] = Token(
        "opprl_token_2v0",
        protocol,
        (
            first_name.soundex,
            last_name.soundex,
            gender,
            birth_date,
        ),
    )

    token3: ClassVar[Token] = Token(
        "opprl_token_3v0",
        protocol,
        (
            first_name.metaphone,
            last_name.metaphone,
            gender,
            birth_date,
        ),
    )
