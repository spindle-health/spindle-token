from typing import ClassVar
from collections.abc import Callable, Iterable, Mapping
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    array,
    array_join,
    col,
    exists,
    isnull,
    lit,
    sha2,
    to_binary,
    udf,
    when,
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
from spindle_token._utils import null_propagating, base64_no_newline, attr_id_to_col_name
from spindle_token.opprl._common import NameAttribute, GenderAttribute, DateAttribute


__all__ = ["OpprlV0"]


def _derive_aes_key(rsa_key: bytes) -> bytes:
    """Derives an AES key from the given RSA private key using SHAKE

    This choice of key derivation function was replaced in OPPRL v1 by HKDF.
    """
    digest = Hash(SHAKE256(32))
    digest.update(rsa_key)
    return digest.finalize()


def _tokenize_impl(
    attribute_ids: list[str],
    encrypt_aes: Callable[[Column], Column],
) -> Column:
    # Attribute IDs sort lexicographically based on the attributes logical names (ie. first, last, email) as long
    # the token is being constructed from attributes from the same OPPRL version.
    # Otherwise the version number will sort first, which is deterministic even in the event that the same logical
    # attribute is used from multiple OPPRL versions for the same token. This is unlikely to be done in practice,
    # but we defend against it nonetheless.
    attribute_order = sorted(attribute_ids)
    attribute_cols = [col(attr_id_to_col_name(attr_id)) for attr_id in attribute_order]
    plaintext_str = array_join(array(*attribute_cols), delimiter=":")
    token = base64_no_newline(encrypt_aes(to_binary(sha2(plaintext_str, 512), lit("hex"))))
    return when(exists(array(*attribute_cols), isnull), lit(None)).otherwise(token)


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

    def tokenize(self, attribute_ids: list[str]) -> Column:
        return _tokenize_impl(attribute_ids, self.encrypt_aes)

    def transcode_out(self, token: Column) -> Column:
        if not self.encrypt_rsa:
            raise ValueError("No recipient public key provided")
        return _transcrypt_out_impl(token, self.decrypt_aes, self.encrypt_rsa)

    def transcode_in(self, ephemeral_token: Column) -> Column:
        return _transcrypt_in_impl(ephemeral_token, self.decrypt_rsa, self.encrypt_aes)


class _ProtocolFactoryV0(TokenProtocolFactory[_ProtocolV0]):

    def __init__(self, factory_id: str):
        super().__init__(factory_id)

    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> _ProtocolV0:
        return _ProtocolV0(private_key, recipient_public_key)


class OpprlV0:
    """All instances of [PiiAttribute][spindle_token.core.PiiAttribute], [Token][spindle_token.core.Token], and
    [TokenProtocolFactory][spindle_token.core.TokenProtocolFactory] for v0 of the OPPRL protocol.

    All members are class variables, and therefore this class does not need to be instantiated.

    Attributes:
        first_name:
            The PII attribute for a subject's first name.
        last_name:
            The PII attribute for a subject's last (aka family) name.
        gender:
            The PII attribute for a subject's gender.
        birth_date:
            The PII attribute for a subject's date of birth.
        protocol:
            The tokenization protocol for producing OPPRL version 0 tokens.
        token1:
            A token generated from first initial, last name, gender, and birth date.
        token2:
            A token generated from first soundex, last soundex, gender, and birth date.
        token3:
            A token generated from first metaphone, last metaphone, gender, and birth date.
    """

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v0.first")

    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v0.last")

    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v0.gender")

    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v0.birth_date", "yyyy-MM-dd")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV0("opprl.v0")

    token1: ClassVar[Token] = Token(
        "opprl_token_1v0",
        protocol,
        (
            first_name.initial.attr_id,
            last_name.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )

    token2: ClassVar[Token] = Token(
        "opprl_token_2v0",
        protocol,
        (
            first_name.soundex.attr_id,
            last_name.soundex.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )

    token3: ClassVar[Token] = Token(
        "opprl_token_3v0",
        protocol,
        (
            first_name.metaphone.attr_id,
            last_name.metaphone.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )
