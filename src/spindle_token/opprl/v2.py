from typing import ClassVar

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

import spindle_token.opprl.v0 as v0
from spindle_token._crypto import (
    make_asymmetric_decrypter,
    make_asymmetric_encrypter,
    make_deterministic_decrypter,
    make_deterministic_encrypter,
)
from spindle_token.core import Token, TokenProtocol, TokenProtocolFactory
from spindle_token.opprl._common import (
    DateAttribute,
    EmailAttribute,
    GenderAttribute,
    GroupNumberAttribute,
    HashedEmail,
    MemberIdAttribute,
    NameAttribute,
    PhoneNumberAttribute,
    SsnAttribute,
)
from spindle_token.opprl._v2_definitions import get_opprl_v2_definition

__all__ = ["OpprlV2"]


def _derive_aes_key(private_key: bytes) -> bytes:
    """Derive the v2 AES key from a canonicalized RSA private key.

    Version 2 canonicalizes the private key before deriving the symmetric key so the
    resulting token bytes are stable across equivalent PEM serializations of the same key.
    """

    key = load_pem_private_key(private_key, None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError(f"Incorrect key type. Expected RSA private key, got {type(key)}.")

    canonical_private_key = key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"opprl.v2.aes")
    return hkdf.derive(canonical_private_key)


class _ProtocolV2(TokenProtocol):

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
        return v0._tokenize_impl(attribute_ids, self.encrypt_aes)

    def transcode_out(self, token: Column) -> Column:
        if not self.encrypt_rsa:
            raise ValueError("No recipient public key provided")
        return v0._transcrypt_out_impl(token, self.decrypt_aes, self.encrypt_rsa)

    def transcode_in(self, ephemeral_token: Column) -> Column:
        return v0._transcrypt_in_impl(ephemeral_token, self.decrypt_rsa, self.encrypt_aes)


class _ProtocolFactoryV2(TokenProtocolFactory[_ProtocolV2]):

    def __init__(self, factory_id: str):
        super().__init__(factory_id)

    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> _ProtocolV2:
        return _ProtocolV2(private_key, recipient_public_key)


def _token(token_id: str, protocol: TokenProtocolFactory) -> Token:
    definition = get_opprl_v2_definition(token_id)
    return Token(definition.name, protocol, definition.attribute_ids)


class OpprlV2:
    """All instances of [PiiAttribute][spindle_token.core.PiiAttribute], [Token][spindle_token.core.Token], and
    [TokenProtocolFactory][spindle_token.core.TokenProtocolFactory] for v2 of the OPPRL protocol.

    Version 2 preserves the v1 token structure and normalization rules, but canonicalizes the
    private key before deriving the AES key so the same underlying RSA key produces the same
    tokens regardless of equivalent PEM serialization format.

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
        email:
            The PII attribute for a subject's email address.
        hem:
            The PII attribute for a subject's SHA2 hashed email address.
        phone:
            The PII attribute for a subject's phone number.
        ssn:
            The PII attribute for a subject's social security number.
        group_number:
            The PII attribute for a subject's health plan group number.
        member_id:
            The PII attribute for a subject's health plan member ID.
        protocol:
            The tokenization protocol for producing OPPRL version 2 tokens.
        token1:
            A token generated from first initial, last name, gender, and birth date.
        token2:
            A token generated from first soundex, last soundex, gender, and birth date.
        token3:
            A token generated from first metaphone, last metaphone, gender, and birth date.
        token4:
            A token generated from first initial, last name, and birth date.
        token5:
            A token generated from first soundex, last soundex, and birth date.
        token6:
            A token generated from first metaphone, last metaphone, and birth date.
        token7:
            A token generated from first name and phone number.
        token8:
            A token generated from birth date and phone number.
        token9:
            A token generated from first name and SSN.
        token10:
            A token generated from birth date and SSN.
        token11:
            A token generated from an email address.
        token12:
            A token generated from a SHA2 hashed email address.
        token13:
            A token generated from health plan group number and member ID.

    """

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v2.first")

    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v2.last")

    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v2.gender")

    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v2.birth_date", "yyyy-MM-dd")

    email: ClassVar[EmailAttribute] = EmailAttribute("opprl.v2.email")

    # This class var should be used when the user's data has a HEM column and therefore we don't specify _parent.
    hem: ClassVar[HashedEmail] = HashedEmail(email.sha2.attr_id)

    phone: ClassVar[PhoneNumberAttribute] = PhoneNumberAttribute("opprl.v2.phone")

    ssn: ClassVar[SsnAttribute] = SsnAttribute("opprl.v2.ssn")

    group_number: ClassVar[GroupNumberAttribute] = GroupNumberAttribute("opprl.v2.group_number")

    member_id: ClassVar[MemberIdAttribute] = MemberIdAttribute("opprl.v2.member_id")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV2("opprl.v2")

    token1: ClassVar[Token] = _token("token1", protocol)

    token2: ClassVar[Token] = _token("token2", protocol)

    token3: ClassVar[Token] = _token("token3", protocol)

    token4: ClassVar[Token] = _token("token4", protocol)

    token5: ClassVar[Token] = _token("token5", protocol)

    token6: ClassVar[Token] = _token("token6", protocol)

    token7: ClassVar[Token] = _token("token7", protocol)

    token8: ClassVar[Token] = _token("token8", protocol)

    token9: ClassVar[Token] = _token("token9", protocol)

    token10: ClassVar[Token] = _token("token10", protocol)

    token11: ClassVar[Token] = _token("token11", protocol)

    token12: ClassVar[Token] = _token("token12", protocol)

    token13: ClassVar[Token] = _token("token13", protocol)
