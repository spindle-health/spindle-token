from __future__ import annotations

from typing import ClassVar

from cryptography.hazmat.primitives.asymmetric import mlkem
from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

import spindle_token.opprl.v0 as v0
from spindle_token._crypto import (
    derive_aes_key_v3,
    make_deterministic_decrypter,
    make_deterministic_encrypter,
    make_kem_decrypter,
    make_kem_encrypter,
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
from spindle_token.opprl._v3_definitions import get_opprl_v3_definition

__all__ = ["OpprlV3"]


class _ProtocolV3(TokenProtocol):
    def __init__(self, private_key_seed: bytes, recipient_public_key: bytes | None):
        mlkem.MLKEM768PrivateKey.from_seed_bytes(private_key_seed)
        if recipient_public_key is not None:
            mlkem.MLKEM768PublicKey.from_public_bytes(recipient_public_key)

        aes_key = derive_aes_key_v3(private_key_seed)

        self.encrypt_aes = udf(make_deterministic_encrypter(aes_key), returnType=BinaryType())
        self.decrypt_aes = udf(make_deterministic_decrypter(aes_key), returnType=BinaryType())
        self.encrypt_kem = None
        if recipient_public_key:
            self.encrypt_kem = udf(
                make_kem_encrypter(recipient_public_key), returnType=BinaryType()
            )
        self.decrypt_kem = udf(make_kem_decrypter(private_key_seed), returnType=BinaryType())

    def tokenize(self, attribute_ids: list[str]) -> Column:
        return v0._tokenize_impl(attribute_ids, self.encrypt_aes)

    def transcrypt_out(self, token: Column) -> Column:
        if not self.encrypt_kem:
            raise ValueError("No recipient ML-KEM-768 public key provided")
        return v0._transcrypt_out_impl(token, self.decrypt_aes, self.encrypt_kem)

    def transcrypt_in(self, ephemeral_token: Column) -> Column:
        return v0._transcrypt_in_impl(ephemeral_token, self.decrypt_kem, self.encrypt_aes)

    def transcode_out(self, token: Column) -> Column:
        return self.transcrypt_out(token)

    def transcode_in(self, ephemeral_token: Column) -> Column:
        return self.transcrypt_in(ephemeral_token)


class _ProtocolFactoryV3(TokenProtocolFactory[_ProtocolV3]):
    def __init__(self, factory_id: str):
        super().__init__(factory_id)

    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> _ProtocolV3:
        return _ProtocolV3(private_key, recipient_public_key)


def _token(token_id: str, protocol: TokenProtocolFactory) -> Token:
    definition = get_opprl_v3_definition(token_id)
    return Token(definition.name, protocol, definition.attribute_ids)


class OpprlV3:
    """Token and attribute definitions for OPPRL v3 using ML-KEM-768."""

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v3.first")
    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v3.last")
    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v3.gender")
    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v3.birth_date", "yyyy-MM-dd")
    email: ClassVar[EmailAttribute] = EmailAttribute("opprl.v3.email")
    hem: ClassVar[HashedEmail] = HashedEmail(email.sha2.attr_id)
    phone: ClassVar[PhoneNumberAttribute] = PhoneNumberAttribute("opprl.v3.phone")
    ssn: ClassVar[SsnAttribute] = SsnAttribute("opprl.v3.ssn")
    group_number: ClassVar[GroupNumberAttribute] = GroupNumberAttribute("opprl.v3.group_number")
    member_id: ClassVar[MemberIdAttribute] = MemberIdAttribute("opprl.v3.member_id")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV3("opprl.v3")

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
