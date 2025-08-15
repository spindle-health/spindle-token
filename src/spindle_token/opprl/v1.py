from typing import ClassVar
from collections.abc import Iterable, Mapping
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from spindle_token.core import PiiAttribute, Token, TokenProtocol, TokenProtocolFactory
from spindle_token._crypto import (
    derive_aes_key,
    make_deterministic_encrypter,
    make_deterministic_decrypter,
    make_asymmetric_encrypter,
    make_asymmetric_decrypter,
)
from spindle_token.opprl._common import NameAttribute, GenderAttribute, DateAttribute
import spindle_token.opprl.v0 as v0


__all__ = ["OpprlV1"]


class _ProtocolV1(TokenProtocol):

    def __init__(self, private_key: bytes, recipient_public_key: bytes | None):
        aes_key = derive_aes_key(private_key)

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
        return v0._tokenize_impl(df, col_mapping, attributes, self.encrypt_aes)

    def transcode_out(self, token: Column) -> Column:
        if not self.encrypt_rsa:
            raise ValueError("No recipient public key provided")
        return v0._transcrypt_out_impl(token, self.decrypt_aes, self.encrypt_rsa)

    def transcode_in(self, ephemeral_token: Column) -> Column:
        return v0._transcrypt_in_impl(ephemeral_token, self.decrypt_rsa, self.encrypt_aes)


class _ProtocolFactoryV1(TokenProtocolFactory[_ProtocolV1]):

    def __init__(self, factory_id: str):
        super().__init__(factory_id)

    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> _ProtocolV1:
        return _ProtocolV1(private_key, recipient_public_key)


class OpprlV1:
    """All instances of [PiiAttribute][spindle_token.core.PiiAttribute], [Token][spindle_token.core.Token], and
    [TokenProtocolFactory][spindle_token.core.TokenProtocolFactory] for v1 of the OPPRL protocol.

    All members are class variables, and therefore this class does not need to be instantiated.
    """

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v1.first")

    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v1.last")

    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v1.gender")

    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v1.birth_date", "yyyy-MM-dd")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV1("opprl.v1")

    token1: ClassVar[Token] = Token(
        "opprl_token_1v1",
        protocol,
        (
            first_name.initial,
            last_name,
            gender,
            birth_date,
        ),
    )

    token2: ClassVar[Token] = Token(
        "opprl_token_2v1",
        protocol,
        (
            first_name.soundex,
            last_name.soundex,
            gender,
            birth_date,
        ),
    )

    token3: ClassVar[Token] = Token(
        "opprl_token_3v1",
        protocol,
        (
            first_name.metaphone,
            last_name.metaphone,
            gender,
            birth_date,
        ),
    )
