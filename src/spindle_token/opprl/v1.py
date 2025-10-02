from typing import ClassVar
from collections.abc import Mapping
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
from spindle_token.opprl._common import (
    EmailAttribute,
    GroupNumberAttribute,
    HashedEmail,
    MemberIdAttribute,
    NameAttribute,
    GenderAttribute,
    DateAttribute,
    PhoneNumberAttribute,
    SsnAttribute,
)
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
    ) -> Column:
        return v0._tokenize_impl(df, col_mapping, self.encrypt_aes)

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
            The tokenization protocol for producing OPPRL version 0 tokens.
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

    first_name: ClassVar[NameAttribute] = NameAttribute("opprl.v1.first")

    last_name: ClassVar[NameAttribute] = NameAttribute("opprl.v1.last")

    gender: ClassVar[GenderAttribute] = GenderAttribute("opprl.v1.gender")

    birth_date: ClassVar[DateAttribute] = DateAttribute("opprl.v1.birth_date", "yyyy-MM-dd")

    email: ClassVar[EmailAttribute] = EmailAttribute("opprl.v1.email")

    # This class var should be used when the user's data has a HEM column and therefore we don't specify _parent.
    hem: ClassVar[HashedEmail] = HashedEmail(email.sha2.attr_id)

    phone: ClassVar[PhoneNumberAttribute] = PhoneNumberAttribute("opprl.v1.phone")

    ssn: ClassVar[SsnAttribute] = SsnAttribute("opprl.v1.ssn")

    group_number: ClassVar[GroupNumberAttribute] = GroupNumberAttribute("opprl.v1.group_number")

    member_id: ClassVar[MemberIdAttribute] = MemberIdAttribute("opprl.v1.member_id")

    protocol: ClassVar[TokenProtocolFactory] = _ProtocolFactoryV1("opprl.v1")

    token1: ClassVar[Token] = Token(
        "opprl_token_1v1",
        protocol,
        (
            first_name.initial.attr_id,
            last_name.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )

    token2: ClassVar[Token] = Token(
        "opprl_token_2v1",
        protocol,
        (
            first_name.soundex.attr_id,
            last_name.soundex.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )

    token3: ClassVar[Token] = Token(
        "opprl_token_3v1",
        protocol,
        (
            first_name.metaphone.attr_id,
            last_name.metaphone.attr_id,
            gender.attr_id,
            birth_date.attr_id,
        ),
    )

    token4: ClassVar[Token] = Token(
        "opprl_token_4v1",
        protocol,
        (
            first_name.initial.attr_id,
            last_name.attr_id,
            birth_date.attr_id,
        ),
    )

    token5: ClassVar[Token] = Token(
        "opprl_token_5v1",
        protocol,
        (
            first_name.soundex.attr_id,
            last_name.soundex.attr_id,
            birth_date.attr_id,
        ),
    )

    token6: ClassVar[Token] = Token(
        "opprl_token_6v1",
        protocol,
        (
            first_name.metaphone.attr_id,
            last_name.metaphone.attr_id,
            birth_date.attr_id,
        ),
    )

    token7: ClassVar[Token] = Token(
        "opprl_token_7v1",
        protocol,
        (
            first_name.attr_id,
            phone.attr_id,
        ),
    )

    token8: ClassVar[Token] = Token(
        "opprl_token_8v1",
        protocol,
        (
            birth_date.attr_id,
            phone.attr_id,
        ),
    )

    token9: ClassVar[Token] = Token(
        "opprl_token_9v1",
        protocol,
        (
            first_name.attr_id,
            ssn.attr_id,
        ),
    )

    token10: ClassVar[Token] = Token(
        "opprl_token_10v1",
        protocol,
        (
            birth_date.attr_id,
            ssn.attr_id,
        ),
    )

    token11: ClassVar[Token] = Token(
        "opprl_token_11v1",
        protocol,
        (email.attr_id,),
    )

    token12: ClassVar[Token] = Token(
        "opprl_token_12v1",
        protocol,
        (hem.attr_id,),
    )

    token13: ClassVar[Token] = Token(
        "opprl_token_13v1",
        protocol,
        (
            group_number.attr_id,
            member_id.attr_id,
        ),
    )
