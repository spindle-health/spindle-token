import pytest
from datetime import date
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import regexp_replace, substring
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from spindle_token import tokenize, transcode_out, transcode_in
from spindle_token._crypto import _PRIVATE_KEY_ENV_VAR, _RECIPIENT_PUBLIC_KEY_ENV_VAR
from spindle_token.core import PiiAttribute, Token
from spindle_token.opprl import OpprlV0 as v0, OpprlV1 as v1, IdentityAttribute


def test_tokenize_and_transcode_opprl(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
    all_tokens = [
        v0.token1,
        v0.token2,
        v0.token3,
        v1.token1,
        v1.token2,
        v1.token3,
        v1.token4,
        v1.token5,
        v1.token6,
        v1.token7,
        v1.token8,
        v1.token9,
        v1.token10,
        v1.token11,
        v1.token12,
        v1.token13,
    ]
    all_token_names = [token.name for token in all_tokens]

    pii = spark.createDataFrame(
        [
            Row(
                first_name="Louis",
                last_name="Pasteur",
                gender="male",
                birth_date="1822-12-27",
                email="lpasteur@notareal.email",
                phone="(234) 555-7890",
                ssn="666-12-3456",  # 666 is invalid
                group_number="FAKE123",
                member_id="12345",
            ),
            Row(
                first_name="louis",
                last_name="pasteur",
                gender="M",
                birth_date="1822-12-27",
                email="LPasteur@NotAReal.Email",
                phone="1-234-555-7890",
                ssn="123-34-0000",  # 0000 is invalid
                group_number="fake123",
                member_id="12345",
            ),
        ]
    )
    pii_col_mapping = {
        v0.first_name: "first_name",
        v0.last_name: "last_name",
        v0.gender: "gender",
        v0.birth_date: "birth_date",
        v1.first_name: "first_name",
        v1.last_name: "last_name",
        v1.gender: "gender",
        v1.birth_date: "birth_date",
        v1.email: "email",
        v1.phone: "phone",
        v1.ssn: "ssn",
        v1.group_number: "group_number",
        v1.member_id: "member_id",
    }

    tokens = tokenize(
        pii,
        col_mapping=pii_col_mapping,
        tokens=all_tokens,
        private_key=private_key,
    )
    assertDataFrameEqual(
        tokens,
        (
            spark.createDataFrame(
                [
                    Row(
                        first_name="Louis",
                        last_name="Pasteur",
                        gender="male",
                        birth_date="1822-12-27",
                        email="lpasteur@notareal.email",
                        phone="(234) 555-7890",
                        ssn="666-12-3456",  # 666 is invalid
                        group_number="FAKE123",
                        member_id="12345",
                        opprl_token_1v0="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2v0="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3v0="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_1v1="dmJAQHj05pZXDYudIx7W+aeZhZXFDULhNoxJGMl2+U5kr/1lFQF1uCaN9vbjBhsQRkImFBz3+F0kxm4lF3bNoes9B3idmq4pwGXD6ey1ncg=",
                        opprl_token_2v1="MdSTC2ZfBYhiavwgFX7Rfgoh5p5Qn7u6fSbTxvi9jfify9NxfP7hKvfP1f6u2hdQR868tgcHT7l/ofc1Hjf/yfC52lbpQG6kx1d1+ZuT6Ek=",
                        opprl_token_3v1="gQFs1BAXwzBWi354bWgFimYO9NdmQ/a+DwtKwwV0oRnvs9O4i5LobZpn8aIVpd4BFAiEPvSBWGdm+xXF+CoM7vF4B3w8TH+Rugo1tXj2Z1Q=",
                        opprl_token_4v1="RG5MT/oM5XxSw6JDthbnPQ7WijnOK5U04nReoHeEayVbqZ+SxVQMIMVWQ2daxFrQU1eDbKouWKJYMDDG8BH8Hm4m96KuPBZQXfDVeYLn/mY=",
                        opprl_token_5v1="SZYc9LpLOanS5z8dDDFr9wWjB0sx9OVgr9ZlsSIKWHe8bH3NNo8zFxYgO1Hm8y8H+8zUf4wDT9I5o6xvnSnA7ZLzKcYik5oNAX6i6liF+KA=",
                        opprl_token_6v1="KpudmHkWqRkyhJNosyar4XBMCiwlbHsf6a//wWJPdsxHW5Nb38VehsB+kiBwTkNTQAtZ8bbqe5DCC+Sm2iXnFNV0guZ633V2MV9oAnBz8T8=",
                        opprl_token_7v1="k3yUcq8i0yQjIQoeqSmVdkemCKjhWSR7xEPhaZBWX0zovoEcB8aXlYd0Jy0AR6wPB26dDjZcATaB6AkC2lRFIH/04J3UVl3O/gOQTn37Qec=",
                        opprl_token_8v1="a4vmZ92xnsZs6sTZqfczaSdSXY73uSrPFifb8ltGgZWKAQWYIUzC6dpiCurkptP4eKetkLU3CTvD5qwRGSJVJ30ACSr9hLWNPJpNn5ln+fc=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="/vpadIbx+oxw9a8Wx9rHhZ4edINxNj3pkfToQ1QiOrMjUnEH2J0z1kBQGTKdMbZUikvyJtXFzETsnLZPIqs6MK0LDTY6KSmhUcBs9rS+RXw=",
                        opprl_token_12v1="LzlHN2TKdivR6w1YsbCXYE2wfRctMXikdR6zI+93HFPugcWqLlZp92HZoWM1H2bU6KLvseIDjbTtBLBvW8LtzrDj7XDNqlQNeOu2ilKkSxs=",
                        opprl_token_13v1="sQuipS/1xLcG5g5LONknoxG8fIv0yLBvXRFA4jJyjk9Im2z543wd1mHLDuQPtM7bS9CkVM81J/ZI6w73pHUgrKeoHDWVTnRj6slAMHqPmKY=",
                    ),
                    Row(
                        first_name="louis",
                        last_name="pasteur",
                        gender="M",
                        birth_date="1822-12-27",
                        email="LPasteur@NotAReal.Email",
                        phone="1-234-555-7890",
                        ssn="123-34-0000",  # 0000 is invalid
                        group_number="fake123",
                        member_id="12345",
                        opprl_token_1v0="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2v0="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3v0="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_1v1="dmJAQHj05pZXDYudIx7W+aeZhZXFDULhNoxJGMl2+U5kr/1lFQF1uCaN9vbjBhsQRkImFBz3+F0kxm4lF3bNoes9B3idmq4pwGXD6ey1ncg=",
                        opprl_token_2v1="MdSTC2ZfBYhiavwgFX7Rfgoh5p5Qn7u6fSbTxvi9jfify9NxfP7hKvfP1f6u2hdQR868tgcHT7l/ofc1Hjf/yfC52lbpQG6kx1d1+ZuT6Ek=",
                        opprl_token_3v1="gQFs1BAXwzBWi354bWgFimYO9NdmQ/a+DwtKwwV0oRnvs9O4i5LobZpn8aIVpd4BFAiEPvSBWGdm+xXF+CoM7vF4B3w8TH+Rugo1tXj2Z1Q=",
                        opprl_token_4v1="RG5MT/oM5XxSw6JDthbnPQ7WijnOK5U04nReoHeEayVbqZ+SxVQMIMVWQ2daxFrQU1eDbKouWKJYMDDG8BH8Hm4m96KuPBZQXfDVeYLn/mY=",
                        opprl_token_5v1="SZYc9LpLOanS5z8dDDFr9wWjB0sx9OVgr9ZlsSIKWHe8bH3NNo8zFxYgO1Hm8y8H+8zUf4wDT9I5o6xvnSnA7ZLzKcYik5oNAX6i6liF+KA=",
                        opprl_token_6v1="KpudmHkWqRkyhJNosyar4XBMCiwlbHsf6a//wWJPdsxHW5Nb38VehsB+kiBwTkNTQAtZ8bbqe5DCC+Sm2iXnFNV0guZ633V2MV9oAnBz8T8=",
                        opprl_token_7v1="k3yUcq8i0yQjIQoeqSmVdkemCKjhWSR7xEPhaZBWX0zovoEcB8aXlYd0Jy0AR6wPB26dDjZcATaB6AkC2lRFIH/04J3UVl3O/gOQTn37Qec=",
                        opprl_token_8v1="a4vmZ92xnsZs6sTZqfczaSdSXY73uSrPFifb8ltGgZWKAQWYIUzC6dpiCurkptP4eKetkLU3CTvD5qwRGSJVJ30ACSr9hLWNPJpNn5ln+fc=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="/vpadIbx+oxw9a8Wx9rHhZ4edINxNj3pkfToQ1QiOrMjUnEH2J0z1kBQGTKdMbZUikvyJtXFzETsnLZPIqs6MK0LDTY6KSmhUcBs9rS+RXw=",
                        opprl_token_12v1="LzlHN2TKdivR6w1YsbCXYE2wfRctMXikdR6zI+93HFPugcWqLlZp92HZoWM1H2bU6KLvseIDjbTtBLBvW8LtzrDj7XDNqlQNeOu2ilKkSxs=",
                        opprl_token_13v1="sQuipS/1xLcG5g5LONknoxG8fIv0yLBvXRFA4jJyjk9Im2z543wd1mHLDuQPtM7bS9CkVM81J/ZI6w73pHUgrKeoHDWVTnRj6slAMHqPmKY=",
                    ),
                ],
                StructType(
                    [
                        StructField("first_name", StringType()),
                        StructField("last_name", StringType()),
                        StructField("gender", StringType()),
                        StructField("birth_date", StringType()),
                        StructField("email", StringType()),
                        StructField("phone", StringType()),
                        StructField("ssn", StringType()),
                        StructField("group_number", StringType()),
                        StructField("member_id", StringType()),
                    ]
                    + [StructField(t, StringType()) for t in all_token_names]
                ),
            )
        ),
    )
    ephemeral_tokens = transcode_out(
        tokens.select(*all_token_names),
        tokens=all_tokens,
        recipient_public_key=acme_public_key,
        private_key=private_key,
    )

    assertSchemaEqual(
        ephemeral_tokens.schema,
        StructType([StructField(token, StringType()) for token in all_token_names]),
    )
    # When transferring between parties, tokens from the same PII should _not_ be equal.
    assert ephemeral_tokens.distinct().count() == 2

    tokens2 = transcode_in(
        ephemeral_tokens.select(*all_token_names),
        tokens=all_tokens,
        private_key=acme_private_key,
    )

    # Check that trancryption and tokenization produce the same result.
    assertDataFrameEqual(
        tokens2,
        tokenize(
            pii,
            col_mapping=pii_col_mapping,
            tokens=all_tokens,
            private_key=acme_private_key,
        ).select(*all_token_names),
    )

    # Test for token stability across changes.
    assertDataFrameEqual(
        tokens2,
        (
            spark.createDataFrame(
                [
                    Row(
                        opprl_token_1v0="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2v0="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3v0="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                        opprl_token_1v1="qGPiZl9WXPsfIiHADpX3M5sR94fQWrsJBvE/jkvuGQ6Xq9OXPanO2urYEO+Jnzfn4b4RTqfT7QAVQufKYmQiHIf9zdzSXAYj70fsGcGEogw=",
                        opprl_token_2v1="7T3UwgmZdzoegjSb5q3uIk5phUsIwjYQGNvdOoLCOfHvLspWNOX/w52OG5lkHedL4/MG2SPL4PHwPLNjBnLnErRqIKsWtjXXQrJdEHmTWOc=",
                        opprl_token_3v1="2tKP3S1R1+5W23ERlImstJnAy6RQ2MRgXBswDdCKIkK7d2Xr0OudL9Kg2tN2vbEiRLg/h2pIcZW1F9l3ECb6Bg3wFLni1HcqXGGkeDJ4cnY=",
                        opprl_token_4v1="mdZmjIYg4q9fuYyurjrNt1lxilsVz3mqnxVyQ4zzPxmNjL99UJT9GR/yzhxgl8liavaMVQjYPQT6hCbl2qNK9NUxnTR1J1g7vu706F24MA8=",
                        opprl_token_5v1="iw0t4M3VuT4zpW0o1LCUaOorJ7E57/qWC0ywgNhDl6Q4nmgdfrpZhLpzccENoygEMnkU3PqrOpDQVO75WfmhWWijxU5X1PuTHOEasqY5V3Q=",
                        opprl_token_6v1="oe6ZhWJdayyzP7IhxARFMBnY6/NykSOiX+3PgxyY63sdlrjCHr2Rvdo+Y5hWYUoUIq1hRb2fd7RgruFrzXZaqsh5VqttHjak92wLHxQFEa4=",
                        opprl_token_7v1="s8bu1NTTLydR9W5sH74l6WoINx8u8+ceYjHiAqdEGvwnbuyQaEJ8MB5SJX1yT48e7YrqYHkV5E4FhblX96sZ+WfBHbrTagp6B7Ti7spg0As=",
                        opprl_token_8v1="Ow5wsAcsuJVvhJ+73pWlM+nI4/u4UxFqLLGlZERlMmQxlO4Uf+IRyf8S6bwcv+2yjbC+HjdJy5Cerab4srl+1zkW7tJ9Fw6F5BzchB3HpGw=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="U/5wghVdYXB0H3/u3saz2STCknPHKJ33BTUKRyb1KlZiCE11LYi7qZFO5Lu3w997m725NQ5u/XBL2f347uXR0FOny7Y0Yo3F3rPKkbrRON8=",
                        opprl_token_12v1="zJUcAWaPTBiPafL39v5nxKFm9mgwSuzPsuyNIlpDRkHScemg8V1WIvJvpFYv4ybcbgQdDPdTfuHbMUfiCKa4yO4Q1QLlf7rQaHwtdxTpLaw=",
                        opprl_token_13v1="FM3HeQXo8U+bZBcrVScValuGVoEcpmlCWgAjGy1tfHgdn/qTshDuRGRMHVxKvHbYixt9r6K91QnzkKLnWWtJbUwLHfLg0b1TdQgm8CvvIIM=",
                    )
                ]
                * 2,
                StructType([StructField(t, StringType()) for t in all_token_names]),
            )
        ),
    )


def test_identity_attribute(spark: SparkSession, private_key: bytes):
    pii = spark.createDataFrame(
        [
            Row(
                email="NotA@Real.Email",
                hem="1f3e6adb230cbd09a16662c9395050fe63f79bd2759305525a185cfda3998e79",
            ),
        ]
    )
    token_from_email = tokenize(pii, {v1.email: "email"}, [v1.token12], private_key).head()
    token_from_hem = tokenize(pii, {v1.hem: "hem"}, [v1.token12], private_key).head()
    token_from_identity = tokenize(
        pii, {IdentityAttribute("opprl.v1.email.sha2"): "hem"}, [v1.token12], private_key
    ).head()
    assert token_from_email
    assert token_from_hem
    assert token_from_identity
    assert token_from_email == token_from_hem == token_from_identity


class _Zip3Attribute(PiiAttribute):
    def __init__(self, underlying: "ZipcodeAttribute"):
        super().__init__(f"{underlying.attr_id}.zip3")
        self.underlying = underlying

    def transform(self, column, dtype):
        return substring(self.underlying.transform(column, dtype), 1, 3)


class ZipcodeAttribute(PiiAttribute):
    def transform(self, column, _):
        return regexp_replace(column, "[^0-9]", "")

    @property
    def zip3(self) -> _Zip3Attribute:
        return _Zip3Attribute(self)

    def derivatives(self):
        attrs = super().derivatives()
        attrs.update(self.zip3.derivatives())
        return attrs


def test_custom_pii_and_token(spark: SparkSession, private_key: bytes):
    zipAttr = ZipcodeAttribute("test.zipcode")
    assertDataFrameEqual(
        tokenize(
            (
                spark.createDataFrame(
                    [
                        Row(
                            id=1,
                            first_name="MARIE",
                            last_name="Curie",
                            gender="f",
                            birth_date="1867-11-07",
                            zipcode="none",
                        ),
                        Row(
                            id=2,
                            first_name="Pierre",
                            last_name="Curie",
                            gender="m",
                            birth_date="1859-05-15",
                            zipcode="none",
                        ),
                        Row(
                            id=3,
                            first_name="Jonas",
                            last_name="Salk",
                            gender="m",
                            birth_date="1914-10-28",
                            zipcode="10016",
                        ),
                    ]
                )
            ),
            col_mapping={
                v1.first_name: "first_name",
                v1.last_name: "last_name",
                v1.gender: "gender",
                v1.birth_date: "birth_date",
                zipAttr: "zipcode",
            },
            tokens=[
                Token(
                    "custom_token", v1.protocol, (v1.last_name.attr_id, zipAttr.zip3.attr_id)
                ),
                v1.token1,
            ],
            private_key=private_key,
        ),
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        first_name="MARIE",
                        last_name="Curie",
                        gender="f",
                        birth_date="1867-11-07",
                        zipcode="none",
                        custom_token="wqj0vIgw/ZyOXxUv+0vNNgQRjD0PDE+mpmoDV87VZtW29Ln9FqP+RD+J5qDxCR55L8DnRVG2glOuMfH+f0ab5xecgRJKiDYQxZclpbtllAo=",
                        opprl_token_1v1="jhAhAyXgsHfitM9RP2wmCylqmZKyBeqVT1/4MUwPSxTtQnkkIAlyp7roClPq+uyxUnbWmJZ8OKHv3gNjr93dMGyW17xeeNPUtFfBG4aJKtc=",
                    ),
                    Row(
                        id=2,
                        first_name="Pierre",
                        last_name="Curie",
                        gender="m",
                        birth_date="1859-05-15",
                        zipcode="none",
                        custom_token="wqj0vIgw/ZyOXxUv+0vNNgQRjD0PDE+mpmoDV87VZtW29Ln9FqP+RD+J5qDxCR55L8DnRVG2glOuMfH+f0ab5xecgRJKiDYQxZclpbtllAo=",
                        opprl_token_1v1="KX2pApi2629jaAQ723ME27MdAoOC0yR3szhnEVMQ6XN+ZWPSETEMSSY90ZBZRvwS7ATX0xBz/77fHzNgVuRdMKKkjK8STwDDgIIeHdRGBGc=",
                    ),
                    Row(
                        id=3,
                        first_name="Jonas",
                        last_name="Salk",
                        gender="m",
                        birth_date="1914-10-28",
                        zipcode="10016",
                        custom_token="ivB2PcFMwqF6X/PhrxfhyOTJSWkmPankGf7+FCdN2WXDXS5mhB16epK+6MG7lQZShTKXSGnlD4CnSM1//HP09zSx7d/TIMR1ddSd6fx6r84=",
                        opprl_token_1v1="x1Q4bxg7lXaiDAQhsFCrkVh9Z3uP9g7fzQZUfs8wsdLuN1qj53GDdWHUQCQUvoJt5XfdFbU2pbhw9frYVs3YaMhkE4QBqxOKjMMN8fkQPfQ=",
                    ),
                ]
            )
        ),
    )


def test_null_safe_tokenize(spark: SparkSession, private_key: bytes):
    actual = tokenize(
        (
            spark.createDataFrame(
                [
                    Row(
                        first_name=None,
                        last_name="PASTEUR",
                        gender="M",
                        birth_date=date(1822, 12, 27),
                    ),
                    Row(
                        first_name="LOUIS",
                        last_name=None,
                        gender="M",
                        birth_date=date(1822, 12, 27),
                    ),
                    Row(
                        first_name="LOUIS",
                        last_name="PASTEUR",
                        gender=None,
                        birth_date=date(1822, 12, 27),
                    ),
                    Row(
                        first_name="LOUIS",
                        last_name="PASTEUR",
                        gender="M",
                        birth_date=None,
                    ),
                ]
            )
        ),
        col_mapping={
            v0.first_name: "first_name",
            v0.last_name: "last_name",
            v0.gender: "gender",
            v0.birth_date: "birth_date",
            v1.first_name: "first_name",
            v1.last_name: "last_name",
            v1.gender: "gender",
            v1.birth_date: "birth_date",
        },
        tokens=[v0.token3, v1.token1, v1.token2, v1.token3],
        private_key=private_key,
    )

    expected = spark.createDataFrame(
        [
            Row(
                first_name=None,
                last_name="PASTEUR",
                gender="M",
                birth_date=date(1822, 12, 27),
                opprl_token_3v0=None,
                opprl_token_1v1=None,
                opprl_token_2v1=None,
                opprl_token_3v1=None,
            ),
            Row(
                first_name="LOUIS",
                last_name=None,
                gender="M",
                birth_date=date(1822, 12, 27),
                opprl_token_3v0=None,
                opprl_token_1v1=None,
                opprl_token_2v1=None,
                opprl_token_3v1=None,
            ),
            Row(
                first_name="LOUIS",
                last_name="PASTEUR",
                gender=None,
                birth_date=date(1822, 12, 27),
                opprl_token_3v0=None,
                opprl_token_1v1=None,
                opprl_token_2v1=None,
                opprl_token_3v1=None,
            ),
            Row(
                first_name="LOUIS",
                last_name="PASTEUR",
                gender="M",
                birth_date=None,
                opprl_token_3v0=None,
                opprl_token_1v1=None,
                opprl_token_2v1=None,
                opprl_token_3v1=None,
            ),
        ],
        StructType(
            [
                StructField("first_name", StringType()),
                StructField("last_name", StringType()),
                StructField("gender", StringType()),
                StructField("birth_date", DateType()),
                StructField("opprl_token_3v0", StringType()),
                StructField("opprl_token_1v1", StringType()),
                StructField("opprl_token_2v1", StringType()),
                StructField("opprl_token_3v1", StringType()),
            ]
        ),
    )

    assertDataFrameEqual(actual, expected)


def test_null_safe_transcode(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
    tokens = spark.createDataFrame(
        [Row(opprl_token_1v0=None, opprl_token_1v1=None)],
        StructType(
            [
                StructField("opprl_token_1v0", StringType()),
                StructField("opprl_token_1v1", StringType()),
            ]
        ),
    )
    ephemeral = transcode_out(tokens, (v0.token1, v1.token1), acme_public_key, private_key)
    assertDataFrameEqual(tokens, ephemeral)
    tokens2 = transcode_in(ephemeral, (v0.token1, v1.token1), acme_private_key)
    assertDataFrameEqual(ephemeral, tokens2)


def test_keys_from_env(
    spark: SparkSession,
    private_key: bytes,
    acme_public_key: bytes,
    acme_private_key: bytes,
    monkeypatch,
):
    monkeypatch.setenv(_PRIVATE_KEY_ENV_VAR, private_key.decode())
    monkeypatch.setenv(_RECIPIENT_PUBLIC_KEY_ENV_VAR, acme_public_key.decode())

    tokens = tokenize(
        (
            spark.createDataFrame(
                [
                    Row(
                        first_name="LOUIS",
                        last_name="PASTEUR",
                        gender="M",
                        birth_date=date(1822, 12, 27),
                    ),
                ]
            )
        ),
        col_mapping={
            v1.first_name: "first_name",
            v1.last_name: "last_name",
            v1.gender: "gender",
            v1.birth_date: "birth_date",
        },
        tokens=[v1.token1],
    )
    assertDataFrameEqual(
        tokens,
        spark.createDataFrame(
            [
                Row(
                    first_name="LOUIS",
                    last_name="PASTEUR",
                    gender="M",
                    birth_date=date(1822, 12, 27),
                    opprl_token_1v1="dmJAQHj05pZXDYudIx7W+aeZhZXFDULhNoxJGMl2+U5kr/1lFQF1uCaN9vbjBhsQRkImFBz3+F0kxm4lF3bNoes9B3idmq4pwGXD6ey1ncg=",
                ),
            ],
        ),
    )
    ephemeral_tokens = transcode_out(tokens, (v1.token1,))

    # Simulate the environment of the recipient.
    monkeypatch.setenv(_PRIVATE_KEY_ENV_VAR, acme_private_key.decode())
    acme_tokens = transcode_in(ephemeral_tokens, (v1.token1,))
    assertDataFrameEqual(
        acme_tokens,
        spark.createDataFrame(
            [
                Row(
                    first_name="LOUIS",
                    last_name="PASTEUR",
                    gender="M",
                    birth_date=date(1822, 12, 27),
                    opprl_token_1v1="qGPiZl9WXPsfIiHADpX3M5sR94fQWrsJBvE/jkvuGQ6Xq9OXPanO2urYEO+Jnzfn4b4RTqfT7QAVQufKYmQiHIf9zdzSXAYj70fsGcGEogw=",
                ),
            ],
        ),
    )


def test_missing_key(spark: SparkSession):
    with pytest.raises(ValueError, match="No private RSA key found"):
        tokenize(
            (
                spark.createDataFrame(
                    [
                        Row(
                            first_name="LOUIS",
                            last_name="PASTEUR",
                            gender="M",
                            birth_date=date(1822, 12, 27),
                        ),
                    ]
                )
            ),
            col_mapping={
                v1.first_name: "first_name",
                v1.last_name: "last_name",
                v1.gender: "gender",
                v1.birth_date: "birth_date",
            },
            tokens=[v1.token1],
        )
