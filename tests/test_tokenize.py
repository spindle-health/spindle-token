import pytest
from datetime import date
from pyspark.sql import Column, SparkSession, Row
from pyspark.sql.types import DataType, StructType, StructField, LongType, StringType, DateType
from pyspark.sql.functions import regexp_replace, substring
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from carduus.token import (
    OpprlPii,
    OpprlToken,
    TokenSpec,
    tokenize,
    transcrypt_out,
    transcrypt_in,
)
from carduus.token.pii import PiiTransform
from carduus.keys import _PRIVATE_KEY_ENV_VAR, _RECIPIENT_PUBLIC_KEY_ENV_VAR


def test_tokenize_and_transcrypt_opprl(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
    pii = spark.createDataFrame(
        [
            Row(
                id=1,
                first_name="Louis",
                last_name="Pasteur",
                gender="male",
                birth_date="1822-12-27",
            ),
            Row(
                id=2,
                first_name="louis",
                last_name="pasteur",
                gender="M",
                birth_date="1822-12-27",
            ),
        ]
    )
    tokens = tokenize(
        pii,
        pii_transforms={
            "first_name": OpprlPii.first_name,
            "last_name": OpprlPii.last_name,
            "gender": OpprlPii.gender,
            "birth_date": OpprlPii.birth_date,
        },
        tokens=[OpprlToken.token1, OpprlToken.token2, OpprlToken.token3],
        private_key=private_key,
    )
    assertDataFrameEqual(
        tokens,
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        first_name="LOUIS",
                        last_name="PASTEUR",
                        gender="M",
                        birth_date=date(1822, 12, 27),
                        opprl_token_1="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                    ),
                    Row(
                        id=2,
                        first_name="LOUIS",
                        last_name="PASTEUR",
                        gender="M",
                        birth_date=date(1822, 12, 27),
                        opprl_token_1="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                    ),
                ]
            )
        ),
    )
    sent_tokens = transcrypt_out(
        tokens.select("id", "opprl_token_1", "opprl_token_2", "opprl_token_3"),
        token_columns=["opprl_token_1", "opprl_token_2", "opprl_token_3"],
        recipient_public_key=acme_public_key,
        private_key=private_key,
    )
    assertSchemaEqual(
        sent_tokens.schema,
        StructType(
            [
                StructField("id", LongType()),
                StructField("opprl_token_1", StringType()),
                StructField("opprl_token_2", StringType()),
                StructField("opprl_token_3", StringType()),
            ]
        ),
    )
    # When transferring between parties, tokens from the same PII should _not_ be equal.
    assert sent_tokens.distinct().count() == 2

    result = transcrypt_in(
        sent_tokens.select("id", "opprl_token_1", "opprl_token_2", "opprl_token_3"),
        token_columns=["opprl_token_1", "opprl_token_2", "opprl_token_3"],
        private_key=acme_private_key,
    )

    # Check that trancryption and tokenization produce the same result.
    assertDataFrameEqual(
        result,
        tokenize(
            pii,
            pii_transforms={
                "first_name": OpprlPii.first_name,
                "last_name": OpprlPii.last_name,
                "gender": OpprlPii.gender,
                "birth_date": OpprlPii.birth_date,
            },
            tokens=[OpprlToken.token1, OpprlToken.token2, OpprlToken.token3],
            private_key=acme_private_key,
        ).select("id", "opprl_token_1", "opprl_token_2", "opprl_token_3"),
    )

    # Test for token stability across changes.
    assertDataFrameEqual(
        result,
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        opprl_token_1="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                    ),
                    Row(
                        id=2,
                        opprl_token_1="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                    ),
                ]
            )
        ),
    )


class ZipcodeTransform(PiiTransform):
    def normalize(self, column: Column, dtype: DataType) -> Column:
        return regexp_replace(column, "[^0-9]", "")

    def enhancements(self, column: Column) -> dict[str, Column]:
        return {"zip3": substring(column, 1, 3)}


def test_custom_pii_and_token(spark: SparkSession, private_key: bytes):
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
            pii_transforms={
                "first_name": OpprlPii.first_name,
                "last_name": OpprlPii.last_name,
                "gender": OpprlPii.gender,
                "birth_date": OpprlPii.birth_date,
                "zipcode": ZipcodeTransform(),
            },
            tokens=[
                TokenSpec("custom_token", ("last_name", "zip3")),
                OpprlToken.token1,
            ],
            private_key=private_key,
        ),
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        first_name="MARIE",
                        last_name="CURIE",
                        gender="F",
                        birth_date=date(1867, 11, 7),
                        zipcode="",
                        custom_token="a4l0zaphD0cOzrrRAsQR4++7c91z+wrhZURjlUQszMHS2H82q5dUzYNmsPaVTHRDVtojQKkvKcj0ziGvRBdKxoqp6b3KgkxAoN9EzbPGQJQ=",
                        opprl_token_1="tqwyRFi77r48A2FRj6O3KmHm9btLa1dxJYn52DpdEy3OQ0j7iuvjwYgems1SFmfOqHJ5KnK7UxzMCi2TTaJWwbnho6J7TVvPhgkNU9U0ot4=",
                    ),
                    Row(
                        id=2,
                        first_name="PIERRE",
                        last_name="CURIE",
                        gender="M",
                        birth_date=date(1859, 5, 15),
                        zipcode="",
                        custom_token="a4l0zaphD0cOzrrRAsQR4++7c91z+wrhZURjlUQszMHS2H82q5dUzYNmsPaVTHRDVtojQKkvKcj0ziGvRBdKxoqp6b3KgkxAoN9EzbPGQJQ=",
                        opprl_token_1="Ui78f0vu3cD01mdnP+1E1yt2Qn6AZu0oA1G2YbRWUBAnTvl6SO+s3cJsHlRkL40LR4IMSb+maEDa5J4ZgNxFD7agtt9wOE8NurHCIrmiRs8=",
                    ),
                    Row(
                        id=3,
                        first_name="JONAS",
                        last_name="SALK",
                        gender="M",
                        birth_date=date(1914, 10, 28),
                        zipcode="10016",
                        custom_token="mbWIfUp4H/1QVWKF+aNuHfJfpUgnJrifZndPdVquuYcRiKJjG21jQ/71pAnlvDNjTNq3k0mxlKhWNaypvBc0ghNfmvS1mPfag6sr12dBu1I=",
                        opprl_token_1="t+Yg6k4aOm5xMOMjT1nUCVbw1xM6mITKRx/APB+oU0dNo/AN2q/p20Pu2fiKd4wX5iFVK119DJAHYkFJYuI1BxgLBrzkiQKdEJKn1kMzA6k=",
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
        pii_transforms={
            "first_name": OpprlPii.first_name,
            "last_name": OpprlPii.last_name,
            "gender": OpprlPii.gender,
            "birth_date": OpprlPii.birth_date,
        },
        tokens=[OpprlToken.token1, OpprlToken.token2, OpprlToken.token3],
        private_key=private_key,
    )

    expected = spark.createDataFrame(
        [
            Row(
                first_name=None,
                last_name="PASTEUR",
                gender="M",
                birth_date=date(1822, 12, 27),
                opprl_token_1=None,
                opprl_token_2=None,
                opprl_token_3=None,
            ),
            Row(
                first_name="LOUIS",
                last_name=None,
                gender="M",
                birth_date=date(1822, 12, 27),
                opprl_token_1=None,
                opprl_token_2=None,
                opprl_token_3=None,
            ),
            Row(
                first_name="LOUIS",
                last_name="PASTEUR",
                gender=None,
                birth_date=date(1822, 12, 27),
                opprl_token_1=None,
                opprl_token_2=None,
                opprl_token_3=None,
            ),
            Row(
                first_name="LOUIS",
                last_name="PASTEUR",
                gender="M",
                birth_date=None,
                opprl_token_1=None,
                opprl_token_2=None,
                opprl_token_3=None,
            ),
        ],
        StructType(
            [
                StructField("first_name", StringType()),
                StructField("last_name", StringType()),
                StructField("gender", StringType()),
                StructField("birth_date", DateType()),
                StructField("opprl_token_1", StringType()),
                StructField("opprl_token_2", StringType()),
                StructField("opprl_token_3", StringType()),
            ]
        ),
    )

    assertDataFrameEqual(actual, expected)


def test_null_safe_transcypt(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
    tokens = spark.createDataFrame(
        [Row(opprl_token_1=None)], StructType([StructField("opprl_token_1", StringType())])
    )
    ephemeral = transcrypt_out(tokens, ["opprl_token_1"], acme_public_key, private_key)
    assertDataFrameEqual(tokens, ephemeral)
    tokens2 = transcrypt_in(ephemeral, ["opprl_token_1"], acme_private_key)
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
        pii_transforms={
            "first_name": OpprlPii.first_name,
            "last_name": OpprlPii.last_name,
            "gender": OpprlPii.gender,
            "birth_date": OpprlPii.birth_date,
        },
        tokens=[OpprlToken.token1],
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
                    opprl_token_1="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                ),
            ],
        ),
    )
    ephemeral_tokens = transcrypt_out(tokens, ("opprl_token_1",))

    # Simulate the environment of the recipient.
    monkeypatch.setenv(_PRIVATE_KEY_ENV_VAR, acme_private_key.decode())
    acme_tokens = transcrypt_in(ephemeral_tokens, ("opprl_token_1",))
    assertDataFrameEqual(
        acme_tokens,
        spark.createDataFrame(
            [
                Row(
                    first_name="LOUIS",
                    last_name="PASTEUR",
                    gender="M",
                    birth_date=date(1822, 12, 27),
                    opprl_token_1="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
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
            pii_transforms={
                "first_name": OpprlPii.first_name,
                "last_name": OpprlPii.last_name,
                "gender": OpprlPii.gender,
                "birth_date": OpprlPii.birth_date,
            },
            tokens=[OpprlToken.token1],
        )
