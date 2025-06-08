from datetime import date
from pyspark.sql import Column, SparkSession, Row
from pyspark.sql.types import DataType, StructType, StructField, StringType, DateType
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


TOKEN_COLUMNS = [f"opprl_token_{i}" for i in range(1, 11)]
TOKEN_FIELDS = [StructField(column, StringType()) for column in TOKEN_COLUMNS]


def test_tokenize_and_transcrypt_opprl(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
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
    tokens = tokenize(
        pii,
        pii_transforms={
            "first_name": OpprlPii.first_name,
            "last_name": OpprlPii.last_name,
            "gender": OpprlPii.gender,
            "birth_date": OpprlPii.birth_date,
            "email": OpprlPii.email,
            "phone": OpprlPii.phone,
            "ssn": OpprlPii.ssn,
            "group_number": OpprlPii.group_number,
            "member_id": OpprlPii.member_id,
        },
        tokens=[
            OpprlToken.token1,
            OpprlToken.token2,
            OpprlToken.token3,
            OpprlToken.token4,
            OpprlToken.token5,
            OpprlToken.token6,
            OpprlToken.token7,
            OpprlToken.token8,
            OpprlToken.token9,
            OpprlToken.token10,
        ],
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
                        opprl_token_1="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_4="ej4NQniPHC01KxLfWljyC867K8doi4Cx2o7GLFxXab+fNvAajNkbDZi4Uvr4tlM9qYHwuIGL/p5SfDKYjostasekQ34+bEysw3Jvzsyx4ec=",
                        opprl_token_5="WoXmm7EIT/K/rEpwjY0DikncHpxA6UmZD5BlVXfujGv7HXnviM8hp+u2AylmypSjDiDIvcnUXLAn1s9p5wt44VawV7jZLOihENbqyq1IplY=",
                        opprl_token_6="aq5uQ8Eo298+oQkaD8RacrTJiipb1f+nui0uAEbQBqNvo54XZEwq1AYfun8rGp0H3Tkb0FnMRckDV5BxbXxk3BgR6VOkcItT/dHz1S3HGjg=",
                        opprl_token_7="qShJpUmXNNHNG5SWby8WKXUSOe0Xp6C2byZFzqnqxKTkgBAN3CpmK8lB1YeGzr2SJMqYhn2YWAg/HGsyj000AaTcuWyLojGXuORYnTfjHR0=",
                        opprl_token_8="QZZcsCcbPw5i0S4M37fuSSFPHAvz+BLEd5ctSM8w8PuT/y8wRP1anRvy8chZo21yANtM4xWgufBa0X/+2fxs0J/72kEDaF1fes/2utvAnio=",
                        opprl_token_9=None,
                        opprl_token_10="mkx2HtY5napyeme50PJaq/59LW5Pp+2UxE7LqjeF3JflPQPoifXME7LmIxxoliSBrH2A69FvWTKVq5wU54smIk3n32zh7h4mXX1bMqTX0dc=",
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
                        opprl_token_1="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_4="ej4NQniPHC01KxLfWljyC867K8doi4Cx2o7GLFxXab+fNvAajNkbDZi4Uvr4tlM9qYHwuIGL/p5SfDKYjostasekQ34+bEysw3Jvzsyx4ec=",
                        opprl_token_5="WoXmm7EIT/K/rEpwjY0DikncHpxA6UmZD5BlVXfujGv7HXnviM8hp+u2AylmypSjDiDIvcnUXLAn1s9p5wt44VawV7jZLOihENbqyq1IplY=",
                        opprl_token_6="aq5uQ8Eo298+oQkaD8RacrTJiipb1f+nui0uAEbQBqNvo54XZEwq1AYfun8rGp0H3Tkb0FnMRckDV5BxbXxk3BgR6VOkcItT/dHz1S3HGjg=",
                        opprl_token_7="qShJpUmXNNHNG5SWby8WKXUSOe0Xp6C2byZFzqnqxKTkgBAN3CpmK8lB1YeGzr2SJMqYhn2YWAg/HGsyj000AaTcuWyLojGXuORYnTfjHR0=",
                        opprl_token_8="QZZcsCcbPw5i0S4M37fuSSFPHAvz+BLEd5ctSM8w8PuT/y8wRP1anRvy8chZo21yANtM4xWgufBa0X/+2fxs0J/72kEDaF1fes/2utvAnio=",
                        opprl_token_9=None,
                        opprl_token_10="mkx2HtY5napyeme50PJaq/59LW5Pp+2UxE7LqjeF3JflPQPoifXME7LmIxxoliSBrH2A69FvWTKVq5wU54smIk3n32zh7h4mXX1bMqTX0dc=",
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
                    + TOKEN_FIELDS
                ),
            )
        ),
    )
    sent_tokens = transcrypt_out(
        tokens.select(*TOKEN_COLUMNS),
        token_columns=TOKEN_COLUMNS,
        recipient_public_key=acme_public_key,
        private_key=private_key,
    )
    assertSchemaEqual(
        sent_tokens.schema,
        StructType(TOKEN_FIELDS),
    )
    # When transferring between parties, tokens from the same PII should _not_ be equal.
    assert sent_tokens.distinct().count() == 2

    result = transcrypt_in(
        sent_tokens,
        token_columns=TOKEN_COLUMNS,
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
                "email": OpprlPii.email,
                "phone": OpprlPii.phone,
                "ssn": OpprlPii.ssn,
                "group_number": OpprlPii.group_number,
                "member_id": OpprlPii.member_id,
            },
            tokens=[
                OpprlToken.token1,
                OpprlToken.token2,
                OpprlToken.token3,
                OpprlToken.token4,
                OpprlToken.token5,
                OpprlToken.token6,
                OpprlToken.token7,
                OpprlToken.token8,
                OpprlToken.token9,
                OpprlToken.token10,
            ],
            private_key=acme_private_key,
        ).select(*TOKEN_COLUMNS),
    )

    # Test for token stability across changes.
    assertDataFrameEqual(
        result,
        (
            spark.createDataFrame(
                [
                    Row(
                        opprl_token_1="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                        opprl_token_4="Ds3U0KWvkJAQlqbILF/MEqq7B2ojrNX75cLzYcRTmXiX2XcKpPhNeSrCJezMEOw7zfdzB1+qALkQXcAx/YISO/Q9NNiDsMMl1GdDU4/HX7U=",
                        opprl_token_5="4AVmWZ99Hj+QaI6a6tu8W90NxEyY+knyM1C6GP2PS2MjIA5rCGXQ3t6flHimM1GfRgD+pFn1mBhOe8+MaZUFXgRaHmMdWwGrlokb1J3+zIw=",
                        opprl_token_6="GeLRwPTPN9fFmCtXZDEHSM6lTt3bu65JlUF85pTLe6tvqXmPXQTS0Ve/QBPC2c8OUsTKdB7WxMINxH2+JY8hOkJaGqtsgDe8FaSuhybehms=",
                        opprl_token_7="1ndyQ8YB1r33Z4J030MiaJFfwCvJN6TWxyyKM2vxjMITITf7F6hJ0kkLpoOTI7/eJC4GLTJ/H3MLSpAmNVc14b2zU168w3y93RgI+TGUuoQ=",
                        opprl_token_8="5IvSOBTv7qBK+Wn3adVqIVI8c1nJ6xUw3f03uq5HLELHJJJLBRqbxyWXh2hVMW5scmEHekSdlrni1YZc4nzybwQboz2k283tSC5J2TCY+vs=",
                        opprl_token_9=None,
                        opprl_token_10="dkOfFGx41B9nfO6z1i7WZFxZXRW2En6Nxyfsam1e5TgAgAUa46p/kGvYJSX0y6IfpzQuAgxYwzYeTLcpHmfQjkzCtRvc7B6LmNT1CIBUBak=",
                    ),
                ]
                * 2,
                schema=StructType(TOKEN_FIELDS),
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
                            first_name="Marie",
                            last_name="Curie",
                            gender="F",
                            birth_date=date(1867, 11, 7),
                            zipcode="none",
                        ),
                        Row(
                            id=2,
                            first_name="Pierre",
                            last_name="Curie",
                            gender="M",
                            birth_date=date(1859, 5, 15),
                            zipcode="none",
                        ),
                        Row(
                            id=3,
                            first_name="Jonas",
                            last_name="Salk",
                            gender="M",
                            birth_date=date(1914, 10, 28),
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
                        first_name="Marie",
                        last_name="Curie",
                        gender="F",
                        birth_date=date(1867, 11, 7),
                        zipcode="none",
                        custom_token="a4l0zaphD0cOzrrRAsQR4++7c91z+wrhZURjlUQszMHS2H82q5dUzYNmsPaVTHRDVtojQKkvKcj0ziGvRBdKxoqp6b3KgkxAoN9EzbPGQJQ=",
                        opprl_token_1="tqwyRFi77r48A2FRj6O3KmHm9btLa1dxJYn52DpdEy3OQ0j7iuvjwYgems1SFmfOqHJ5KnK7UxzMCi2TTaJWwbnho6J7TVvPhgkNU9U0ot4=",
                    ),
                    Row(
                        id=2,
                        first_name="Pierre",
                        last_name="Curie",
                        gender="M",
                        birth_date=date(1859, 5, 15),
                        zipcode="none",
                        custom_token="a4l0zaphD0cOzrrRAsQR4++7c91z+wrhZURjlUQszMHS2H82q5dUzYNmsPaVTHRDVtojQKkvKcj0ziGvRBdKxoqp6b3KgkxAoN9EzbPGQJQ=",
                        opprl_token_1="Ui78f0vu3cD01mdnP+1E1yt2Qn6AZu0oA1G2YbRWUBAnTvl6SO+s3cJsHlRkL40LR4IMSb+maEDa5J4ZgNxFD7agtt9wOE8NurHCIrmiRs8=",
                    ),
                    Row(
                        id=3,
                        first_name="Jonas",
                        last_name="Salk",
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
