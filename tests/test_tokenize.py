import pytest
from datetime import date
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from pyspark.sql.functions import regexp_replace, substring
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from spindle_token import tokenize, transcrypt_out, transcrypt_in
from spindle_token._crypto import _PRIVATE_KEY_ENV_VAR, _RECIPIENT_PUBLIC_KEY_ENV_VAR
from spindle_token.core import PiiAttribute, Token
from spindle_token.opprl.v0 import OpprlV0 as v0
from spindle_token.opprl.v1 import OpprlV1 as v1


def test_tokenize_and_transcrypt_opprl(
    spark: SparkSession, private_key: bytes, acme_public_key: bytes, acme_private_key: bytes
):
    all_tokens = [v0.token1, v0.token2, v0.token3, v1.token1, v1.token2, v1.token3]
    all_token_names = [token.name for token in all_tokens]

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
        tokens=all_tokens,
        private_key=private_key,
    )
    assertDataFrameEqual(
        tokens,
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        first_name="Louis",
                        last_name="Pasteur",
                        gender="male",
                        birth_date="1822-12-27",
                        opprl_token_1v0="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2v0="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3v0="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
                        opprl_token_2v1="tpddR895Id1+iBdWDWRgD+uncxwnAfLJaYkJfF5L5UW2tLeqIikffQ9MLLjSsCY8lqEf8O5iqCswqCuMgn3L0MIfnlSi2BQf0Gtf9ForU/E=",
                        opprl_token_3v1="H2Wj+vmO+IrwL2/y9JzQUWYAJizTuaQe2+lJSYLUafjqJL0IUqSBP7+Rs8u6+2sb0saBBqO098TfaQTcLORkfzJ44uhTutfqng7HyD4ikw8=",
                    ),
                    Row(
                        id=2,
                        first_name="louis",
                        last_name="pasteur",
                        gender="M",
                        birth_date="1822-12-27",
                        opprl_token_1v0="NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                        opprl_token_2v0="l6MNGG4InDZCnVX5/h3ajn1m1rCoko062CD8the2nkKO3Y0fARGZ5p4BP3pXPp3HOL603KCwpWLIXMN8fnsG3D4Tea/WOQX5kB1OQ28t2L0=",
                        opprl_token_3v0="op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                        opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
                        opprl_token_2v1="tpddR895Id1+iBdWDWRgD+uncxwnAfLJaYkJfF5L5UW2tLeqIikffQ9MLLjSsCY8lqEf8O5iqCswqCuMgn3L0MIfnlSi2BQf0Gtf9ForU/E=",
                        opprl_token_3v1="H2Wj+vmO+IrwL2/y9JzQUWYAJizTuaQe2+lJSYLUafjqJL0IUqSBP7+Rs8u6+2sb0saBBqO098TfaQTcLORkfzJ44uhTutfqng7HyD4ikw8=",
                    ),
                ]
            )
        ),
    )
    sent_tokens = transcrypt_out(
        tokens.select("id", *all_token_names),
        tokens=all_tokens,
        recipient_public_key=acme_public_key,
        private_key=private_key,
    )
    sent_tokens.show(truncate=False)

    assertSchemaEqual(
        sent_tokens.schema,
        StructType(
            [
                StructField("id", LongType()),
            ]
            + [StructField(token, StringType()) for token in all_token_names]
        ),
    )
    # When transferring between parties, tokens from the same PII should _not_ be equal.
    assert sent_tokens.distinct().count() == 2

    result = transcrypt_in(
        sent_tokens.select("id", *all_token_names),
        tokens=all_tokens,
        private_key=acme_private_key,
    )

    # Check that trancryption and tokenization produce the same result.
    assertDataFrameEqual(
        result,
        tokenize(
            pii,
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
            tokens=all_tokens,
            private_key=acme_private_key,
        ).select("id", *all_token_names),
    )

    # Test for token stability across changes.
    assertDataFrameEqual(
        result,
        (
            spark.createDataFrame(
                [
                    Row(
                        id=1,
                        opprl_token_1v0="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2v0="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3v0="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                        opprl_token_1v1="iVev4mqFAJhERbOyS1VCf37RXoyAFpbJDfapJOLpMaP5enFICVhHwaN6UlxhtBh8+nmYMJBCNFXgFEvOYUpohivogEBnM2AQHlmQKPDLG2Y=",
                        opprl_token_2v1="dE+z4cJ5Av0XdAJTYRYsCZH7XsBIQY/8b1TrpzmA6HjQbbJKBwwKV/dS0BRKbwSijCCcGcrmFDclP7qDbfn6sLxlkP70tdo1dOsKIfrwhMw=",
                        opprl_token_3v1="TK4/1PmoAR2Xu8jCxSAgMoEXmS5YsTbjSYiM6mTX+zFnXH9aP+JJOUZuEVEiM0nKdwVhRVUjbRi6FVRZjK7kCMjTP1SNIjmhICS4u+o8W0Y=",
                    ),
                    Row(
                        id=2,
                        opprl_token_1v0="U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                        opprl_token_2v0="GDV/IQ0x6ZR/Gtl+nFOMOoKtTJ6gOHTvVJoaZZhP0BHUsymHbw+pyF9Cbjr0Q/Apa07wvN93CBnr4aBi8vvCDxi0Qg8x8wJf+yZZpwFR3Dw=",
                        opprl_token_3v0="cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                        opprl_token_1v1="iVev4mqFAJhERbOyS1VCf37RXoyAFpbJDfapJOLpMaP5enFICVhHwaN6UlxhtBh8+nmYMJBCNFXgFEvOYUpohivogEBnM2AQHlmQKPDLG2Y=",
                        opprl_token_2v1="dE+z4cJ5Av0XdAJTYRYsCZH7XsBIQY/8b1TrpzmA6HjQbbJKBwwKV/dS0BRKbwSijCCcGcrmFDclP7qDbfn6sLxlkP70tdo1dOsKIfrwhMw=",
                        opprl_token_3v1="TK4/1PmoAR2Xu8jCxSAgMoEXmS5YsTbjSYiM6mTX+zFnXH9aP+JJOUZuEVEiM0nKdwVhRVUjbRi6FVRZjK7kCMjTP1SNIjmhICS4u+o8W0Y=",
                    ),
                ]
            )
        ),
    )


class _Zip3Attribute(PiiAttribute):
    def __init__(self, underlying: "ZipcodeAttribute"):
        super().__init__(f"{underlying.attr_id}.zip3")
        self.underlying = underlying

    def transform(self, column, dtype):
        return substring(self.underlying.transform(column, dtype), 1, 3)


class ZipcodeAttribute(PiiAttribute):
    def transform(self, column, dtype):
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
                Token("custom_token", v1.protocol, (v1.last_name, zipAttr.zip3)),
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
                        custom_token="xsNZvQ6g6pMhdMzAeZzu9AkA8LCilSFiezHOBTkRL965UI4DdO5y9FDY3oj/PjpSjKd1hxiYMN3vDUZNOWWtwrmjFwam8Hz0VnyKm7erUKM=",
                        opprl_token_1v1="8Mc0RV8ksiW05c4xQPx6+hbery1vVREIWk08kIdsYMvS+4JyvmYG0xKiWqTaRZwPiGepdIURR+4urBXG15R+YygnWp1QNGf2oX+nceImnLU=",
                    ),
                    Row(
                        id=2,
                        first_name="Pierre",
                        last_name="Curie",
                        gender="m",
                        birth_date="1859-05-15",
                        zipcode="none",
                        custom_token="xsNZvQ6g6pMhdMzAeZzu9AkA8LCilSFiezHOBTkRL965UI4DdO5y9FDY3oj/PjpSjKd1hxiYMN3vDUZNOWWtwrmjFwam8Hz0VnyKm7erUKM=",
                        opprl_token_1v1="w1rl8Rw+OehvgITp1fyG9QmYv7YFSR0MbpwHOPzyA/DCTiIbJGSnzB5gTOyYQzJ0GetCQU7/xlHRdULjlPpDA6G/SAZiantFxYDRlakqknQ=",
                    ),
                    Row(
                        id=3,
                        first_name="Jonas",
                        last_name="Salk",
                        gender="m",
                        birth_date="1914-10-28",
                        zipcode="10016",
                        custom_token="acydfnBp2n8gNMMKu0BQOOcapafSXEDsObEvqMHPo3t39Hahzoxm6GuMa68+/z0c1tQ5dHw+H1rBpBsfzVEwJXcwsmk8jQ7+5v28ST1FdDU=",
                        opprl_token_1v1="MoNcrkVgbNwW0Tf8Tw2ZooyKD+3TNZQU1nXTqww8aFvIvmuG+rgyjljuIcYcoIdKq79kMR0/4QA9Q6EiYTtgQrejvjadABxyS/0ZzivdMik=",
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


def test_null_safe_transcypt(
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
    ephemeral = transcrypt_out(tokens, (v0.token1, v1.token1), acme_public_key, private_key)
    assertDataFrameEqual(tokens, ephemeral)
    tokens2 = transcrypt_in(ephemeral, (v0.token1, v1.token1), acme_private_key)
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
                    opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
                ),
            ],
        ),
    )
    ephemeral_tokens = transcrypt_out(tokens, (v1.token1,))

    # Simulate the environment of the recipient.
    monkeypatch.setenv(_PRIVATE_KEY_ENV_VAR, acme_private_key.decode())
    acme_tokens = transcrypt_in(ephemeral_tokens, (v1.token1,))
    assertDataFrameEqual(
        acme_tokens,
        spark.createDataFrame(
            [
                Row(
                    first_name="LOUIS",
                    last_name="PASTEUR",
                    gender="M",
                    birth_date=date(1822, 12, 27),
                    opprl_token_1v1="iVev4mqFAJhERbOyS1VCf37RXoyAFpbJDfapJOLpMaP5enFICVhHwaN6UlxhtBh8+nmYMJBCNFXgFEvOYUpohivogEBnM2AQHlmQKPDLG2Y=",
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
