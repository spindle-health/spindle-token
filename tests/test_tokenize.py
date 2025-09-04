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
                        opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
                        opprl_token_2v1="tpddR895Id1+iBdWDWRgD+uncxwnAfLJaYkJfF5L5UW2tLeqIikffQ9MLLjSsCY8lqEf8O5iqCswqCuMgn3L0MIfnlSi2BQf0Gtf9ForU/E=",
                        opprl_token_3v1="H2Wj+vmO+IrwL2/y9JzQUWYAJizTuaQe2+lJSYLUafjqJL0IUqSBP7+Rs8u6+2sb0saBBqO098TfaQTcLORkfzJ44uhTutfqng7HyD4ikw8=",
                        opprl_token_4v1="FaTssGYTKBZMf2yUoWZAKRVm7QEKsFWzD/l8UiVOXryeQOLWtwikVxr5Ff4F3REe7YD6PW2w4gXqOqfqVJwjstSr3Oc4vMTEa5Pd1P1kzTM=",
                        opprl_token_5v1="BbmVhYL39W+bj5w6eSm05ykaEJtBrBtFKloL+LPxsMPlbnhwnS6/jyts2faKP+P2qipUNr2ws2JPAQwe6qySQoGoaiCRHRsI1wNWTkqf+YM=",
                        opprl_token_6v1="NUnS1Cc//mDglIQsD4Vs1TNyCEzqroio7LCvmB60RzMzICuVOpBb2aYRzKSUaAI48OKJXc/oXVcgiXXxFDhhvealbiSIsOrFLa6LGWVFNp8=",
                        opprl_token_7v1="jQSijbniu3pNHOf4frTtxfaP/aEcDfXSCKwRFbMvq6TtaUWq/8k/4uww/y6djtlzuuPW3Rv76hmI0x41X4uCCiviZ2UHXsXUsldKJSEx5ZY=",
                        opprl_token_8v1="/npShR3CAHHyKGpkoEjr2IWQpX1h80x+kc2ZAxzbzO+bIpbtQtGuyHCDgwi1YTz+LfCZJ/dvCrlhu1Tt3peGJfgVZhsKZr1OCMRJwD2BVG0=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="kghwIEFPriVFGXpafPfHdOGzfWfd+T19tRfoQqKXugxVQaqVEmWih9viD5mdWWXMeoSLfcaQwyJ0WIFme/VGcGFSt1QH5btJKsYPSA6iLgc=",
                        opprl_token_12v1="KYAbMneOypA67F1BIAmbZF0v4Aw7xRXqKvf3LCJ2nKHqRuoJWxkWnWfspHt8cF+pwNyLSOLfLddsfF58yyuz/s+G6xTXjVymbMw0ZtnCWRg=",
                        opprl_token_13v1="LMfG/ItICop/P4wYGS0pdljnLyuQN8KgmEuUcCjpB/VQ4K0bgVZF/6Epn7oEFI6I9cFil3wqRlJtcJI7BtrmQ+oGHZKfEshNad8LjICzIaQ=",
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
                        opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
                        opprl_token_2v1="tpddR895Id1+iBdWDWRgD+uncxwnAfLJaYkJfF5L5UW2tLeqIikffQ9MLLjSsCY8lqEf8O5iqCswqCuMgn3L0MIfnlSi2BQf0Gtf9ForU/E=",
                        opprl_token_3v1="H2Wj+vmO+IrwL2/y9JzQUWYAJizTuaQe2+lJSYLUafjqJL0IUqSBP7+Rs8u6+2sb0saBBqO098TfaQTcLORkfzJ44uhTutfqng7HyD4ikw8=",
                        opprl_token_4v1="FaTssGYTKBZMf2yUoWZAKRVm7QEKsFWzD/l8UiVOXryeQOLWtwikVxr5Ff4F3REe7YD6PW2w4gXqOqfqVJwjstSr3Oc4vMTEa5Pd1P1kzTM=",
                        opprl_token_5v1="BbmVhYL39W+bj5w6eSm05ykaEJtBrBtFKloL+LPxsMPlbnhwnS6/jyts2faKP+P2qipUNr2ws2JPAQwe6qySQoGoaiCRHRsI1wNWTkqf+YM=",
                        opprl_token_6v1="NUnS1Cc//mDglIQsD4Vs1TNyCEzqroio7LCvmB60RzMzICuVOpBb2aYRzKSUaAI48OKJXc/oXVcgiXXxFDhhvealbiSIsOrFLa6LGWVFNp8=",
                        opprl_token_7v1="jQSijbniu3pNHOf4frTtxfaP/aEcDfXSCKwRFbMvq6TtaUWq/8k/4uww/y6djtlzuuPW3Rv76hmI0x41X4uCCiviZ2UHXsXUsldKJSEx5ZY=",
                        opprl_token_8v1="/npShR3CAHHyKGpkoEjr2IWQpX1h80x+kc2ZAxzbzO+bIpbtQtGuyHCDgwi1YTz+LfCZJ/dvCrlhu1Tt3peGJfgVZhsKZr1OCMRJwD2BVG0=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="kghwIEFPriVFGXpafPfHdOGzfWfd+T19tRfoQqKXugxVQaqVEmWih9viD5mdWWXMeoSLfcaQwyJ0WIFme/VGcGFSt1QH5btJKsYPSA6iLgc=",
                        opprl_token_12v1="KYAbMneOypA67F1BIAmbZF0v4Aw7xRXqKvf3LCJ2nKHqRuoJWxkWnWfspHt8cF+pwNyLSOLfLddsfF58yyuz/s+G6xTXjVymbMw0ZtnCWRg=",
                        opprl_token_13v1="LMfG/ItICop/P4wYGS0pdljnLyuQN8KgmEuUcCjpB/VQ4K0bgVZF/6Epn7oEFI6I9cFil3wqRlJtcJI7BtrmQ+oGHZKfEshNad8LjICzIaQ=",
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
                        opprl_token_1v1="iVev4mqFAJhERbOyS1VCf37RXoyAFpbJDfapJOLpMaP5enFICVhHwaN6UlxhtBh8+nmYMJBCNFXgFEvOYUpohivogEBnM2AQHlmQKPDLG2Y=",
                        opprl_token_2v1="dE+z4cJ5Av0XdAJTYRYsCZH7XsBIQY/8b1TrpzmA6HjQbbJKBwwKV/dS0BRKbwSijCCcGcrmFDclP7qDbfn6sLxlkP70tdo1dOsKIfrwhMw=",
                        opprl_token_3v1="TK4/1PmoAR2Xu8jCxSAgMoEXmS5YsTbjSYiM6mTX+zFnXH9aP+JJOUZuEVEiM0nKdwVhRVUjbRi6FVRZjK7kCMjTP1SNIjmhICS4u+o8W0Y=",
                        opprl_token_4v1="wGwtW6zPRiM5ySg9SDruvsUh1trvMPsPHeh9mxhDoFslduxS2MCUWgOwmzx1aO4wtzoM3Oa4yobmWRr0/1DjXsVWzoNY1y3NefTWm/4/hh0=",
                        opprl_token_5v1="AZ2fSNNo37AnL2tI0N6eK6tWXHZ0dn2rYJMrfGzbNZdqjLUPNKieCth/AfvRCwGGx6P1GS6f+iTQvmoc+wZg3XRI32v+rMPvgOAr6EAFvzo=",
                        opprl_token_6v1="X8rEpi1xpHi7ItQpTuSQqssPKCHN0xGyj78AjNewHuJT1M67IicOG92ojcxN34lX/+jlI0nqXgRAgVmKO9Tfe8Vvo8EVsYbfkLCPai0IY78=",
                        opprl_token_7v1="TxsgzlFWe9loSlFmsh92HCtu5CRUd+6TD6dL6woFbwuCcrm10spPQhAhFEyM2XAQNIIIbA4VyBJIpNvMrTbRGEgGK7SDgpAGcjxCagcWElA=",
                        opprl_token_8v1="9VJvJ4IF1wDPP4ySGgrkfbVYDnoHkQLGmIMWowZ7+XocuTc07SO+hZkFROHbZsWSXTIkQ7VS8fwk56OGgdHxIxI5SQpVAOCVaFywhLx8M1E=",
                        opprl_token_9v1=None,
                        opprl_token_10v1=None,
                        opprl_token_11v1="OvQjwdmG2zuN+yru8dWZ3yL1W0Uv6qMF7HSbuPkCiWORmsvSoRKzTQV2eBlj5Ds+VnZlzgfykbuo4H1jh7a53UmdnK1e/tLi775vqThKRy8=",
                        opprl_token_12v1="5RfmceqAl0/cPOGt/gRlM/9VCnhJsVnLJw0VpNxtgiUQd3Q0gsLvNZGomyldg2QQzcNzMp+cVJ+qy0KM+B36tzALOh/M+nKJDZoPugUn1ls=",
                        opprl_token_13v1="fArMtZhWOrpOeTSmAjX7heNXhr6OtwI2rBpa+ERNEWTYotD3VR8Pmiws95pvWCPn24Sc+vjz2IHF80DeXi4C6DRMQxqnhpedUIl5KgfEo5A=",
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
                    opprl_token_1v1="MZYNcbue6knwp5xhJHWmYvuIxR2Lza0OhYIutY+gS/cMBYm3DAJvah0kmeCkohs6Gs2s61KiBOJ+/yFiFSYCRYZJxz2/rqg3Q/PclfyDxxQ=",
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
