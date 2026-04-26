from __future__ import annotations

import os
import time
from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spindle_token import tokenize, transcode_in, transcode_out
from spindle_token.core import Token
from spindle_token.opprl import OpprlV2 as v2


pytestmark = pytest.mark.performance

ROW_COUNT = 1_000
TOKENS = (v2.token1, v2.token2, v2.token3)


def _synthetic_pii(spark: SparkSession, rows: int) -> DataFrame:
    return spark.range(rows).select(
        F.concat(F.lit("First"), F.col("id").cast("string")).alias("first_name"),
        F.concat(F.lit("Last"), (F.col("id") % F.lit(100)).cast("string")).alias(
            "last_name"
        ),
        F.when(F.col("id") % F.lit(2) == F.lit(0), F.lit("M"))
        .otherwise(F.lit("F"))
        .alias("gender"),
        F.lit(date(1980, 1, 1)).alias("birth_date"),
    )


def _force_token_columns(df: DataFrame, tokens: tuple[Token, ...]) -> None:
    expressions = [
        F.sum(F.length(F.col(token.name))).alias(f"{token.name}_length")
        for token in tokens
    ]
    df.agg(*expressions).collect()


def _time(label: str, operation) -> float:
    start = time.perf_counter()
    operation()
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.2f}s")
    return elapsed


@pytest.mark.skipif(
    os.environ.get("SPINDLE_PERF_TESTS") != "1",
    reason="set SPINDLE_PERF_TESTS=1 to run opt-in performance tests",
)
def test_crypto_performance_1000_rows(
    spark: SparkSession,
    private_key: bytes,
    acme_public_key: bytes,
    acme_private_key: bytes,
) -> None:
    pii = _synthetic_pii(spark, ROW_COUNT)
    col_mapping = {
        v2.first_name: "first_name",
        v2.last_name: "last_name",
        v2.gender: "gender",
        v2.birth_date: "birth_date",
    }

    tokenized = tokenize(pii, col_mapping, TOKENS, private_key=private_key).select(
        *(token.name for token in TOKENS)
    )
    tokenized_cached = tokenized.cache()
    ephemeral_cached = None

    try:
        tokenize_seconds = _time(
            f"tokenize rows={ROW_COUNT}",
            lambda: _force_token_columns(tokenized_cached, TOKENS),
        )

        ephemeral = transcode_out(
            tokenized_cached,
            TOKENS,
            recipient_public_key=acme_public_key,
            private_key=private_key,
        ).select(*(token.name for token in TOKENS))
        ephemeral_cached = ephemeral.cache()

        transcode_out_seconds = _time(
            f"transcode_out rows={ROW_COUNT}",
            lambda: _force_token_columns(ephemeral_cached, TOKENS),
        )

        restored = transcode_in(
            ephemeral_cached,
            TOKENS,
            private_key=acme_private_key,
        ).select(*(token.name for token in TOKENS))

        transcode_in_seconds = _time(
            f"transcode_in rows={ROW_COUNT}",
            lambda: _force_token_columns(restored, TOKENS),
        )

        print(
            "crypto performance summary: "
            f"tokenize={tokenize_seconds:.2f}s, "
            f"transcode_out={transcode_out_seconds:.2f}s, "
            f"transcode_in={transcode_in_seconds:.2f}s"
        )
    finally:
        tokenized_cached.unpersist()
        if ephemeral_cached is not None:
            ephemeral_cached.unpersist()
