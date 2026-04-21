from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from io import BytesIO
from pathlib import Path
from typing import Any

import click

from spindle_token.core import PiiAttribute, Token
from spindle_token._crypto import _PRIVATE_KEY_ENV_VAR, _RECIPIENT_PUBLIC_KEY_ENV_VAR


def _spark_error() -> ImportError:
    return ImportError(
        "spindle-token CLI functionality requires the optional 'spark' extra. "
        "Install with `pip install spindle-token[spark]`."
    )


class _LazyTokenSpecs(Mapping[str, Token]):
    def __init__(self) -> None:
        self._data: dict[str, Token] | None = None

    def _load(self) -> dict[str, Token]:
        if self._data is None:
            try:
                from spindle_token.opprl.v0 import OpprlV0 as v0
                from spindle_token.opprl.v1 import OpprlV1 as v1
                from spindle_token.opprl.v2 import OpprlV2 as v2
            except ModuleNotFoundError as exc:
                if exc.name == "pyspark" or (
                    exc.name is not None and exc.name.startswith("pyspark.")
                ) or "No module named 'pyspark'" in str(exc):
                    raise _spark_error() from exc
                raise

            self._data = {
                v0.token1.name: v0.token1,
                v0.token2.name: v0.token2,
                v0.token3.name: v0.token3,
                v1.token1.name: v1.token1,
                v1.token2.name: v1.token2,
                v1.token3.name: v1.token3,
                v1.token4.name: v1.token4,
                v1.token5.name: v1.token5,
                v1.token6.name: v1.token6,
                v1.token7.name: v1.token7,
                v1.token8.name: v1.token8,
                v1.token9.name: v1.token9,
                v1.token10.name: v1.token10,
                v1.token11.name: v1.token11,
                v1.token12.name: v1.token12,
                v1.token13.name: v1.token13,
                v2.token1.name: v2.token1,
                v2.token2.name: v2.token2,
                v2.token3.name: v2.token3,
                v2.token4.name: v2.token4,
                v2.token5.name: v2.token5,
                v2.token6.name: v2.token6,
                v2.token7.name: v2.token7,
                v2.token8.name: v2.token8,
                v2.token9.name: v2.token9,
                v2.token10.name: v2.token10,
                v2.token11.name: v2.token11,
                v2.token12.name: v2.token12,
                v2.token13.name: v2.token13,
            }
        return self._data

    def __getitem__(self, key: str) -> Token:
        return self._load()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._load())

    def __len__(self) -> int:
        return len(self._load())


TOKEN_SPECS: Mapping[str, Token] = _LazyTokenSpecs()


class _LazyColumnAttributes(Mapping[str, list[PiiAttribute]]):
    def __init__(self) -> None:
        self._data: dict[str, list[PiiAttribute]] | None = None

    def _load(self) -> dict[str, list[PiiAttribute]]:
        if self._data is None:
            try:
                from spindle_token.opprl.v0 import OpprlV0 as v0
                from spindle_token.opprl.v1 import OpprlV1 as v1
                from spindle_token.opprl.v2 import OpprlV2 as v2
            except ModuleNotFoundError as exc:
                if exc.name == "pyspark" or (
                    exc.name is not None and exc.name.startswith("pyspark.")
                ) or "No module named 'pyspark'" in str(exc):
                    raise _spark_error() from exc
                raise

            self._data = {
                "first_name": [v0.first_name, v1.first_name, v2.first_name],
                "last_name": [v0.last_name, v1.last_name, v2.last_name],
                "gender": [v0.gender, v1.gender, v2.gender],
                "birth_date": [v0.birth_date, v1.birth_date, v2.birth_date],
                "email": [v1.email, v2.email],
                "hem": [v1.hem, v2.hem],
                "phone": [v1.phone, v2.phone],
                "ssn": [v1.ssn, v2.ssn],
                "group_number": [v1.group_number, v2.group_number],
                "member_id": [v1.member_id, v2.member_id],
            }
        return self._data

    def __getitem__(self, key: str) -> list[PiiAttribute]:
        return self._load()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._load())

    def __len__(self) -> int:
        return len(self._load())


_COLUMN_TO_ATTRIBUTES: Mapping[str, list[PiiAttribute]] = _LazyColumnAttributes()


def infer_column_mapping(columns: list[str]) -> dict[PiiAttribute, str]:
    col_mapping = {}
    for column in columns:
        for attribute in _COLUMN_TO_ATTRIBUTES.get(column, []):
            col_mapping[attribute] = column
    return col_mapping


class _TokenChoice(click.ParamType):
    name = "token"

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None):
        value = str(value)
        if value not in TOKEN_SPECS:
            self.fail(f"invalid choice: {value!r}", param, ctx)
        return value


TOKEN_CHOICE = _TokenChoice()


def get_spark(num_threads: int | None):
    """Gets active spark session or creates a new local spark session."""
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark" or (
            exc.name is not None and exc.name.startswith("pyspark.")
        ) or "No module named 'pyspark'" in str(exc):
            raise _spark_error() from exc
        raise

    p = str(num_threads) if num_threads else "*"
    return SparkSession.Builder().master(f"local[{p}]").appName("Spindle Token CLI").getOrCreate()


def write_single_file(dfw, path: str):
    """Writes a 1 partition PySpark Dataframe to a single file."""
    tmp_dir = tempfile.mkdtemp(prefix="spindle-token-")
    try:
        tmp_path = Path(tmp_dir) / "data"
        dfw.save(str(tmp_path))
        part_file = next(tmp_path.rglob("part-*"), None)
        if part_file is None:
            raise FileNotFoundError("No data files found in the temporary directory.")
        shutil.copy(part_file, path)
    finally:
        shutil.rmtree(tmp_dir)


##################################################################
# CLI


def common_options(func):
    """A decorator that adds the click options that are found on all commands."""
    func = click.option(
        "-p",
        "--parallelism",
        envvar="SPINDLE_TOKEN_PARALLELISM",
        type=click.INT,
        help="The number of worker threads to parallelize over. Useful when the input dataset is "
        "partitioned into multiple part files. If not supplied, defaults to the number of logical cores.",
    )(func)
    func = click.option(
        "-f",
        "--format",
        envvar="SPINDLE_TOKEN_FORMAT",
        type=click.Choice(("parquet", "csv"), case_sensitive=False),
        required=True,
        help="The file format of input and output data files.",
    )(func)
    func = click.option(
        "-k",
        "--key",
        envvar=_PRIVATE_KEY_ENV_VAR + "_FILE",
        type=click.File(mode="rb"),
        help="The PEM file containing your private key.",
    )(func)
    return func


@click.group()
def cli():
    """A command line tool for tokenizing and transcoding data files using the Open Privacy Preserving Record Linkage (OPPRL) protocol."""
    pass


@cli.command()
@click.option(
    "-t",
    "--token",
    type=TOKEN_CHOICE,
    multiple=True,
    required=True,
    help="An OPPRL token to add to the dataset. Can be passed multiple times.",
)
@common_options
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path(exists=False))
def tokenize(
    input: str,
    output: str,
    key: BytesIO | None,
    token: list[str],
    format: str,
    parallelism: int | None,
):
    """Add tokens to a dataset of PII.

    Creates a dataset at the OUTPUT location that adds encrypted OPPRL tokens to the INPUT dataset.
    Does not modify the INPUT dataset.

    INPUT is the path to the dataset to tokenize. If INPUT is a file, it must be of the format provided to
    the `--format` option. If INPUT is a directory, all files within the directory that match the given
    format will be considered a partition of the dataset.

    OUTPUT is the file or directory in which the tokenized dataset will be written. If INPUT is a
    file, the OUTPUT will be written to as a file. If INPUT is a directory, the OUTPUT will be a directory
    containing a dataset partitioned into files.
    """
    from spindle_token import tokenize as tokenize_df

    input_path = Path(input)
    output_path = Path(output)
    tokens = [TOKEN_SPECS[t] for t in token]
    tokens.sort(key=lambda t: t.name)

    spark = get_spark(parallelism)
    df = spark.read.format(format).option("delimiter", "|").option("header", True).load(input)
    col_mapping = infer_column_mapping(df.columns)

    df = tokenize_df(
        df,
        col_mapping,
        tokens,
        key.read() if key else None,
    )

    if input_path.is_file():
        df = df.repartition(1)

    dfw = df.write.format(format).option("delimiter", "|").option("header", True)
    if input_path.is_file():
        write_single_file(dfw, str(output_path))
    else:
        dfw.save(str(output_path))


@cli.group()
def transcode():
    """Prepare tokenized datasets to be sent or received."""
    pass


def _run_transcode(
    input: str,
    output: str,
    key: BytesIO | None,
    token: list[str],
    format: str,
    parallelism: int | None,
    direction: str,
    recipient: BytesIO | None = None,
):
    from spindle_token import transcode_in as transcode_in_df
    from spindle_token import transcode_out as transcode_out_df

    input_path = Path(input)
    output_path = Path(output)
    tokens = [TOKEN_SPECS[t] for t in token]
    tokens.sort(key=lambda t: t.name)

    spark = get_spark(parallelism)
    df = spark.read.format(format).option("delimiter", "|").option("header", True).load(input)

    if direction == "out":
        df = transcode_out_df(
            df,
            tokens,
            recipient_public_key=recipient.read() if recipient else None,
            private_key=key.read() if key else None,
        )
    else:
        df = transcode_in_df(
            df,
            tokens,
            private_key=key.read() if key else None,
        )

    if input_path.is_file():
        df = df.repartition(1)

    dfw = df.write.format(format).option("delimiter", "|").option("header", True)
    if input_path.is_file():
        write_single_file(dfw, str(output_path))
    else:
        dfw.save(str(output_path))


@transcode.command("out")
@click.option(
    "-t",
    "--token",
    type=TOKEN_CHOICE,
    multiple=True,
    required=True,
    help="The column name of an OPPRL token on the input data to transcode.",
)
@click.option(
    "-r",
    "--recipient",
    envvar=_RECIPIENT_PUBLIC_KEY_ENV_VAR + "_FILE",
    type=click.File(mode="rb"),
    help="The PEM file containing the recipients public key.",
)
@common_options
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path(exists=False))
def out(
    input: str,
    output: str,
    key: BytesIO | None,
    recipient: BytesIO | None,
    token: list[str],
    format: str,
    parallelism: int | None,
):
    """Prepare ephemeral tokens for a specific recipient."""
    _run_transcode(
        input,
        output,
        key,
        token,
        format,
        parallelism,
        direction="out",
        recipient=recipient,
    )


@transcode.command("in")
@click.option(
    "-t",
    "--token",
    type=TOKEN_CHOICE,
    multiple=True,
    required=True,
    help="The column name of an OPPRL token on the input data to transcode.",
)
@common_options
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path(exists=False))
def in_(
    input: str,
    output: str,
    key: BytesIO | None,
    token: list[str],
    format: str,
    parallelism: int | None,
):
    """Convert a dataset of ephemeral tokens into tokens."""
    _run_transcode(
        input,
        output,
        key,
        token,
        format,
        parallelism,
        direction="in",
    )
