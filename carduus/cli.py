import tempfile
import shutil
from pathlib import Path
from io import BytesIO
import click
from pyspark.sql import SparkSession, DataFrameWriter
import carduus.token as t
from carduus.keys import _PRIVATE_KEY_ENV_VAR, _RECIPIENT_PUBLIC_KEY_ENV_VAR


# All OPPRL token specifications in a dictionary for easy lookup by the token name.
TOKEN_SPECS: dict[str, t.OpprlToken] = {
    "opprl_token_1": t.OpprlToken.token1,
    "opprl_token_2": t.OpprlToken.token2,
    "opprl_token_3": t.OpprlToken.token3,
}


# The PII fields that each OPPRL token depends on.
TOKEN_PII_DEPENDENCIES: dict[str, list[t.OpprlPii]] = {
    "opprl_token_1": [
        t.OpprlPii.first_name,
        t.OpprlPii.last_name,
        t.OpprlPii.gender,
        t.OpprlPii.birth_date,
    ],
    "opprl_token_2": [
        t.OpprlPii.first_name,
        t.OpprlPii.last_name,
        t.OpprlPii.gender,
        t.OpprlPii.birth_date,
    ],
    "opprl_token_3": [
        t.OpprlPii.first_name,
        t.OpprlPii.last_name,
        t.OpprlPii.gender,
        t.OpprlPii.birth_date,
    ],
}


def pii_transforms_for_tokens(tokens: list[str]) -> dict[str, t.PiiTransform]:
    pii_transforms = {}
    for token in tokens:
        for pii_field in TOKEN_PII_DEPENDENCIES[token]:
            pii_transforms[pii_field.name] = pii_field.value
    return pii_transforms


def get_spark(num_threads: int | None) -> SparkSession:
    """Gets active spark session or creates a new local spark session."""
    p = str(num_threads) if num_threads else "*"
    return (
        SparkSession.Builder().master(f"local[{p}]").appName("Spindle Token CLI").getOrCreate()
    )


def write_single_file(dfw: DataFrameWriter, path: str):
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
    """A command line tool for tokenizing and transcrypting data files using the Open Privacy Preserving Record Linkage (OPPRL) protocol."""
    pass


@cli.command()
@click.option(
    "-t",
    "--token",
    type=click.Choice(list(TOKEN_SPECS.keys())),
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
    input_path = Path(input)
    output_path = Path(output)
    tokens = [TOKEN_SPECS[t] for t in token]
    tokens.sort(key=lambda t: t.name)
    pii_transforms = pii_transforms_for_tokens(token)

    spark = get_spark(parallelism)
    df = spark.read.format(format).option("delimiter", "|").option("header", True).load(input)

    df = t.tokenize(
        df,
        pii_transforms=pii_transforms,
        tokens=tokens,
        private_key=key.read() if key else None,
    )

    if input_path.is_file():
        df = df.repartition(1)

    dfw = df.write.format(format).option("delimiter", "|").option("header", True)
    if input_path.is_file():
        write_single_file(dfw, str(output_path))
    else:
        dfw.save(str(output_path))


@cli.group()
def transcrypt():
    """Prepare tokenized datasets to be sent or received."""
    pass


@transcrypt.command()
@click.option(
    "-t",
    "--token",
    type=click.Choice(list(TOKEN_SPECS.keys())),
    multiple=True,
    required=True,
    help="The column name of an OPPRL token on the input data to transcrypt.",
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
    """Prepare ephemeral tokens for a specific recipient.

    INPUT is the path to the dataset of tokens to create ephemeral tokens from. If INPUT is a file, it must
    be of the format provided to the `--format` option. If INPUT is a directory, all files within the
    directory that match the given format will be considered a partition of the dataset.

    OUTPUT is the file or directory in which the tokenized dataset will be written. If INPUT is a
    file, the OUTPUT will be written to as a file. If INPUT is a directory, the OUTPUT will be a directory
    containing a dataset partitioned into files.
    """
    input_path = Path(input)
    output_path = Path(output)
    spark = get_spark(parallelism)
    df = spark.read.format(format).option("delimiter", "|").option("header", True).load(input)

    df = t.transcrypt_out(
        df,
        token_columns=token,
        recipient_public_key=recipient.read() if recipient else None,
        private_key=key.read() if key else None,
    )

    if input_path.is_file():
        df = df.repartition(1)

    dfw = df.write.format(format).option("delimiter", "|").option("header", True)
    if input_path.is_file():
        write_single_file(dfw, str(output_path))
    else:
        dfw.save(str(output_path))


@transcrypt.command("in")
@click.option(
    "-t",
    "--token",
    type=click.Choice(list(TOKEN_SPECS.keys())),
    multiple=True,
    required=True,
    help="The column name of an OPPRL token on the input data to transcrypt.",
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
    """Convert a dataset of ephemeral tokens into tokens.

    INPUT is the path to the dataset of ephemeral tokens to create tokens from. If INPUT is a file, it must
    be of the format provided to the `--format` option. If INPUT is a directory, all files within the
    directory that match the given format will be run.

    OUTPUT is the file or directory in which the tokenized dataset will be written. If INPUT is a
    file, the OUTPUT will be written to as a file. If INPUT is a directory, the OUTPUT will be a directory
    containing a dataset partitioned into files.

    """
    input_path = Path(input)
    output_path = Path(output)
    spark = get_spark(parallelism)
    df = spark.read.format(format).option("delimiter", "|").option("header", True).load(input)

    df = t.transcrypt_in(
        df,
        token_columns=token,
        private_key=key.read() if key else None,
    )

    if input_path.is_file():
        df = df.repartition(1)

    dfw = df.write.format(format).option("delimiter", "|").option("header", True)
    if input_path.is_file():
        write_single_file(dfw, str(output_path))
    else:
        dfw.save(str(output_path))
