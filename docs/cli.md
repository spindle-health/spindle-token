# Command Line Interface

The spindle-token command line interface (CLI) offers tokenization and token transcoding capabilities of data files from the local file system.

# Usage Guide

The spindle-token CLI is included with every installation of the spindle-token library. Install spindle-token to your python (virtual) environment using `pip`. See our [getting started guide](./guides/getting-started.md) for more information. Make sure the python interpreter directory is on your PATH.

You can test your installation and environment setup by running the `--help` command. You should see documentation about the spindle-token CLI.

```
spindle-token --help
```

Once installed, you can run the commands for `tokenize` and `transcode` with the relevant options and arguments. All commands and sub-commands follow the same general design. Positional arguments and paths the the input data and desired location to write output data. Options configure how the input data is transformed. For example, options dictate which tokens should be generated, which file format to use, and the encryption key.

This example invocation of the `tokenize` command illustrates the general pattern.

```
spindle-token tokenize \
    --token opprl_token_1v1 --token opprl_token_2v1 --token opprl_token_3v1 \
    --key private_key.pem \
    --format csv \
    --parallelism 1 \
    pii.csv tokens.csv
```

### Encryption Keys

The OPPRL protocol leaves the responsibility of encryption key management to the user. The spindle-token CLI assumes the public and private keys are stored in files on the local filesystem. The location of the PEM file can be passed using the corresponding option or an environment variable. This table describes the option names and environment variables that can be use to supply private and public keys respectively.

**Private Key**

The private RSA key can be set using one of the following methods:

- Use the `--key` option (or `-k` alias) to specify a path to a PEM file.
- Set the `SPINDLE_TOKEN_PRIVATE_KEY_FILE` environment variable to specify a path the PEM file.
- Set the `SPINDLE_TOKEN_PRIVATE_KEY` environment variable to specify the key as a UTF-8 string. If both environment variables are set, the `_FILE` variant takes precedence.

**Public Keys**

The public keys of data recipients (used in transcryption) can be set using one of the following methods:

- Use the `--recipient` option (or `-r` alias) to specify a path to a PEM file.
- Set the `SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY_FILE` environment variable to specify a path the PEM file.
- Set the `SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY` environment variable to specify the key as a UTF-8 string. If both environment variables are set, the `_FILE` variant takes precedence.

### File Formats

The spindle-token CLI supports multiple data file formats. The recommended file format is [parquet](https://parquet.apache.org/) because parquet files are efficient, compressed, and have unambiguous schemas. The spindle-token CLI also supports CSV files. The file format must be specified using the `--format` option or `SPINDLE_TOKEN_FORMAT` environment variable.

When using CSV files there are a few assumptions that the data file(s) must meet. 

1. The first row of each CSV file must be column headers. See the [next section](#column-names) for expectation on the column names.

2. The field separate (aka delimiter) should be a pipe `|` character.

Input datasets can either be a single data file or partitioned into a directory of multiple data files. Splitting larger datasets into multiple data files can help the CLI [parallelize](#parallelism) over larger datasets. If the input dataset is a single data file the output dataset will be single data file. Similarly, if the input dataset is a directory the argument for the output location will be a directory that will include a partitioned output dataset.

### <a name="column-names"></a> Column Names

THe OPPRL tokenization protocol requires specific PII attributes to be normalized, transformed, concatenated, hashed, and then encrypted together. Thus, the spindle-token CLI must know which columns of the input dataset correspond to each logical PII attribute (first name, last name, birth date, etc).

The spindle-token CLI requires specific column naming for PII columns so that the proper normalization rules are applied to each attribute and the final tokens are created from the correct subset of inputs. The following list contains the exact column names the CLI will look for.

- `first_name`
- `last_name`
- `gender`
- `birth_date`
- `email`
- `hem`
- `phone`
- `ssn`
- `group_number`
- `member_id`

If you are only adding tokens that require a subset of these PII fields, the input dataset may omit columns for the other PII attributes that are not required. For information on which PII attributes are required for each token in the OPPRL protocol, see the official [specification](./opprl/PROTOCOL.md).

### <a name="parallelism"></a> Parallelism

The spindle-token CLI supports multi-threaded parallelism. This helps work with larger datasets that are partitioned into multiple part files within a directory. If `--parallelism` is set to a number >1 then that number of partition files will be processed at once. This option can also be set with the `SPINDLE_TOKEN_PARALLELISM` environment variable. If parallelism is not provided, the spindle-token will default to using the same number of threads as the host machine has logical cores.

# Commands

The help text, options, and arguments of every command and sub-command of the spindle-token CLI. You can get this documentation for the specific version of the CLI installed in your python environment using the `--help` option on any command or sub-command.

::: mkdocs-click
    :module: spindle_token._cli
    :command: tokenize
    :prog_name: spindle-token tokenize
    :depth: 1

::: mkdocs-click
    :module: spindle_token._cli
    :command: transcode
    :prog_name: spindle-token transcode
    :depth: 1

# Limitations

The following limitations of the spindle-token CLI are. For a superior experience, consider using spindle-token as a Python library. If your use case requires addressing some of these limitations, please open an [issue](https://github.com/spindle-health/spindle-token/issues) with additional details. 

### No Horizontal Scaling

The spindle-token CLI is built with [Apache Spark](https://spark.apache.org/) to allow for data parallelism. Spark is designed to distribute workloads horizontally across a cluster of multiple machines connected to the same network. The spindle-token CLI runs spark in "local" mode that switches execution to a multi-threaded design on a single host machine.

The spindle-token CLI cannot be passed to `spark-submit`, nor is there currently a way to pass [spark connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) information to a remote spark cluster. If you would like to use spindle-token on a Spark cluster, it is recommended that you use the spindle-token Python library.

### No Remote File Systems

The spindle-token CLI reads and writes files using the local file system. This means there is no native support for files stored on remote filesystems, like S3. 

You may be able to work with remote file systems if you have a method of mounting the remote file system to the local file system.

If you require working with datasets on remote filesystems like S3, it is recommended that you use the spindle-token Python library and configure pyspark to read from S3.
