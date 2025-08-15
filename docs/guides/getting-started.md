# Getting Started

This guide provides installation instructions and a quick tour of the main features of spindle-token. 

## Installation

Spindle-token is a cross-platform python library built on top of [PySpark](https://spark.apache.org/docs/latest/api/python/index.html). It supports the following range of versions:

- Python: 3.10+
- PySpark: 3.5+

The latest stable release of spindle-token can be installed from [PyPI](https://pypi.org/project/spindle-token/) into your active Python environment using `pip`.

```
pip install spindle-token
```

You can also build spindle-token from source using [Poetry](https://python-poetry.org/). The source code is hosted [on Github](https://github.com/spindle-health/carduus). Checkout the commit you wish to build and run  `poetry build` in the project's root.

## Encryption Keys

Tokenization is a specific use case of cryptography and relies on encryption keys. Spindle-token users are expected to provision and manage their own encryption keys.

There are 3 kinds of encryption keys that play different roles:

  1. Your private RSA key - Used to transcode incoming data and derive a symmetric encryption key used to tokenize PII. **This key must never be shared or accessed by untrusted parties.**
  2. Your public RSA key - The public key counterpart to your private key. This key will be shared with trusted parties that will be sending you tokenized data.
  3. Trusted partner public keys - A collection of public keys from the various trusted parties that you will be sending tokenized to.

### Configuring Encryption Keys

Organization's often manage encryption keys using a secrets manager, such as [AWS Secrets Manger](https://aws.amazon.com/secrets-manager/), [HashiCorp Vault](https://www.hashicorp.com/en/products/vault), [Databricks Secrets Manger](https://docs.databricks.com/aws/en/security/secrets/), and [EnvKey](https://www.envkey.com/). It is recommended that spindle-token users adopt a secrets manager in order to control which principals have access to encryption keys.

To help encourage users to not hard-code encryption keys in their source code, spindle-token will default to reading encryption keys from environment variables. Most secrets managers support the pattern of injecting secrets as ephemeral environment variables and the use of an `.env` file on developer workstations allow for easy local development. You can set the `SPINDLE_TOKEN_PRIVATE_KEY` environment variable with a private RSA keys in the PEM format.

In some cases, it may be appropriate to explicitly pass the private keys as arguments to the relevant spindle-token functions. For example, if your organization's secret manager encourages programmatic access at runtime or if you are overriding the encryption key during testing. Spindle-token functions support the explicit passing of encryption keys as `bytes` but it is highly recommended that users do not hardcode encryption keys into source code or put PEM files with production encryption keys into version control.

This guide assumes that your private key is set via the `SPINDLE_TOKEN_PRIVATE_KEY` environment variable. For information about passing the private key as an explicit argument, see the spindle-token [API](../api.md) documentation. 

Public keys don't need to be managed as secrets. It is possible to specify the public key (in PEM format) for your intended data recipient with the `SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY` environment variable or pass the public key to spindle-token functions as an explicit argument. Users can pick whichever method is more convenient. 

### Generating New Keys

Spindle-token expects encryption keys to be represented with the [PEM](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail) encoding. Private keys should use the [PKCS #8](https://en.wikipedia.org/wiki/PKCS_8) format and public keys should be formatted as [SubjectPublicKeyInfo](https://www.rfc-editor.org/rfc/rfc5280#section-4.1). It is recommended to use a key size of 2048 bits.

You can generate these keys using tools like [`openssl`](https://www.openssl.org/) or by calling the `generate_pem_keys` function provided by Carduus. This function will return a `tuple` containing 2 instances of `bytes`. The first is the PEM data for your private key that you must keep secret. The second is the PEM data for your public key that can may share with the parties you intend to receive data from.

You can decode these keys into strings of text (using UTF-8) or write them into a `.pem` file for later use. 

``` python
from spindle_token import generate_pem_keys

private, public = generate_pem_keys()  # Or provide key_size= 

print(private.decode())
# -----BEGIN PRIVATE KEY-----
# ... [ Base64-encoded private key ] ...
# -----END PRIVATE KEY-----

print(public.decode())
# -----BEGIN PUBLIC KEY-----
# ... [ Base64-encoded public key ] ...
# -----END PUBLIC KEY-----
```

## Tokenization

Tokenization refers to the process of replacing PII with encrypted tokens using a one-way cryptographic function. Spindle-token implements the OPPRL tokenization protocol, which performs a standard set of PII normalizations and transformations such that records pertaining to the same subject are more likely to receive the same token despite minor differences in PII representation across records. According to the OPPRL protocol, the normalized PII is then hashed using SHA512 and encrypted with a symmetric encryption based on a secret key derived from your private RSA key. In the event that the private encryption key of one party is compromised, there risk to all other parties is mitigated by the fact that everyone's tokens are encrypted with a different key.

To demonstrate the tokenization process, we will use a dataset of 5 records shown below. Each record is assigned a `label` that corresponds to the true identity of the subject.

```python
pii = spark.createDataFrame(
    [
        (1, "Jonas", "Salk", "male", "1914-10-28"),
        (1, "jonas", "salk", "M", "1914-10-28"),
        (2, "Elizabeth", "Blackwell", "F", "1821-02-03"),
        (3, "Edward", "Jenner", "m", "1749-05-17"),
        (4, "Alexander", "Fleming", "M", "1881-08-06"),
    ],
    ("label", "first_name", "last_name", "gender", "birth_date")
)
pii.show()
# +-----+----------+---------+------+----------+
# |label|first_name|last_name|gender|birth_date|
# +-----+----------+---------+------+----------+
# |    1|     Jonas|     Salk|  male|1914-10-28|
# |    1|     jonas|     salk|     M|1914-10-28|
# |    2| Elizabeth|Blackwell|     F|1821-02-03|
# |    3|    Edward|   Jenner|     m|1749-05-17|
# |    4| Alexander|  Fleming|     M|1881-08-06|
# +-----+----------+---------+------+----------+
```

To perform tokenization, call the `tokenize` function and pass it the PII `DataFrame`, a column mapping, a collection of specifications for token you want to generate, and the private key.

```python
from spindle_token import tokenize
from spindle_token.opprl import OpprlV1

tokens = tokenize(
    pii,
    col_mapping={
        OpprlV1.first_name: "first_name",
        OpprlV1.last_name: "last_name",
        OpprlV1.gender: "gender",
        OpprlV1.birth_date: "birth_date",
    },
    tokens=[OpprlV1.token1, OpprlV1.token2, OpprlV1.token3],
).drop("first_name", "last_name", "gender", "birth_date")
# +-----+--------------------+--------------------+--------------------+
# |label|     opprl_token_1v1|     opprl_token_2v1|     opprl_token_3v1|
# +-----+--------------------+--------------------+--------------------+
# |    1|4YO6eFn0u75yrF+Td...|V6uRRgDgXylFsNM2c...|6N+/voOASNM0ivgA7...|
# |    1|4YO6eFn0u75yrF+Td...|V6uRRgDgXylFsNM2c...|6N+/voOASNM0ivgA7...|
# |    2|OLU6TLB4XdWmfhvIS...|3QtCKgeIqO20Jgp9E...|JrrjTxjy97Xe93afx...|
# |    3|mks197t0d8Uhc3l3s...|YccsE4wMErZvz9oBd...|7wMcWiDjImAhuP307...|
# |    4|kRZfUY8KScpiHKmxC...|1gKSJDcq6YtwdUc7C...|3h8jUydHTIcOU8jhW...|
# +-----+--------------------+--------------------+--------------------+
```

For OPPRL tokens, the column names used for token columns are the OPPRL token names. See the OPPRL protocol for more information on understanding the token name format.

Notice that both records with `label = 1` received the same pair of tokens despite slight representation differences in the original PII.

The `col_mapping` argument is a dictionary that maps standard OPPRL attributes -- logical types of PII fields -- to the corresponding column names from the `pii` DataFrame. This tells spindle-health how to normalize and enhance the values found in each column. For example, the `Opprlv1.first_name` object will apply name cleaning rules of version 1 of the OPPRL protocol to the values found in the `first_name` column. Spindle-token will also use this mapping to determine how to derive other PII attributes (such as first initial, soundex, and metaphone encodings) used to construct some tokens.

The `tokens` argument is collection of OPPRL token specifications. Each token specification denotes a PII fields to jointly hash and encrypt to create a token. The current latest version of the OPPRL protocol supports three token specifications, described below:

| Token | Fields to jointly encrypt |
|-|-|
| `token1` | `first_initial`, `last_name`, `gender`, `birth_date` |
| `token2` | `first_soundex`, `last_soundex`, `gender`, `birth_date` |
| `token3` | `first_metaphone`, `last_metaphone`, `gender`, `birth_date` |

> :bulb: **Why multiple tokens?** 
>
> Each use case has a different tolerance for false positive and false negative matches. By producing multiple tokens for each record using PII attributes, each user can customize their match logic to trade-off between different kinds of match errors. Linking records that match on _any_ token will result in fewer false negatives, and linking records that match _all_ tokens will result in fewer false positives. User can design their own match strategies by using subsets of tokens.

## Transcoding and Ephemeral Tokens

Tokens generated by a given private key can be transcoded into tokens that match those of another private key through a "transcoding" protocol. This process is most commonly done in the context of data sharing.

Transcoding is performed when a user wants to share tokenized data with another party. The sender and recipient each have a corresponding transcode function that must be invoked to ensure safe transfer of data between trusted parties. The sender transcodes their tokens into "ephemeral tokens" that are specific to the transaction. In other words, the ephemeral tokens do not match the sender's data, the recipients data, or any prior ephemeral token from any _any_ transactions between _any_ sender and recipient. 

Furthermore, records from the same dataset that have identical PII will be assigned unique ephemeral tokens. This destroys the utility of the tokens until the recipient performs the reverse transcoding process using their private key. This is beneficial in the event that a third party gains access to the dataset during transfer (eg. if transcoded datasets are delivered over an insecure connection) because records pertaining to the same subject cannot be associated with each other.

### Sender

Spindle-token provides the `transcode_out` function in for the sender to call on their tokenized datasets. In the following code snippet, notice the 2 records pertaining to the same subject (`label = 1`) no longer have identical tokens. The `tokens_to_send` DataFrame can safely be written files or a database and delivered to the recipient.

```python
from spindle_token import transcode_out
from spindle_token.opprl import OpprlV1

tokens_to_send = transcode_out(
    tokens, 
    tokens=(OpprlV1.token1, OpprlV1.token2, OpprlV1.token3), 
    recipient_public_key=b"""-----BEGIN PUBLIC KEY----- ...""",
)
tokens.to_send.show()
# +-----+--------------------+--------------------+--------------------+
# |label|     opprl_token_1v1|     opprl_token_2v1|     opprl_token_3v1|
# +-----+--------------------+--------------------+--------------------+
# |    1|EUqTdA0Yjb5F82oqj...|iiI/sd+sn+t5qhPDg...|JwpZVzJe+3CMocLGK...|
# |    1|AXVDGgQ0KMa1ek1/v...|YJeYE9pz507CUzlks...|EKY2JdLhduCq+zj+p...|
# |    2|XmYqnnMDqD9ZUR6yS...|A4e3L03NWeKbbL8vR...|CAZtbKDYjbbHsqVdc...|
# |    3|mVhnJ1kz+ZRZvlfKo...|uO13H3LHjVnZ1flUp...|C16RKoV4SvWD5wuVp...|
# |    4|oI/KS52K4H3M+WBdn...|vczsK9qlbU6TN1vBi...|fpTSrlGgtz3vASFXc...|
# +-----+--------------------+--------------------+--------------------+
```

The `tokens` argument is a iterable collection containing the token specifications of the tokens to transcode. It is expected that the input DataFrame has columns with the same name as the `name` attribute of the token specification. For example, `OpprlV1.token3.name` is `opprl_token_3v1` and therefore the DataFrame must contain a column with that name. See the OPPRL protocol for more information on understanding the token name format.

The `recipient_public_key` argument is the public key provided by the intended recipient. 

### Recipient

The `transcode_in` function provides the transcoding process for the recipient. It is called on a dataset produced by the sender using `transcode_out` to convert ephemeral tokens into normal tokens that will match other tokenized datasets maintained by the recipient, including prior datasets delivered from the same sender.

Notice that the first 2 records pertaining to the same subject (label = 1) have identical tokens again, but do these tokens are not the same as the original tokens because they are encrypted with the scheme for the recipient.

```python
from spindle_token import transcode_in
from spindle_token.opprl import OpprlV1

tokens_received = transcrypt_in(
    tokens_to_send, 
    tokens=(OpprlV1.token1, OpprlV1.token2, OpprlV1.token3), 
)
tokens_received.show()
# +-----+--------------------+--------------------+--------------------+
# |label|       opprl_token_1|       opprl_token_2|       opprl_token_3|
# +-----+--------------------+--------------------+--------------------+
# |    1|q1a5o2gg1hX+CFjdP...|gS2WMqSs0SReCN7Xp...|2XAgeBwomiKq2s4xO...|
# |    1|q1a5o2gg1hX+CFjdP...|gS2WMqSs0SReCN7Xp...|2XAgeBwomiKq2s4xO...|
# |    2|FEyzz52oI5hym0G/+...|UQDqblSWpqoH5znps...|ud2cG5YD/2yBhjZel...|
# |    3|aEgXJZGUpkZxt7Z7T...|QZ5LaSiE8pMDmMKEu...|0yQxfv4EFAUHatIva...|
# |    4|CgmXSDhY8mxZCRV+O...|wu+eNOqsOvh14Bjei...|7aGCrwy8tTDL3RkO6...|
# +-----+--------------------+--------------------+--------------------+
```

As with `transcode_out`, the `tokens` argument is a iterable collection containing the token specifications of the tokens to transcode. The input DataFrame is expected to include columns with names matching the  `.name` of each token specification.

## Deployment

Spindle-token is a Python library that uses PySpark to parallelize and distribute the tokenization and transcode workloads. Your application that uses spindle-token can be submitted to any compatible Spark cluster, or use a connection to a remote spark cluster. Otherwise, spindle-health will start a local Spark process that uses the resources of the host machine.

For more information about different modes of deployment, see the official Spark documentation.

- [Submitting Applications to a Spark cluster](https://spark.apache.org/docs/latest/submitting-applications.html)
- [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html)

## Next Steps

- Reference the full [API](../api.md)
- Learn more about using Carduus and extending it's functionality from the advanced user guides:
    - [Using spindle-token on Databricks](./databricks.ipynb)
    - [Defining custom token specifications](./custom-tokens.md)
