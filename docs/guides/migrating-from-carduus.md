# Migrating from carduus to spindle-token

Prior to the v1.0 release of [spindle-token](https://pypi.org/project/spindle-token/), the python library [carduus](https://pypi.org/project/carduus/) implemented the early version of the Open Privacy Preserving Record Linkage (OPPRL) protocol.
Since the release of carduus, new versions of the OPPRL protocol have been published and the spindle-token python package was introduced. 
See [the pull request](https://github.com/spindle-health/spindle-token/pull/26) for the full story.

In summary, carduus only supported version 0 of the OPPRL while spindle-token supports all versions (including version 0). The spindle-token library can be parameterized to produce tokens that match tokens produced by carduus.

This guide is intended to help carduus users migrate their code to spindle-token without breaking their tokenized data assets. At a high level, the abstraction of both libraries is the same. The main differences are in parameterization and naming. 

## Tokenization

The API for tokenizing PII has undergone the largest change in spindle-token, however the parameters are very similar to carduus.

The following code snippets show the function calls to tokenize PII attributes in both carduus and spindle-token. Both code examples assume the private key is passed via environment variables, although it can be passed explicitly in both libraries.

```python
# carduus
from carduus.token import tokenize, OpprlPii, OpprlToken

tokens = tokenize(
    pii,
    pii_transforms=dict(
        first_name=OpprlPii.first_name,
        last_name=OpprlPii.last_name,
        gender=OpprlPii.gender,
        birth_date=OpprlPii.birth_date,
    ),
    tokens=[OpprlToken.token1, OpprlToken.token2, OpprlToken.token3],
)
# +-----+--------------------+--------------------+--------------------+
# |...  |       opprl_token_1|       opprl_token_2|       opprl_token_3|
# +-----+--------------------+--------------------+--------------------+
# |     |4YO6eFn0u75yrF+Td...|V6uRRgDgXylFsNM2c...|6N+/voOASNM0ivgA7...|
# +-----+--------------------+--------------------+--------------------+
```

```python
# spindle-health
from spindle_token import tokenize
from spindle_token.opprl import OpprlV0 as v0  # OPPRL v0 tokens match carduus

tokens = tokenize(
    pii,
    col_mapping={
        v0.first_name: "first_name",
        v0.last_name: "last_name",
        v0.gender: "gender",
        v0.birth_date: "birth_date",
    },
    tokens=[v0.token1, v0.token2, v0.token3],
)
# +-----+--------------------+--------------------+--------------------+
# |...  |     opprl_token_1v1|     opprl_token_2v1|     opprl_token_3v1|
# +-----+--------------------+--------------------+--------------------+
# |     |4YO6eFn0u75yrF+Td...|V6uRRgDgXylFsNM2c...|6N+/voOASNM0ivgA7...|
# +-----+--------------------+--------------------+--------------------+
```

### Key Difference

**Protocol Versions -** Spindle-token requires importing the `PiiAttribute` and `Token` objects for specific versions of OPPRL. Carduus only supports version 0 of the OPPRL specification.

**Column mapping -** Carduus `pii_transforms` use column names as keys and values are PII attribute annotations. This limits each column being used once which prevents different tokens from processing the columns in different ways (ie. using multiple versions of OPPRL at the same time). Spindle-token solves remove this limitation in `col_mapping` by using PII attributes as the keys and column names as the values.

**Token Column Names -** The spindle-token library produced token columns that have versioned names to distinguish which version of the OPPRL specification was used.

## Transcoding

The API for transcoding between tokens and ephemeral tokens is nearly identical in spindle-token and carduus.

Carduus used the made-up verb "transcypt" instead of the more straightforward "transcode". The semantics are identical with respect to both projects and this guide will use "transcode".

```python
# carduus
from carduus.token import transcrypt_out, transcrypt_in, OpprlPii, OpprlToken

ephemeral_tokens = transcrypt_out(
    tokens, 
    token_columns=("opprl_token_1", "opprl_token_2", "opprl_token_3"), 
    recipient_public_key=b"""-----BEGIN PUBLIC KEY----- ...""",
)

tokens = transcrypt_in(
    ephemeral_tokens, 
    token_columns=("opprl_token_1", "opprl_token_2", "opprl_token_3"), 
)
```

```python
# spindle-health
from spindle_token import transcode_out, transcode_in
from spindle_token.opprl import OpprlV0 as v0

ephemeral_tokens = transcode_out(
    tokens, 
    tokens=(v0.token1, v0.token2, v0.token3), 
    recipient_public_key=b"""-----BEGIN PUBLIC KEY----- ...""",
)

tokens = transcode_in(
    ephemeral_tokens, 
    tokens=(v0.token1, v0.token2, v0.token3), 
)
```

### Key Difference

**Specifying Tokens -** Carduus only supported one method of transcoding tokens and therefore the API only required the names of columns containing tokens to transcode. Spindle-token allows each token to use its own protocol (ie. different versions of OPPRL) and thus the transcode functions need instances of `Token`. The column names are expected to match the `name` attribute of the `Token` instance.
