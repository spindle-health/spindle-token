# <a name="custom-token"></a> Custom Tokens

In essence, a token is simply the concatenation of multiple PII fields into a single string of text which is passed through a hash and a subsequent cryptographic function. The spindle-token library provides implementations of the standard tokens proposed in the OPPRL protocol. These tokens are known to have low false positive and false negative match rates for subjects whose PII is close to the distribution found in the United States population, while only relying on a minimal set of PII attributes that are commonly populated in real-world datasets.

Some users may want to create tokens by concatenating different PII fields than what is proposed by OPPRL. This is often the case when the datasets of a particular user and the parties they share data with have additional PII beyond what OPPRL leverages that will lead to lower match errors for the sample of subjects used their use case. For a guide on how to extend spindle-token with support for additional PII attributes, see [Custom PII Attributes](#custom-pii).

To create a custom token specification, simply instantiate the [spindle_token.core.Token][spindle_token.core.Token] class and provide the required arguments.

The following code example creates a new `Token` definition called `"my_custom_token"` that tokenizes the first initial, the metaphone phonetic encoding of the last name, and the birth date. All fields are combined, hashed, and encrypted according to v1 of the OPPRL protocol.

```python
from spindle_token.core import Token
from spindle_token.opprl import OpprlV1

my_token = Token(
    "my_custom_token",
    OpprlV1.protocol,
    attributes=[
        OpprlV1.first_name.initial,
        OpprlV1.last_name.metaphone,
        OpprlV1.birth_date,
    ]
)
```

The `name` attribute will be used as the the column name expected by the other spindle-token functions. Thus, it must only contain character that are safe in column names and each logically distinct `Token` instance should have a unique name. 

The `protocol` argument can be any implementation of [TokenProtocolFactory][spindle_token.core.TokenProtocolFactory] but for the remainder of this section we will use the OPPRLv1 implementation provided by spindle-token. See [this section](#custom-protocol) for more details.

The instances of [PiiAttribute][spindle_token.core.PiiAttribute] in the `attributes` argument determine the PII fields to jointly tokenize into a single token. This guide uses `PiiAttribute` objects for OPPRLv1 that are built-in to the spindle-health library. See [Custom PII Attributes](#custom-pii) for a guide on defining your own PII.

We can pass `my_token` to the core spindle-token functions -- such as `tokenize`, `transcode_out`, and `transcode_in` -- alongside the OPPRL tokens or other custom token specifications.

# <a name="custom-pii"></a> Custom PII Attributes

The OPPRL provides specifications for normalizing and transforming some commonly used PII attributes to account for representation differences that often don't indicate a different true identity for the subject. For example, the OPPRL protocol calls for first names to have their capitalization changed to uppercase and strings of whitespace replaced with a single space character among other normalizations. Furthermore, a "first initial" attributes can be derived from a normalized first name attribute by taking the first letter.

In some cases, users may be working with datasets that have PII attributes that aren't included in the OPPRL specification. In some scenarios, these additional PII attributes may be useful for creating custom tokens with the optimal match quality for your use case. Spindle-token provides the `PiiAttribute` that can be extended to add support for additional types of PII.

> :warning: Note
>
> Tokenization is often used in scenarios where datasets from different origins will be joined on token equality. Be sure all data origins are using the _exact_ same PII attributes and normalization logic when creating tokens that will be compared. Using (even slightly) different PII attributes and normalization logic can negatively impact match quality or make matching impossible.

The follow code example shows an implementation of `PiiAttribute` for a hypothetical PII attribute containing the first 3 digits of a US zipcode.

```python
from pyspark.sql import Column
from pyspark.sql.functions import lpad, substring, trim
from pyspark.sql.types import DataType, StringType
from spindle_token.core import PiiAttribute

class CustomZip3Attribute(PiiAttribute):

    def __init__(self):
        super().__init__("my-company.zip3")

    def transform(self, column: Column, dtype: DataType) -> Column:
        if not isinstance(dtype, StringType):
            column = column.cast(StringType())
        column = lpad(trim(column), 5, "0")
        column = substring(column, 1, 3)
        return column

```

The `__init__` method must call the `super()` constructor of `PiiAttribute` and pass an attribute identifier string. The attribute identifier is used uniquely identify the logical PII attribute expressed by the instance of `PiiAttribute`. In this case, all instances of `CustomZip3Attribute` use the same logic to produce the same normalized PII attribute (ie. zip3), therefore we hardcode `"my-company.zip3"` as the identifier. If `CustomZip3Attribute` was reused for multiple different PII attributes -- perhaps through some parameterization --  it would be required for the attribute ID to differ across instances.

All built-in `PiiAttribute` classes for the OPPRL standard use the prefix `opprl.v{X}` where `{X}` is the major version number of the OPPRL protocol that specifies the normalization logic. It is recommended that implementers of `PiiAttribute` namespace their attribute IDs to avoid collision with other implementations of similar PII attributes.

# <a name="custom-protocol"></a> Custom Protocols

The core of the OPPRL protocol is a set of specific data flows that produce tokens from sets of PII attributes using various cryptographic functions. In the spindle-token library, these data flows are provided by implementations of the `TokenProtocol` and `TokenProtocolFactory` interfaces.