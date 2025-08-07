# Defining Custom Token Specifications

In essence, a token is simply the concatenation of multiple PII fields into a single string of text which is passed through a hash and a subsequent cryptographic function. Carduus provides specifications of the tokens proposed in the OPPRL protocol. These tokens are known to have low false positive and false negative match rates for subjects whose PII is close to the distribution found in the United States population, while only relying on a minimal set of PII attributes that are commonly populated in real-world datasets.

Some users may want to create tokens by concatenating different PII fields than what is proposed by OPPRL. This is often the case when the datasets of a particular user and the parties they share data with have additional PII beyond what OPPRL leverages that will lead to lower match errors for the sample of subjects used their use case. For a guide on how to extend Carduus with support for additional PII attributes, see [this guide](./custom-pii.md)

To declare a custom token specification, simply instantiate the `TokenSpec` class provided by Carduus and provide it with a token name and a collection of columns to jointly encrypt.

```python
from carduus.token import TokenSpec

my_token = TokenSpec(name="geo_token", fields=["last_name", "birth_date", "zipcode"])
```

This `my_token` object can be passed to the `tokenize` function to replace the PII columns with a `geo_token` column. If the PII data does not have one or more of the fields referenced by the token definition (after normalization and enhancement) the `tokenize` function will throw an exception prior to performing any significant workloads.
