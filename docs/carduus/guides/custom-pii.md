# Using Custom PII

Carduus applies normalizations to incoming PII to account for representation differences that often don't indicate a different true identity for the subject. After normalizing the PII columns present on the data provided by the user, Carduus derives additional implicit PII attributes and adds them to the Dataset if they aren't already present. The procedure is called "enhancement". For example, a `first_initial` attributes can be derived from a `first_name` attribute by taking the first letter.

Commonly used PII attributes have standard normalization and enhancement rules proposed in the OPPRL protocol. Carduus provides builtin implementations of the normalizations and enhancements via a set of concrete classes that implement the `PiiTransform` interface.

Users may extend the `PiiTransform` abstract base class and provide instances to the column mapping used by the `tokenize` function.

> :warning: This guide is incomplete.