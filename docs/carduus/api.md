# Functions

THe core functionality of carduus is provided by a the following functions that operate over pyspark `DataFrame`.

## ::: carduus.token.tokenize
    handler: python
    options:
      show_root_heading: true
      show_source: false
      separate_signature: true
      show_signature_annotations: false

## ::: carduus.token.transcrypt_out
    handler: python
    options:
      show_root_heading: true
      show_source: false
      separate_signature: true
      show_signature_annotations: false

## ::: carduus.token.transcrypt_in
    handler: python
    options:
      show_root_heading: true
      show_source: false
      separate_signature: true
      show_signature_annotations: false

The `TokenSpec` class allows for custom tokens, beyond the builtin OPPRL tokens, to be generated during tokenization. See the [custom tokens guide](./guides/custom-tokens.md) for more information.

## ::: carduus.token.TokenSpec
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false

# OPPRL Implementation

Although carduus is designed to be extensible, most users will want to use the tokenization procedure proposed by the [Open Privacy Preserving Record Linkage](../opprl.md) (OPPRL) protocol. This open specification proposes standard ways of normalizing, enhancing, and encrypting data such that all user across all OPPRL implementations, including carduus, can share data between trusted parties.

The following two `enum` objects provide `PiiTransform` instances and `TokenSpec` instances that comply with OPPRL. These can be passed to the column mapping and token set arguments of `tokenize` respectively.

## ::: carduus.token.OpprlPii
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## ::: carduus.token.OpprlToken
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false

# Encryption Key Management

See the [encryption key section](./guides/getting-started.md#encryption-keys) of the "Getting started" guide for details about how Carduus accesses encryption keys.

## ::: carduus.keys.generate_pem_keys
    handler: python
    options:
      show_root_heading: true
      show_source: false
      separate_signature: true
      show_signature_annotations: false


# Interfaces

Carduus offers interfaces that can be extended by the user to add additional behaviors to the tokenization and transcryption processes.

The `PiiTransform` abstract base class can be extended to add support for custom PII attributes, normalizations, and enhancements.

## ::: carduus.token.PiiTransform
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false
