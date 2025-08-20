The spindle-token API is broken up into 3 namespaces: 

1. The top-level `spindle_token` module containing the most commonly used public functions.
1. An OPPRL module containing implementations of every OPPRL protocol version, including PII normalization, token specifications, and cryptographic transformations.
1. A module of `core` abstractions than can be extended in advanced use cases to add additional functionality.

## ::: spindle_token
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## ::: spindle_token.opprl
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## ::: spindle_token.core
    handler: python
    options:
      members_order: source
      show_root_heading: true
      show_source: false
