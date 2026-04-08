# Changelog

All notable changes to `spindle-token` will be documented in this file.

## 2.0.0

### Security and Compatibility

- Stabilized v2 token derivation across equivalent PEM encodings.
- Added tests to ensure v2 rejects non-RSA private keys.
- Fixed an SSN normalization bug.
- Updated dependency security upgrades.
- Preserved V0 and V1 token outputs while making token assembly Spark 4-safe for mixed
  string/date inputs.
- Verified the Spark 4.0.2 Databricks notebook path with string `birth_date` inputs.
- Widened the `pyspark` install metadata to allow Spark 3.5.x through 4.1.x
  environments.
- Verified the Spark 4.1 Databricks notebook and local test path after widening
  the supported Spark range.

### Documentation

- Added a note in the getting-started guide explaining that `oci` and
  `pyOpenSSL` warnings are typically transitive environment noise rather than
  direct runtime dependencies of `spindle-token`.
- Updated the getting-started guide to reflect the tested Spark 3.5.x through
  4.1.x matrix.
- Updated the Echelon security statement in the README.
- Refreshed links after the repository rename.
- Added the `CNAME` file for the documentation site.

## 1.0.0 - 2025-10-30

- First `spindle-token` 1.0 release.
- Added a clearer error message when a required attribute is missing.
