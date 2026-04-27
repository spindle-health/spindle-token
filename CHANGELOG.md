# Changelog

All notable changes to `spindle-token` will be documented in this file.

## 2.2.0

### Performance

- Improved Spark UDF crypto performance by reusing AES-GCM-SIV and RSA key objects
  within each deserialized UDF callable instead of rebuilding them for every row.
- In local Spark benchmarks, `transcode_in()` improved dramatically.

## 2.1.0

### Packaging and Architecture

- Split the library into a base install and an optional `spark` extra so the
  core package can be used without owning PySpark in the environment.
- Made the top-level package import-safe without Spark by lazily loading Spark
  integration only when Spark-backed APIs are called.
- Kept the public root API stable, including `TokenProtocol`, while moving the
  Spark-backed implementation behind a cleaner module boundary.
- Updated Databricks notebook and CLI documentation to direct Spark users to
  `spindle-token[spark]`.

### Bug Fixes

- Fixed `tokenize()`, `transcode_out()`, and `transcode_in()` so they no longer
  silently fail when `tokens` is passed as a generator or one-shot iterator.
- Fixed the CLI import path so `import spindle_token._cli` no longer requires
  Spark at module import time.

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
