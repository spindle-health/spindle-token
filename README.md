# Spindle Token

[![PyPI version](https://badge.fury.io/py/spindle-token.svg)](https://badge.fury.io/py/spindle-token)

The open source implementation of the Open Privacy Preserving Record Linkage (OPPRL) protocol build on Spark.

## Rationale

Privacy Preserving Record Linkage (PPRL) is crucial component to data de-identification systems. PPRL obfuscates identifying attributes or other sensitive information about the subjects described in the records of a dataset while still preserving the ability to link records pertaining to the same subject through the use of an encrypted token. This practice is sometimes referred to as "tokenization" and is one of the components of data de-identification.

The task of PPRL is to replace the attributes of a every record denoting Personally Identifiable Information (PII) with a token produced by a one-way cryptographic function. This prevents observers of the tokenized data from obtaining the PII. The tokens are produced deterministically such that input records with the same, or similar, PII attributes will produce an identical token. This allows practitioners to associate records across datasets that are highly likely to belong to the same data subject without having access to PII.

Tokenization is also used when data is shared between organizations to limit, or in some cases fully mitigate, the risk of subject re-identification in the event that an untrusted third party gains access to a dataset containing sensitive data. Each party produces encrypted tokens using a different secret key so that any compromised data asset is, at worst, only matchable to other datasets maintained by the same party. During data sharing transactions, a specific "transcode" data flow is used to re-encrypt the sender's tokens into ephemeral tokens that do not match tokens in any other dataset and can only be ingested using the recipients secret key. At no point in the "transcode" data flow is the original PII used.

The spindle-token is the canonical implementation of the [Open Privacy Preserving Record Linkage](https://token.spindlehealth.com/opprl/PROTOCOL/) (OPPRL) protocol. This protocol presents a standardized methodology for tokenization that can be implemented in any data system to increase interoperability. The spindle-token implementation is a python library that distributes tokenization workloads using apache [Spark](https://spark.apache.org/) across multiple cores or multiple machines in a high performance computing cluster for efficient tokenization of any scale datasets.

The pre-v1.0 versions of this library were published under the name "carduus" and the deprecated APIs can be found [here](https://token.spindlehealth.com/carduus/api/).

## Getting Started

See the [getting started guide](https://token.spindlehealth.com/guides/getting-started/) on the project's web page for an detailed explanation of how spindle-token is used including example code snippets.

The full [API](https://token.spindlehealth.com/api/) and an [example usage on Databricks](https://token.spindlehealth.com/guides/databricks/) are also provided on the project's web page.

## Migrating from OPPRL V1 to V2

New integrations should use `OpprlV2`. The current examples in this repository
use V2 unless they are explicitly historical.

If you already have code that imports `OpprlV1`, the migration path is usually a
small mechanical change:

1. Replace `OpprlV1` imports with `OpprlV2`.
2. Keep the same attribute mapping and token selection.
3. Re-run your tokenization and transcode tests against your existing data.

`OpprlV2` preserves the V1 token structure and normalization rules, but it
canonicalizes the private key before deriving the AES key. That means V2 is the
right choice when you want the same logical protocol behavior with stable
results across equivalent PEM encodings of the same RSA key.

If you need byte-for-byte compatibility with historical V1 token outputs, keep
using `OpprlV1` for those flows.

## Independent Security Review

Spindle engaged [Echelon Risk + Cyber](https://echeloncyber.com/), an independent, leading cybersecurity firm, to conduct an audit of the Spindle Token implementation. The review evaluated and confirmed alignment with industry best practices in secure software development, cryptographic algorithm selection, and tokenization methods.

[View the full report](https://echeloncyber.com/spindle-health-security-certification-report).

This report reflects Echelon Risk + Cyber’s independent professional opinion as of the date of review and does not constitute a warranty, certification, or guarantee of future performance or security. The scope of the assessment was limited to the artifacts, documentation, and workshop discussions reviewed through May 30, 2025, and does not extend to undisclosed code, subsequent modifications, or third-party dependencies.

## Contributing

Please refer to the spindle-token [contributing guide](https://token.spindlehealth.com/CONTRIBUTING/) for information on how to get started contributing to the project.

### Organizations that have contributed to spindle-token

<a href="https://spindlehealth.com/">
    <img
        src="assets/spindle_health.png"
        alt="Spindle Health"
        style="height: 3rem"
    />
</a>
<br/>
<a href="https://echeloncyber.com/">
    <img
        src="assets/echelon_risk_cyber.png"
        alt="Echelon Risk + Cyber"
        style="height: 3rem"
    />
</a>

### Individuals that have contributed to spindle-token

[Brian Fallik](https://github.com/bfallik) - @bfallik
