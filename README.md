# Spindle Token

<!-- @TODO Replace with spindle-token badge after publishing. -->
<!-- [![PyPI version](https://badge.fury.io/py/carduus.svg)](https://badge.fury.io/py/carduus) -->

The open source implementation of the Open Privacy Preserving Record Linkage (OPPRL) protocol build on Spark.

## Rationale

Privacy Preserving Record Linkage (PPRL) is crucial component to data de-identification systems. PPRL obfuscates identifying attributes or other sensitive information about the subjects described in the records of a dataset while still preserving the ability to link records pertaining to the same subject through the use of an encrypted token. This practice is sometimes referred to as "tokenization" and is one of the components of data de-identification.

The task of PPRL is to replace the attributes of a every record denoting Personally Identifiable Information (PII) with a token produced by a one-way cryptographic function. This prevents observers of the tokenized data from obtaining the PII. The tokens are produced deterministically such that input records with the same, or similar, PII attributes will produce an identical token. This allows practitioners to associate records across datasets that are highly likely to belong to the same data subject without having access to PII.

Tokenization is also used when data is shared between organizations to limit, or in some cases fully mitigate, the risk of subject re-identification in the event that an untrusted third party gains access to a dataset containing sensitive data. Each party produces encrypted tokens using a different secret key so that any compromised data asset is, at worst, only matchable to other datasets maintained by the same party. During data sharing transactions, a specific "transcode" data flow is used to re-encrypt the sender's tokens into ephemeral tokens that do not match tokens in any other dataset and can only be ingested using the recipients secret key. At no point in the "transcode" data flow is the original PII used.

The spindle-token is the canonical implementation of the [Open Privacy Preserving Record Linkage](https://spindle-health.github.io/carduus/opprl/) (OPPRL) protocol. This protocol presents a standardized methodology for tokenization that can be implemented in any data system to increase interoperability. The spindle-token implementation is a python library that distributes tokenization workloads using apache [Spark](https://spark.apache.org/) across multiple cores or multiple machines in a high performance computing cluster for efficient tokenization of any scale datasets.

The pre-v1.0 versions of this library were published under the name "carduus" and the deprecated APIs can be found [here](https://spindle-health.github.io/carduus/carduus/api/).

## Getting Started

See the [getting started guide](https://spindle-health.github.io/carduus/guides/getting-started/) on the project's web page for an detailed explanation of how carduus is used including example code snippets.

The full [API](https://spindle-health.github.io/carduus/api/) and an [example usage on Databricks](https://spindle-health.github.io/carduus/guides/databricks/) are also provided on the project's web page.

## Security Audit

This project has received a security audit from [Echelon Risk + Cyber](https://echeloncyber.com/) who provided the following statement. More details on this security audit can be obtained from Echelon Risk + Cyber at [this link](https://echeloncyber.com/spindle-health-security-certification-report).

 > Echelon Risk + Cyber certifies that as of May 30, 2025, The Spindle Token implementation and Open Privacy Preserving Record Linkage (OPPRL) Protocol exhibit a high degree of alignment with secure cryptographic standards and secure development practices. The use of FIPS -compliant algorithms (AES-GCM-SIV, RSA-OAEP, SHA2 family), layered encryption, and privacy preserving design patterns indicate strong foundational security. Note: This certification is issued in good faith, based on the materials available to the Echelon team at the time of the review.

## Contributing

Please refer to the spindle-token [contributing guide](https://spindle-health.github.io/carduus/CONTRIBUTING/) for information on how to get started contributing to the project.

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
