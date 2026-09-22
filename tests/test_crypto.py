from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import mlkem, padding
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)
import pyspark.cloudpickle as cloudpickle

from spindle_token import _crypto


def test_deterministic_encrypter_constructs_cipher_once(monkeypatch) -> None:
    calls: list[bytes] = []

    class FakeCipher:
        def __init__(self, key: bytes):
            calls.append(key)

        def encrypt(self, nonce: bytes, message: bytes, aad):
            return b"encrypted:" + message

    monkeypatch.setattr(_crypto, "AESGCMSIV", FakeCipher)

    encrypt = _crypto.make_deterministic_encrypter(b"key-material")

    assert calls == []
    assert encrypt(bytearray(b"alpha")) == b"encrypted:alpha"
    assert encrypt(bytearray(b"beta")) == b"encrypted:beta"
    assert calls == [b"key-material"]


def test_deterministic_decrypter_constructs_cipher_once(monkeypatch) -> None:
    calls: list[bytes] = []

    class FakeCipher:
        def __init__(self, key: bytes):
            calls.append(key)

        def decrypt(self, nonce: bytes, message: bytes, aad):
            return b"decrypted:" + message

    monkeypatch.setattr(_crypto, "AESGCMSIV", FakeCipher)

    decrypt = _crypto.make_deterministic_decrypter(b"key-material")

    assert calls == []
    assert decrypt(bytearray(b"alpha")) == b"decrypted:alpha"
    assert decrypt(bytearray(b"beta")) == b"decrypted:beta"
    assert calls == [b"key-material"]


def test_deterministic_encrypter_is_picklable() -> None:
    encrypt = _crypto.make_deterministic_encrypter(b"0" * 32)
    assert encrypt(bytearray(b"before-pickle")) is not None

    round_tripped = cloudpickle.loads(cloudpickle.dumps(encrypt))
    assert round_tripped(bytearray(b"alpha")) is not None


def test_deterministic_decrypter_is_picklable() -> None:
    key = b"1" * 32
    encrypt = _crypto.make_deterministic_encrypter(key)
    decrypt = _crypto.make_deterministic_decrypter(key)

    ciphertext = encrypt(bytearray(b"alpha"))
    assert decrypt(bytearray(ciphertext)) == b"alpha"
    round_tripped = cloudpickle.loads(cloudpickle.dumps(decrypt))
    assert round_tripped(bytearray(ciphertext)) == b"alpha"


def test_asymmetric_encrypter_loads_public_key_once(monkeypatch) -> None:
    calls: list[bytes] = []

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    public_key_pem = public_key.public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )

    public_key_obj = public_key

    def fake_loader(public_key: bytes):
        calls.append(public_key)
        return public_key_obj

    monkeypatch.setattr(_crypto, "load_pem_public_key", fake_loader)

    encrypt = _crypto.make_asymmetric_encrypter(public_key_pem)

    assert calls == []
    assert isinstance(encrypt(bytearray(b"alpha")), bytes)
    assert isinstance(encrypt(bytearray(b"beta")), bytes)
    assert calls == [public_key_pem]


def test_asymmetric_decrypter_loads_private_key_once(monkeypatch) -> None:
    calls: list[bytes] = []

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    public_key = private_key.public_key()

    private_key_obj = private_key

    def fake_loader(private_key: bytes, password):
        calls.append(private_key)
        return private_key_obj

    monkeypatch.setattr(_crypto, "load_pem_private_key", fake_loader)

    decrypt = _crypto.make_asymmetric_decrypter(private_key_pem)
    ciphertext = public_key.encrypt(
        b"alpha",
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    assert calls == []
    assert decrypt(bytearray(ciphertext)) == b"alpha"
    assert decrypt(bytearray(ciphertext)) == b"alpha"
    assert calls == [private_key_pem]


def test_asymmetric_encrypter_is_picklable() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key_pem = private_key.public_key().public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )

    encrypt = _crypto.make_asymmetric_encrypter(public_key_pem)
    assert isinstance(encrypt(bytearray(b"before-pickle")), bytes)
    round_tripped = cloudpickle.loads(cloudpickle.dumps(encrypt))

    assert isinstance(round_tripped(bytearray(b"alpha")), bytes)


def test_asymmetric_decrypter_is_picklable() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    public_key = private_key.public_key()

    decrypt = _crypto.make_asymmetric_decrypter(private_key_pem)
    ciphertext = public_key.encrypt(
        b"alpha",
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    assert decrypt(bytearray(ciphertext)) == b"alpha"
    round_tripped = cloudpickle.loads(cloudpickle.dumps(decrypt))

    assert round_tripped(bytearray(ciphertext)) == b"alpha"


def test_v3_aes_key_uses_the_v3_hkdf_domain() -> None:
    seed = bytes(range(64))
    expected = HKDF(
        algorithm=hashes.SHA256(), length=32, salt=None, info=b"opprl.v3.aes"
    ).derive(seed)

    assert _crypto.derive_aes_key_v3(seed) == expected


def test_v3_kem_transcrypt_round_trips_and_uses_the_specified_envelope() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    seed = private_key.private_bytes_raw()
    public_key = private_key.public_key().public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    encrypt = _crypto.make_kem_encrypter(public_key)
    decrypt = _crypto.make_kem_decrypter(seed)
    message = b"h" * 64

    ciphertext = encrypt(bytearray(message))

    assert len(ciphertext) == 1180
    assert decrypt(bytearray(ciphertext)) == message
    assert encrypt(bytearray(message)) != ciphertext


def test_v3_kem_decrypter_rejects_invalid_envelope_length() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    decrypt = _crypto.make_kem_decrypter(private_key.private_bytes_raw())

    try:
        decrypt(bytearray(b"too short"))
    except ValueError as exc:
        assert "1180" in str(exc)
    else:
        raise AssertionError("expected invalid v3 envelope to be rejected")


def test_v3_kem_decrypter_rejects_an_empty_non_null_envelope() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    decrypt = _crypto.make_kem_decrypter(private_key.private_bytes_raw())

    try:
        decrypt(bytearray())
    except ValueError as exc:
        assert "1180" in str(exc)
    else:
        raise AssertionError("expected empty non-null envelope to be rejected")


def test_v3_kem_encrypter_rejects_non_sha512_payloads() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    encrypt = _crypto.make_kem_encrypter(public_key)

    try:
        encrypt(bytearray())
    except ValueError as exc:
        assert "64 bytes" in str(exc)
    else:
        raise AssertionError("expected a non-SHA-512 payload to be rejected")


def test_v3_kem_decrypter_rejects_modified_aes_gcm_tag() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    ciphertext = bytearray(_crypto.make_kem_encrypter(public_key)(bytearray(b"m" * 64)))
    ciphertext[-1] ^= 1
    decrypt = _crypto.make_kem_decrypter(private_key.private_bytes_raw())

    try:
        decrypt(ciphertext)
    except InvalidTag:
        pass
    else:
        raise AssertionError("expected modified AES-GCM tag to be rejected")


def test_v3_kem_helpers_are_picklable_for_spark_workers() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    encrypt = cloudpickle.loads(cloudpickle.dumps(_crypto.make_kem_encrypter(public_key)))
    decrypt = cloudpickle.loads(
        cloudpickle.dumps(_crypto.make_kem_decrypter(private_key.private_bytes_raw()))
    )

    ciphertext = encrypt(bytearray(b"s" * 64))

    assert decrypt(bytearray(ciphertext)) == b"s" * 64


def test_mlkem_private_and_public_key_environment_values_are_base64_decoded(
    monkeypatch,
) -> None:
    private_seed = bytes(range(64))
    public_key = bytes(range(256)) * 4 + bytes(range(160))
    monkeypatch.setenv("SPINDLE_TOKEN_PRIVATE_KEY", base64.b64encode(private_seed).decode())
    monkeypatch.setenv(
        "SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY", base64.b64encode(public_key).decode()
    )

    assert _crypto.private_key_from_env(raw_base64=True) == private_seed
    assert _crypto.public_key_from_env(raw_base64=True) == public_key


def test_v3_key_derivation_rejects_seed_lengths_other_than_64_bytes() -> None:
    with pytest.raises(ValueError, match="expected 64 bytes"):
        _crypto.derive_aes_key_v3(b"short")


@pytest.mark.parametrize(
    "tampered_offset",
    [0, 1088, 1100],
    ids=["kem-ciphertext", "nonce", "aes-gcm-ciphertext"],
)
def test_v3_kem_decrypter_rejects_tampering_in_every_envelope_component(
    tampered_offset: int,
) -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
    envelope = bytearray(_crypto.make_kem_encrypter(public_key)(bytearray(b"h" * 64)))
    envelope[tampered_offset] ^= 1
    decrypt = _crypto.make_kem_decrypter(private_key.private_bytes_raw())

    with pytest.raises(InvalidTag):
        decrypt(envelope)


def test_v3_kem_decrypter_rejects_an_envelope_for_another_recipient() -> None:
    recipient = mlkem.MLKEM768PrivateKey.generate()
    other_recipient = mlkem.MLKEM768PrivateKey.generate()
    public_key = recipient.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    envelope = _crypto.make_kem_encrypter(public_key)(bytearray(b"h" * 64))
    decrypt = _crypto.make_kem_decrypter(other_recipient.private_bytes_raw())

    with pytest.raises(InvalidTag):
        decrypt(bytearray(envelope))


def test_v3_kem_helpers_preserve_nulls_and_reject_non_64_byte_payloads() -> None:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    encrypt = _crypto.make_kem_encrypter(public_key)
    decrypt = _crypto.make_kem_decrypter(private_key.private_bytes_raw())

    assert encrypt(None) is None
    assert decrypt(None) is None
    for payload in (b"", b"h" * 63, b"h" * 65):
        with pytest.raises(ValueError, match="expected 64 bytes"):
            encrypt(bytearray(payload))


def test_v3_key_environment_variables_require_valid_base64(monkeypatch) -> None:
    monkeypatch.setenv("SPINDLE_TOKEN_PRIVATE_KEY", "not base64!")
    monkeypatch.setenv("SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY", "also not base64!")

    with pytest.raises(ValueError, match="PRIVATE_KEY must contain a base64"):
        _crypto.private_key_from_env(raw_base64=True)
    with pytest.raises(ValueError, match="RECIPIENT_PUBLIC_KEY must contain a base64"):
        _crypto.public_key_from_env(raw_base64=True)


def test_v3_key_environment_variables_report_missing_values(monkeypatch) -> None:
    monkeypatch.delenv("SPINDLE_TOKEN_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY", raising=False)

    with pytest.raises(ValueError, match="No ML-KEM-768 private seed"):
        _crypto.private_key_from_env(raw_base64=True)
    with pytest.raises(ValueError, match="No recipient ML-KEM-768 public key"):
        _crypto.public_key_from_env(raw_base64=True)
