from __future__ import annotations

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa
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
