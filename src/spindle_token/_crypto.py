import base64
import binascii
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import mlkem, padding, rsa
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, AESGCMSIV
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)

# For tokenizing PII, We use the same nonce to encrypt
# The AES GCM SIV encryption scheme does NOT fail catastrophically when a nonce is
# reused, however an attacker can use a chosen-plaintext attack to attack specific
# records. https://en.wikipedia.org/wiki/Chosen-plaintext_attack
#
# When transferring data between parties, v1/v2 use RSA-OAEP, while v3
# uses ML-KEM-768 with AES-GCM so intercepted datasets cannot exploit this vulnerability.

DEFAULT_NONCE = bytes([0] * 12)
_ML_KEM_768_CIPHERTEXT_SIZE = 1088
_V3_TRANSCRYPT_NONCE_SIZE = 12
_V3_TRANSCRYPT_ENVELOPE_SIZE = 1180
_V3_TRANSCRYPT_INFO = b"opprl.v3.transcrypt.aesgcm"
_V3_TRANSCRYPT_AAD = b"opprl.v3.transcrypt"


class _DeterministicEncrypter:

    def __init__(self, key: bytes):
        self._key = key
        self._cipher = None

    def __getstate__(self):
        return {"key": self._key}

    def __setstate__(self, state):
        self._key = state["key"]
        self._cipher = None

    def _get_cipher(self):
        # Cipher is cached for the lifetime of this instance. Key rotation requires a new instance.
        if self._cipher is None:
            self._cipher = AESGCMSIV(self._key)
        return self._cipher

    def __call__(self, message: bytearray | None) -> bytes | None:
        if not message:
            return None
        return self._get_cipher().encrypt(DEFAULT_NONCE, bytes(message), None)


class _DeterministicDecrypter(_DeterministicEncrypter):

    def __call__(self, message: bytearray | None) -> bytes | None:
        if not message:
            return None
        return self._get_cipher().decrypt(DEFAULT_NONCE, bytes(message), None)


class _AsymmetricEncrypter:

    def __init__(self, public_key: bytes):
        self._public_key = public_key
        self._key = None

    def __getstate__(self):
        return {"public_key": self._public_key}

    def __setstate__(self, state):
        self._public_key = state["public_key"]
        self._key = None

    def _get_key(self):
        # Key is cached for the lifetime of this instance. Key rotation requires a new instance.
        if self._key is None:
            key = load_pem_public_key(self._public_key)
            if not isinstance(key, rsa.RSAPublicKey):
                raise TypeError(
                    f"Incorrect key type. Expected RSA public key, got {type(key)}."
                )
            self._key = key
        return self._key

    def __call__(self, message: bytearray | None) -> bytes | None:
        if not message:
            return None
        return self._get_key().encrypt(
            bytes(message),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )


class _AsymmetricDecrypter:

    def __init__(self, private_key: bytes):
        self._private_key = private_key
        self._key = None

    def __getstate__(self):
        return {"private_key": self._private_key}

    def __setstate__(self, state):
        self._private_key = state["private_key"]
        self._key = None

    def _get_key(self):
        # Key is cached for the lifetime of this instance. Key rotation requires a new instance.
        if self._key is None:
            key = load_pem_private_key(self._private_key, None)
            if not isinstance(key, rsa.RSAPrivateKey):
                raise TypeError(
                    f"Incorrect key type. Expected RSA private key, got {type(key)}."
                )
            self._key = key
        return self._key

    def __call__(self, message: bytearray | None) -> bytes | None:
        if not message:
            return None
        return self._get_key().decrypt(
            bytes(message),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )


class _KemEncrypter:

    def __init__(self, public_key: bytes):
        self._public_key = bytes(public_key)
        self._key = None

    def __getstate__(self):
        return {"public_key": self._public_key}

    def __setstate__(self, state):
        self._public_key = state["public_key"]
        self._key = None

    def _get_key(self):
        if self._key is None:
            self._key = mlkem.MLKEM768PublicKey.from_public_bytes(self._public_key)
        return self._key

    def __call__(self, message: bytearray | None) -> bytes | None:
        if message is None:
            return None

        plaintext = bytes(message)
        if len(plaintext) != 64:
            raise ValueError(
                "Invalid v3 transcrypt payload length: expected 64 bytes, "
                f"got {len(plaintext)}"
            )

        shared_secret, kem_ciphertext = self._get_key().encapsulate()
        aes_key = _derive_v3_transcrypt_key(shared_secret)
        nonce = os.urandom(_V3_TRANSCRYPT_NONCE_SIZE)
        ciphertext = AESGCM(aes_key).encrypt(nonce, plaintext, _V3_TRANSCRYPT_AAD)
        return kem_ciphertext + nonce + ciphertext


class _KemDecrypter:

    def __init__(self, private_key_seed: bytes):
        self._private_key_seed = bytes(private_key_seed)
        self._key = None

    def __getstate__(self):
        return {"private_key_seed": self._private_key_seed}

    def __setstate__(self, state):
        self._private_key_seed = state["private_key_seed"]
        self._key = None

    def _get_key(self):
        if self._key is None:
            self._key = mlkem.MLKEM768PrivateKey.from_seed_bytes(self._private_key_seed)
        return self._key

    def __call__(self, message: bytearray | None) -> bytes | None:
        if message is None:
            return None

        envelope = bytes(message)
        if len(envelope) != _V3_TRANSCRYPT_ENVELOPE_SIZE:
            raise ValueError(
                "Invalid v3 ephemeral token length: expected "
                f"{_V3_TRANSCRYPT_ENVELOPE_SIZE} bytes, got {len(envelope)}"
            )

        kem_ciphertext = envelope[:_ML_KEM_768_CIPHERTEXT_SIZE]
        nonce_end = _ML_KEM_768_CIPHERTEXT_SIZE + _V3_TRANSCRYPT_NONCE_SIZE
        nonce = envelope[_ML_KEM_768_CIPHERTEXT_SIZE:nonce_end]
        ciphertext = envelope[nonce_end:]

        shared_secret = self._get_key().decapsulate(kem_ciphertext)
        aes_key = _derive_v3_transcrypt_key(shared_secret)
        return AESGCM(aes_key).decrypt(nonce, ciphertext, _V3_TRANSCRYPT_AAD)


def _derive_v3_transcrypt_key(shared_secret: bytes) -> bytes:
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=_V3_TRANSCRYPT_INFO,
    )
    return hkdf.derive(shared_secret)


def make_deterministic_encrypter(key: bytes):
    return _DeterministicEncrypter(key)


def make_deterministic_decrypter(key: bytes):
    return _DeterministicDecrypter(key)


def make_asymmetric_encrypter(public_key: bytes):
    return _AsymmetricEncrypter(public_key)


def make_asymmetric_decrypter(private_key: bytes):
    return _AsymmetricDecrypter(private_key)


def make_kem_encrypter(public_key: bytes):
    return _KemEncrypter(public_key)


def make_kem_decrypter(private_key_seed: bytes):
    return _KemDecrypter(private_key_seed)


def derive_aes_key(rsa_key: bytes) -> bytes:
    """Derives the corresponding AES key from the given RSA private key.

    Arguments:
        rsa_key:
            A RSA private key.

    Returns:
        An 32 byte AES key.

    """
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"opprl.v1.aes")
    return hkdf.derive(rsa_key)


def derive_aes_key_v3(private_key_seed: bytes) -> bytes:
    """Derives the v3 tokenization AES key from an ML-KEM-768 private seed."""
    if len(private_key_seed) != 64:
        raise ValueError(
            f"Invalid ML-KEM-768 private seed length: expected 64 bytes, got {len(private_key_seed)}"
        )

    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"opprl.v3.aes")
    return hkdf.derive(private_key_seed)


_PRIVATE_KEY_ENV_VAR = "SPINDLE_TOKEN_PRIVATE_KEY"
_RECIPIENT_PUBLIC_KEY_ENV_VAR = "SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY"


def private_key_from_env(raw_base64: bool = False) -> bytes:
    if _PRIVATE_KEY_ENV_VAR not in os.environ:
        if raw_base64:
            raise ValueError(
                "No ML-KEM-768 private seed found. Set "
                f"{_PRIVATE_KEY_ENV_VAR} to its base64-encoded value."
            )
        raise ValueError(
            f"No private RSA key found. Set the {_PRIVATE_KEY_ENV_VAR} environment variable or pass the key as an argument."
        )
    value = os.environ[_PRIVATE_KEY_ENV_VAR]
    if raw_base64:
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(
                f"{_PRIVATE_KEY_ENV_VAR} must contain a base64-encoded ML-KEM-768 seed."
            ) from exc
    return value.encode()


def public_key_from_env(raw_base64: bool = False) -> bytes:
    if _RECIPIENT_PUBLIC_KEY_ENV_VAR not in os.environ:
        if raw_base64:
            raise ValueError(
                "No recipient ML-KEM-768 public key found. Set "
                f"{_RECIPIENT_PUBLIC_KEY_ENV_VAR} to its base64-encoded value."
            )
        raise ValueError(
            f"No public RSA key set. Set the {_RECIPIENT_PUBLIC_KEY_ENV_VAR} environment variable or pass the key as an argument."
        )
    value = os.environ[_RECIPIENT_PUBLIC_KEY_ENV_VAR]
    if raw_base64:
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(
                f"{_RECIPIENT_PUBLIC_KEY_ENV_VAR} must contain a base64-encoded ML-KEM-768 public key."
            ) from exc
    return value.encode()
