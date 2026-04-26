import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)

# For tokenizing PII, We use the same nonce to encrypt
# The AES GCM SIV encryption scheme does NOT fail catastrophically when a nonce is
# reused, however an attacker can use a chosen-plaintext attack to attack specific
# records. https://en.wikipedia.org/wiki/Chosen-plaintext_attack
#
# When transferring data between parties we use RSA with OAEP so that datasets intercepted
# by untrusted third parties cannot exploit this vulnerability.

DEFAULT_NONCE = bytes([0] * 12)


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
        if self._key is None:
            key = load_pem_public_key(self._public_key)
            if not isinstance(key, rsa.RSAPublicKey):
                raise TypeError(f"Incorrect key type. Expected RSA public key, got {type(key)}.")
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


def make_deterministic_encrypter(key: bytes):
    return _DeterministicEncrypter(key)


def make_deterministic_decrypter(key: bytes):
    return _DeterministicDecrypter(key)


def make_asymmetric_encrypter(public_key: bytes):
    return _AsymmetricEncrypter(public_key)


def make_asymmetric_decrypter(private_key: bytes):
    return _AsymmetricDecrypter(private_key)


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


_PRIVATE_KEY_ENV_VAR = "SPINDLE_TOKEN_PRIVATE_KEY"
_RECIPIENT_PUBLIC_KEY_ENV_VAR = "SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY"


def private_key_from_env() -> bytes:
    if _PRIVATE_KEY_ENV_VAR not in os.environ:
        raise ValueError(
            f"No private RSA key found. Set the {_PRIVATE_KEY_ENV_VAR} environment variable or pass the key as an argument."
        )
    return os.environ[_PRIVATE_KEY_ENV_VAR].encode()


def public_key_from_env() -> bytes:
    if _RECIPIENT_PUBLIC_KEY_ENV_VAR not in os.environ:
        raise ValueError(
            f"No public RSA key set. Set the {_RECIPIENT_PUBLIC_KEY_ENV_VAR} environment variable or pass the key as an argument."
        )
    return os.environ[_RECIPIENT_PUBLIC_KEY_ENV_VAR].encode()
