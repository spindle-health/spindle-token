import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


# For tokenizing PII, We use the same nonce to encrypt
# The AES GCM SIV encryption scheme does NOT fail catastrophically when a nonce is
# reused, however an attacker can use a chosen-plaintext attack to attack specific
# records. https://en.wikipedia.org/wiki/Chosen-plaintext_attack
#
# When transferring data between parties we use RSA with OAEP so that datasets intercepted
# by untrusted third parties cannot exploit this vulnerability.

DEFAULT_NONCE = bytes([0] * 12)


def make_deterministic_encrypter(key: bytes):
    def encrypt(message: bytearray | None) -> bytes | None:
        if not message:
            return None
        from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV

        return AESGCMSIV(key).encrypt(DEFAULT_NONCE, bytes(message), None)

    return encrypt


def make_deterministic_decrypter(key: bytes):
    def decrypt(message: bytearray | None) -> bytes | None:
        if not message:
            return None
        from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV

        return AESGCMSIV(key).decrypt(DEFAULT_NONCE, bytes(message), None)

    return decrypt


def make_asymmetric_encrypter(public_key: bytes):
    def encrypt(message: bytearray | None) -> bytes | None:
        if not message:
            return None
        from cryptography.hazmat.primitives.serialization import load_pem_public_key

        key = load_pem_public_key(public_key)
        if not isinstance(key, rsa.RSAPublicKey):
            raise TypeError(f"Incorrect key type. Expected RSA public key, got {type(key)}.")
        return key.encrypt(
            bytes(message),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

    return encrypt


def make_asymmetric_decrypter(private_key: bytes):
    def decrypt(message: bytearray | None) -> bytes | None:
        if not message:
            return None
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        key = load_pem_private_key(private_key, None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise TypeError(f"Incorrect key type. Expected RSA private key, got {type(key)}.")
        return key.decrypt(
            bytes(message),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

    return decrypt


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
