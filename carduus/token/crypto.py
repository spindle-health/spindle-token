from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

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
        assert isinstance(key, rsa.RSAPublicKey)
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
        assert isinstance(key, rsa.RSAPrivateKey)
        return key.decrypt(
            bytes(message),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

    return decrypt
