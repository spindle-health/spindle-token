from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.hashes import SHAKE256, Hash

__all__ = [
    "generate_pem_keys",
    "derive_aes_key",
]


def generate_pem_keys(key_size: int = 2048) -> tuple[bytes, bytes]:
    """Generates a fresh RSA key pair.

    Arguments:
        key_size:
            The size (in bits) of the key.

    Returns:
        A tuple containing the private key and public key bytes. Both in the PEM encoding.

    """
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )
    private = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private, public


def derive_aes_key(rsa_key: bytes) -> bytes:
    """Derives the corresponding AES key from the given RSA private key.

    Arguments:
        rsa_key:
            A RSA private key.

    Returns:
        An 32 bit AES key.

    """
    digest = Hash(SHAKE256(32))
    digest.update(rsa_key)
    return digest.finalize()
