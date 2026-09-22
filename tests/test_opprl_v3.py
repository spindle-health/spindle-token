from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives.asymmetric import mlkem
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from pyspark.sql import Row
from pyspark.testing.utils import assertDataFrameEqual

from spindle_token import tokenize, transcrypt_in, transcrypt_out


def _mlkem_keypair() -> tuple[bytes, bytes]:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    return (
        private_key.private_bytes_raw(),
        private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw),
    )


def test_v3_opprl_api_is_exported() -> None:
    from spindle_token.opprl import OpprlV3

    assert OpprlV3.token1.name == "opprl_token_1v3"
    assert OpprlV3.protocol.factory_id == "opprl.v3"


def test_v3_protocol_routes_tokenize_and_transcrypt_calls(monkeypatch) -> None:
    import spindle_token.opprl.v3 as v3_module
    from spindle_token.opprl import OpprlV3

    private_seed, public_key = _mlkem_keypair()
    monkeypatch.setattr(v3_module, "udf", lambda function, returnType: function)
    monkeypatch.setattr(
        v3_module.v0,
        "_tokenize_impl",
        lambda attribute_ids, encrypt: ("tokenize", attribute_ids, encrypt),
    )
    monkeypatch.setattr(
        v3_module.v0,
        "_transcrypt_out_impl",
        lambda token, decrypt, encrypt: ("out", token, decrypt, encrypt),
    )
    monkeypatch.setattr(
        v3_module.v0,
        "_transcrypt_in_impl",
        lambda token, decrypt, encrypt: ("in", token, decrypt, encrypt),
    )

    protocol = OpprlV3.protocol.bind(private_seed, public_key)

    tokenization = protocol.tokenize(["attribute"])
    assert tokenization == ("tokenize", ["attribute"], protocol.encrypt_aes)
    assert protocol.transcrypt_out("token") == (
        "out",
        "token",
        protocol.decrypt_aes,
        protocol.encrypt_kem,
    )
    assert protocol.transcrypt_in("ephemeral") == (
        "in",
        "ephemeral",
        protocol.decrypt_kem,
        protocol.encrypt_aes,
    )
    assert protocol.transcode_out("token") == protocol.transcrypt_out("token")
    assert protocol.transcode_in("ephemeral") == protocol.transcrypt_in("ephemeral")


def test_v3_protocol_rejects_invalid_keys_and_missing_recipient_key(monkeypatch) -> None:
    import spindle_token.opprl.v3 as v3_module
    from spindle_token.opprl import OpprlV3

    private_seed, public_key = _mlkem_keypair()
    monkeypatch.setattr(v3_module, "udf", lambda function, returnType: function)

    with pytest.raises(ValueError):
        OpprlV3.protocol.bind(private_seed[:-1], public_key)
    with pytest.raises(ValueError):
        OpprlV3.protocol.bind(private_seed, public_key[:-1])

    protocol = OpprlV3.protocol.bind(private_seed, None)
    with pytest.raises(ValueError, match="No recipient ML-KEM-768 public key"):
        protocol.transcrypt_out("token")


def test_v3_token_definitions_preserve_v2_attribute_composition() -> None:
    from spindle_token.opprl import OpprlV2, OpprlV3

    for token_number in range(1, 14):
        v2_token = getattr(OpprlV2, f"token{token_number}")
        v3_token = getattr(OpprlV3, f"token{token_number}")
        expected_attributes = tuple(
            attribute_id.replace("opprl.v2.", "opprl.v3.")
            for attribute_id in v2_token.attribute_ids
        )

        assert v3_token.name == f"opprl_token_{token_number}v3"
        assert tuple(v3_token.attribute_ids) == expected_attributes


def test_v3_and_rsa_protocols_cannot_share_a_key_operation() -> None:
    from spindle_token.opprl import OpprlV2, OpprlV3
    from spindle_token._spark import _uses_mlkem_keys

    assert _uses_mlkem_keys([OpprlV3.token1])
    assert not _uses_mlkem_keys([OpprlV2.token1])
    with pytest.raises(ValueError, match="cannot mix"):
        _uses_mlkem_keys([OpprlV2.token1, OpprlV3.token1])


def test_v3_tokenization_and_transcryption_use_raw_mlkem_keys(spark) -> None:
    from spindle_token.opprl import OpprlV3

    sender_seed, _ = _mlkem_keypair()
    recipient_seed, recipient_public_key = _mlkem_keypair()
    token = OpprlV3.token1
    mapping = {
        OpprlV3.first_name: "first_name",
        OpprlV3.last_name: "last_name",
        OpprlV3.gender: "gender",
        OpprlV3.birth_date: "birth_date",
    }
    pii = spark.createDataFrame(
        [
            Row(
                first_name="Ada",
                last_name="Lovelace",
                gender="F",
                birth_date="1815-12-10",
            ),
            Row(
                first_name="Ada",
                last_name="Lovelace",
                gender="F",
                birth_date="1815-12-10",
            ),
        ]
    )

    sender_tokens = tokenize(pii, mapping, [token], private_key=sender_seed)
    ephemeral = transcrypt_out(
        sender_tokens,
        [token],
        recipient_public_key=recipient_public_key,
        private_key=sender_seed,
    )
    wire_token = ephemeral.first()[token.name]
    recipient_tokens = transcrypt_in(ephemeral, [token], private_key=recipient_seed)
    expected_tokens = tokenize(pii, mapping, [token], private_key=recipient_seed)

    assert token.name == "opprl_token_1v3"
    assert token.protocol.factory_id == "opprl.v3"
    assert len(base64.b64decode(wire_token)) == 1180
    assert sender_tokens.first()[token.name] != expected_tokens.first()[token.name]
    assertDataFrameEqual(recipient_tokens, expected_tokens)
