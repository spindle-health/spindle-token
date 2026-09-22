from __future__ import annotations

import base64
from types import SimpleNamespace

import pytest
import spindle_token._spark as spark_api
import spindle_token.opprl.v3 as v3_module
from cryptography.hazmat.primitives.asymmetric import mlkem
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from spindle_token.opprl._common import IdentityAttribute
from spindle_token.opprl import OpprlV3


class _FakeColumn:
    def __init__(self, name: str):
        self.name = name

    def alias(self, name: str) -> _FakeColumn:
        return _FakeColumn(name)


class _FakeDataFrame:
    def __init__(self, columns: list[str], schema: dict | None = None):
        self.columns = columns
        self.schema = schema or {}

    def select(self, *expressions: _FakeColumn) -> _FakeDataFrame:
        columns = []
        for expression in expressions:
            if expression.name == "*":
                columns.extend(self.columns)
            else:
                columns.append(expression.name)
        return _FakeDataFrame(columns, self.schema)

    def drop(self, *names: str) -> _FakeDataFrame:
        return _FakeDataFrame(
            [column for column in self.columns if column not in names], self.schema
        )

    def withColumns(self, columns: dict[str, _FakeColumn]) -> dict[str, _FakeColumn]:
        return columns


def _keypair() -> tuple[bytes, bytes]:
    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return private_key.private_bytes_raw(), public_key


def test_v3_tokenize_dispatch_uses_base64_seed_without_rsa_parsing(monkeypatch) -> None:
    private_seed, _ = _keypair()
    monkeypatch.setenv("SPINDLE_TOKEN_PRIVATE_KEY", base64.b64encode(private_seed).decode())
    monkeypatch.setattr(v3_module, "udf", lambda function, returnType: function)
    monkeypatch.setattr(spark_api, "col", _FakeColumn)
    monkeypatch.setattr(
        v3_module.v0,
        "_tokenize_impl",
        lambda attribute_ids, encrypt: _FakeColumn("computed_token"),
    )
    monkeypatch.setattr(
        spark_api,
        "load_pem_private_key",
        lambda *args: (_ for _ in ()).throw(AssertionError("RSA parsing must be skipped")),
    )

    attribute = IdentityAttribute(OpprlV3.email.attr_id)
    frame = _FakeDataFrame(["email"], {"email": SimpleNamespace(dataType="string")})
    result = spark_api.tokenize(
        frame,
        {attribute: "email"},
        [OpprlV3.token11],
    )

    assert result.columns == ["email", OpprlV3.token11.name]


def test_v3_transcrypt_dispatch_uses_base64_keys_and_skips_rsa_parsing(monkeypatch) -> None:
    private_seed, _ = _keypair()
    _, recipient_public_key = _keypair()
    monkeypatch.setenv("SPINDLE_TOKEN_PRIVATE_KEY", base64.b64encode(private_seed).decode())
    monkeypatch.setenv(
        "SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY", base64.b64encode(recipient_public_key).decode()
    )
    monkeypatch.setattr(v3_module, "udf", lambda function, returnType: function)
    monkeypatch.setattr(spark_api, "col", _FakeColumn)
    monkeypatch.setattr(
        v3_module.v0,
        "_transcrypt_out_impl",
        lambda token, decrypt, encrypt: _FakeColumn("ephemeral_token"),
    )
    monkeypatch.setattr(
        v3_module.v0,
        "_transcrypt_in_impl",
        lambda token, decrypt, encrypt: _FakeColumn("recipient_token"),
    )
    monkeypatch.setattr(
        spark_api,
        "load_pem_private_key",
        lambda *args: (_ for _ in ()).throw(AssertionError("RSA parsing must be skipped")),
    )
    monkeypatch.setattr(
        spark_api,
        "load_pem_public_key",
        lambda *args: (_ for _ in ()).throw(AssertionError("RSA parsing must be skipped")),
    )
    frame = _FakeDataFrame([OpprlV3.token11.name])

    ephemeral = spark_api.transcrypt_out(
        frame,
        [OpprlV3.token11],
        recipient_public_key=b"",
        private_key=b"",
    )
    restored = spark_api.transcrypt_in(frame, [OpprlV3.token11], private_key=None)

    assert ephemeral[OpprlV3.token11.name].name == "ephemeral_token"
    assert restored[OpprlV3.token11.name].name == "recipient_token"


def test_v3_public_api_rejects_mixed_versions_before_loading_keys(monkeypatch) -> None:
    from spindle_token.opprl import OpprlV2

    monkeypatch.delenv("SPINDLE_TOKEN_PRIVATE_KEY", raising=False)

    with pytest.raises(ValueError, match="cannot mix"):
        spark_api.transcrypt_in(
            _FakeDataFrame([OpprlV3.token1.name, OpprlV2.token1.name]),
            [OpprlV3.token1, OpprlV2.token1],
        )
