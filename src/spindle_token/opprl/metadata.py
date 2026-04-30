from __future__ import annotations

from dataclasses import dataclass

from spindle_token.opprl._v2_definitions import OPPRL_V2_TOKEN_DEFINITIONS

__all__ = ["OpprlTokenMetadata", "get_opprl_v2_tokens", "get_opprl_v2_token_by_name"]


@dataclass(frozen=True)
class OpprlTokenMetadata:
    protocol: str
    version: str
    token_id: str
    name: str
    attribute_ids: tuple[str, ...]


_OPPRL_V2_TOKEN_METADATA = tuple(
    OpprlTokenMetadata(
        protocol=definition.protocol,
        version=definition.version,
        token_id=definition.token_id,
        name=definition.name,
        attribute_ids=definition.attribute_ids,
    )
    for definition in OPPRL_V2_TOKEN_DEFINITIONS
)

_OPPRL_V2_TOKEN_METADATA_BY_NAME = {
    metadata.name: metadata for metadata in _OPPRL_V2_TOKEN_METADATA
}


def get_opprl_v2_tokens() -> tuple[OpprlTokenMetadata, ...]:
    return _OPPRL_V2_TOKEN_METADATA


def get_opprl_v2_token_by_name(name: str) -> OpprlTokenMetadata | None:
    return _OPPRL_V2_TOKEN_METADATA_BY_NAME.get(name)
