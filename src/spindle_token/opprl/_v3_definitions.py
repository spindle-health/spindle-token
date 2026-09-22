from __future__ import annotations

from dataclasses import dataclass

__all__ = ["OpprlV3TokenDefinition", "OPPRL_V3_TOKEN_DEFINITIONS", "get_opprl_v3_definition"]


@dataclass(frozen=True)
class OpprlV3TokenDefinition:
    protocol: str
    version: str
    token_id: str
    name: str
    attribute_ids: tuple[str, ...]


_TOKEN_ATTRIBUTE_SUFFIXES = (
    ("first.initial", "last", "gender", "birth_date"),
    ("first.soundex", "last.soundex", "gender", "birth_date"),
    ("first.metaphone", "last.metaphone", "gender", "birth_date"),
    ("first.initial", "last", "birth_date"),
    ("first.soundex", "last.soundex", "birth_date"),
    ("first.metaphone", "last.metaphone", "birth_date"),
    ("first", "phone"),
    ("birth_date", "phone"),
    ("first", "ssn"),
    ("birth_date", "ssn"),
    ("email",),
    ("email.sha2",),
    ("group_number", "member_id"),
)

OPPRL_V3_TOKEN_DEFINITIONS = tuple(
    OpprlV3TokenDefinition(
        protocol="opprl",
        version="v3",
        token_id=f"token{token_number}",
        name=f"opprl_token_{token_number}v3",
        attribute_ids=tuple(f"opprl.v3.{suffix}" for suffix in suffixes),
    )
    for token_number, suffixes in enumerate(_TOKEN_ATTRIBUTE_SUFFIXES, start=1)
)

_OPPRL_V3_DEFINITIONS_BY_ID = {
    definition.token_id: definition for definition in OPPRL_V3_TOKEN_DEFINITIONS
}


def get_opprl_v3_definition(token_id: str) -> OpprlV3TokenDefinition:
    return _OPPRL_V3_DEFINITIONS_BY_ID[token_id]
