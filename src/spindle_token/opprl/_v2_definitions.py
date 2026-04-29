from __future__ import annotations

from dataclasses import dataclass

__all__ = ["OpprlTokenDefinition", "OPPRL_V2_TOKEN_DEFINITIONS", "get_opprl_v2_definition"]


@dataclass(frozen=True)
class OpprlTokenDefinition:
    protocol: str
    version: str
    token_id: str
    name: str
    attribute_ids: tuple[str, ...]


OPPRL_V2_TOKEN_DEFINITIONS: tuple[OpprlTokenDefinition, ...] = (
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token1",
        name="opprl_token_1v2",
        attribute_ids=(
            "opprl.v2.first.initial",
            "opprl.v2.last",
            "opprl.v2.gender",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token2",
        name="opprl_token_2v2",
        attribute_ids=(
            "opprl.v2.first.soundex",
            "opprl.v2.last.soundex",
            "opprl.v2.gender",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token3",
        name="opprl_token_3v2",
        attribute_ids=(
            "opprl.v2.first.metaphone",
            "opprl.v2.last.metaphone",
            "opprl.v2.gender",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token4",
        name="opprl_token_4v2",
        attribute_ids=(
            "opprl.v2.first.initial",
            "opprl.v2.last",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token5",
        name="opprl_token_5v2",
        attribute_ids=(
            "opprl.v2.first.soundex",
            "opprl.v2.last.soundex",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token6",
        name="opprl_token_6v2",
        attribute_ids=(
            "opprl.v2.first.metaphone",
            "opprl.v2.last.metaphone",
            "opprl.v2.birth_date",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token7",
        name="opprl_token_7v2",
        attribute_ids=(
            "opprl.v2.first",
            "opprl.v2.phone",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token8",
        name="opprl_token_8v2",
        attribute_ids=(
            "opprl.v2.birth_date",
            "opprl.v2.phone",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token9",
        name="opprl_token_9v2",
        attribute_ids=(
            "opprl.v2.first",
            "opprl.v2.ssn",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token10",
        name="opprl_token_10v2",
        attribute_ids=(
            "opprl.v2.birth_date",
            "opprl.v2.ssn",
        ),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token11",
        name="opprl_token_11v2",
        attribute_ids=("opprl.v2.email",),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token12",
        name="opprl_token_12v2",
        attribute_ids=("opprl.v2.email.sha2",),
    ),
    OpprlTokenDefinition(
        protocol="opprl",
        version="v2",
        token_id="token13",
        name="opprl_token_13v2",
        attribute_ids=(
            "opprl.v2.group_number",
            "opprl.v2.member_id",
        ),
    ),
)

_OPPRL_V2_DEFINITIONS_BY_ID = {
    definition.token_id: definition for definition in OPPRL_V2_TOKEN_DEFINITIONS
}


def get_opprl_v2_definition(token_id: str) -> OpprlTokenDefinition:
    return _OPPRL_V2_DEFINITIONS_BY_ID[token_id]
