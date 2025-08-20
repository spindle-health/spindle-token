"""A module of classes that provide standard configuration for different versions of the OPPRL protocol.

Each class is made up of class variables holding all instances of [PiiAttributes][spindle_token.core.PiiAttribute],
[Token][spindle_token.core.Token], and [TokenProtocolFactory][spindle_token.core.TokenProtocolFactory] that make up
the complete spec of the corresponding OPPRL version.

"""

from spindle_token.opprl.v0 import OpprlV0
from spindle_token.opprl.v1 import OpprlV1

__all__ = ["OpprlV0", "OpprlV1"]
