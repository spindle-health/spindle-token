from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Generic, TypeVar
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DataType


__all__ = ["PiiAttribute", "Token", "TokenProtocol", "TokenProtocolFactory"]


class PiiAttribute(ABC):
    """An attribute (aka column) of personally identifiable information (PII) to use when constructing tokens.

    This abstract base class is intended to be extended by users to add support for building tokens from a custom PII attribute.

    Attributes:
        attr_id: An identifier for the PiiAttribute. Should be unique across all logically different PiiAttributes.
    """

    def __init__(self, attr_id: str):
        """Initializes the PiiAttribute with the given globally unique attribute ID."""
        self.attr_id = attr_id

    @abstractmethod
    def transform(self, column: Column, dtype: DataType) -> Column:
        """Transforms the raw PII column into a normalized representation.

        A normalized value has eliminated all representation or encoding differences so all instances of the
        same logical values have identical physical values. For example, text attributes will often be normalized
        by filtering to alpha-numeric characters and whitespace, standardizing all whitespace to the space character,
        and converting all alpha characters to uppercase to ensure that all ways of representing the same phrase
        normalize to the exact same string.

        Arguments:
            column:
                The spark `Column` expression for the PII attribute being normalized.
            dtype:
                The spark `DataType` object of the `column` object found on the `DataFrame` being normalized.
                Can be used to delegate to different normalization logic based on different schemas of input data.
                For example, a subject's birth date may be a `DateType`, `StringType`, or `LongType` on input data and
                thus requires corresponding normalization into a `DateType`.

        Returns:
            A pyspark Column expression of normalized PII values.
        """
        pass

    def derivatives(self) -> dict[str, "PiiAttribute"]:
        """A collection of PII attributes that can be derived from this PII attribute, including this PiiAttribute.

        Returns:
            A `dict` with globally unique (typically namespaced) attribute IDs as the key. Values are instances of
            [PiiAttribute][spindle_token.core.PiiAttribute] that produce normalized values for each derivative attribute.
        """
        return {self.attr_id: self}

    def __repr__(self):
        return f"{type(self).__qualname__}({self.attr_id})"

    def __hash__(self):
        return self.__repr__().__hash__()


class TokenProtocol(ABC):
    """An abstract base class for a specific version of the OPPRL tokenization protocol.

    This abstract base class is intended to be extended by users who want to implement custom tokenization protocols.

    It is assumed that instances of the `TokenProtocol` will provide any configuration or other inputs
    (such as encryption keys) required to produce tokens. See [`TokenProtocolFactory`][spindle_token.core.TokenProtocolFactory]
    for more information.

    """

    @abstractmethod
    def tokenize(
        self,
        df: DataFrame,
        col_mapping: Mapping[PiiAttribute, str],
        attributes: Iterable[PiiAttribute],
    ) -> Column: ...

    """Creates a Column expression for a single token.

    Arguments:
        df:
            The pyspark `DataFrame` containing all PII attributes.
        col_mapping:
            A dictionary that maps instances of [`PiiAttribute`][spindle_token.core.PiiAttribute] to the corresponding
            column name of `df`. 
        attributes:
            A collection [`PiiAttribute`][spindle_token.core.PiiAttribute] instances to derive from `df`, normalize, and tokenize.

    Returns:
        A pyspark `Column` expression representing token values. 
    
    """

    @abstractmethod
    def transcode_out(self, token: Column) -> Column:
        """Transcodes the given token into an ephemeral token.

        Arguments:
            token:
                A pyspark `Column` of tokens.

        Returns:
            A pyspark `Column` expression of ephemeral tokens created from the input tokens.
        """
        ...

    @abstractmethod
    def transcode_in(self, ephemeral_token: Column) -> Column:
        """Transcodes the given ephemeral token into a normal token.

        Arguments:
            ephemeral_token:
                A pyspark `Column` of ephemeral tokens.

        Returns:
            A pyspark `Column` expression of tokens created from the input ephemeral tokens.
        """
        ...


P = TypeVar("P", bound=TokenProtocol)


class TokenProtocolFactory(ABC, Generic[P]):
    """An abstract base class for factories that instantiate `TokenProtocol` implementations with user provided encryption keys.

    This abstract base class is intended to be extended by users who want to implement custom tokenization protocols.

    Attributes:
        factory_id: An identifier for the `TokenProtocolFactory`. Should be globally unique across all logically different `TokenProtocolFactory`.
    """

    def __init__(self, factory_id: str):
        """Initializes the `TokenProtocolFactory` with the given globally unique factory ID."""
        self.factory_id = factory_id

    def __repr__(self):
        return f"{type(self).__qualname__}({self.factory_id})"

    @abstractmethod
    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> P:
        """Creates an instance of the [TokenProtocol][spindle_token.core.TokenProtocol] with the user provided encryption keys.

        Arguments:
            private_key:
                The private RSA key to use when tokenizing PII and transcoding tokens.
            recipient_public_key:
                The public RSA key of the intended data recipient to use when transcoding tokens into ephemeral tokens.
                Can be `None` if the instance of `TokenProtocol` will not be transcoding tokens into ephemeral tokens.

        Returns:
            An instance of a `TokenProtocol` implementation.
        """
        ...


@dataclass(frozen=True)
class Token:
    """A specification of a token.

    Attributes:
        name:
            An identifier safe name for the attribute. Will be used as the column name on dataframes. Must be unique
            across other `Token` specifications.
        protocol:
            An instance of `TokenProtocolFactory` that, when provided encryption keys, produced an instance of the
            `TokenProtocol` that generates this kind of token.
        attributes:
            A collection of `PiiAttribute` objects that denote the PII fields used to create the tokens.

    """

    name: str
    protocol: TokenProtocolFactory
    attributes: Iterable[PiiAttribute]
