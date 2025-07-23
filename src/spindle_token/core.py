from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DataType


class PiiAttribute(ABC):

    def __init__(self, attr_id: str):
        self.attr_id = attr_id

    def __repr__(self):
        return f"{type(self).__qualname__}({self.attr_id})"

    def __hash__(self):
        return self.__repr__().__hash__()

    @abstractmethod
    def transform(self, column: Column, dtype: DataType) -> Column:
        pass

    def derivatives(self) -> dict[str, "PiiAttribute"]:
        return {self.attr_id: self}


class TokenProtocol(ABC):

    @abstractmethod
    def tokenize(
        self,
        df: DataFrame,
        col_mapping: Mapping[PiiAttribute, str],
        attributes: Iterable[PiiAttribute],
    ) -> Column: ...

    @abstractmethod
    def transcode_out(self, token: Column) -> Column: ...

    @abstractmethod
    def transcode_in(self, ephemeral_token: Column) -> Column: ...


class TokenProtocolFactory(ABC):

    def __init__(self, id: str):
        self.id = id

    @abstractmethod
    def bind(self, private_key: bytes, recipient_public_key: bytes | None) -> TokenProtocol: ...


@dataclass(frozen=True)
class Token:
    name: str
    protocol: TokenProtocolFactory
    attributes: Iterable[PiiAttribute]
