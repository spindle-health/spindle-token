from pyspark.sql import Column
from pyspark.sql.functions import (
    when,
    coalesce,
    soundex,
    regexp_replace,
    to_date,
)
from pyspark.sql.types import (
    DataType,
    StringType,
    DateType,
    TimestampType,
    TimestampNTZType,
)
from spindle_token.core import PiiAttribute
from spindle_token._utils import normalize_text, first_char, metaphone, remap


class _InitialAttribute(PiiAttribute):

    def __init__(self, parent: "NameAttribute"):
        super().__init__(f"{parent.attr_id}.initial")
        self.parent = parent

    def transform(self, column: Column, dtype: DataType) -> Column:
        return first_char(self.parent.transform(column, dtype))


class _SoundexAttribute(PiiAttribute):

    def __init__(self, parent: "NameAttribute"):
        super().__init__(f"{parent.attr_id}.soundex")
        self.parent = parent

    def transform(self, column: Column, dtype: DataType) -> Column:
        return soundex(self.parent.transform(column, dtype))


class _MetaphoneAttribute(PiiAttribute):

    def __init__(self, parent: "NameAttribute"):
        super().__init__(f"{parent.attr_id}.metaphone")
        self.parent = parent

    def transform(self, column: Column, dtype: DataType) -> Column:
        return metaphone(self.parent.transform(column, dtype))


class NameAttribute(PiiAttribute):

    def transform(self, column: Column, _: DataType) -> Column:
        return normalize_text(regexp_replace(column, "[^a-zA-Z ]", ""))

    @property
    def initial(self) -> _InitialAttribute:
        return _InitialAttribute(self)

    @property
    def soundex(self) -> _SoundexAttribute:
        return _SoundexAttribute(self)

    @property
    def metaphone(self) -> _MetaphoneAttribute:
        return _MetaphoneAttribute(self)

    def derivatives(self) -> dict[str, PiiAttribute]:
        attrs = super().derivatives()
        attrs.update(self.initial.derivatives())
        attrs.update(self.soundex.derivatives())
        attrs.update(self.metaphone.derivatives())
        return attrs


class GenderAttribute(PiiAttribute):

    _lookup = {
        "F": "F",
        "W": "F",
        "G": "F",
        "M": "M",
        "B": "M",
    }

    def transform(self, column: Column, _: DataType) -> Column:
        return coalesce(
            remap(self._lookup, first_char(normalize_text(column))),
            when(column.isNotNull(), "O"),
        )


class DateAttribute(PiiAttribute):

    def __init__(self, attr_id: str, date_format: str):
        super().__init__(attr_id)
        self.date_format = date_format

    def transform(self, column: Column, dtype: DataType) -> Column:
        if isinstance(dtype, (DateType, TimestampType, TimestampNTZType)):
            return column
        if isinstance(dtype, StringType):
            return to_date(column, self.date_format)
        raise Exception(
            f"Cannot normalize column of type {dtype} into a DateType column. Column: {column}."
        )
