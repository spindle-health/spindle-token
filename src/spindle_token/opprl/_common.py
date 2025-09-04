"""An private (internal) module containing common implementations of [PiiAttribute][spindle_token.core.PiiAttribute] for attributes used to construct tokens across OPPRL versions.

This module is private to allow for implementations to be changed without creating breaking changes. 
If breaking behavior changes are introduced to any PiiAttribute implementation a copy of the old 
behavior should be added to the corresponding module of the lowest dependant OPPRL version and all
references across all dependant OPPRL versions should use updated to use that "frozen" class instead.
"""

import phonenumbers
from pyspark.sql import Column
from pyspark.sql.functions import (
    when,
    coalesce,
    soundex,
    regexp_replace,
    to_date,
    lower,
    udf,
    lit,
    length,
    substring,
    upper,
    sha2,
)
from pyspark.sql.types import (
    DataType,
    StringType,
    DateType,
    TimestampType,
    TimestampNTZType,
)
from spindle_token.core import PiiAttribute
from spindle_token._utils import empty_to_null, normalize_text, first_char, metaphone, remap


class IdentityAttribute(PiiAttribute):
    """An implementation of [PiiAttribute][spindle_token.core.PiiAttribute] with no transformation (normalization) logic.

    This class is useful when your data contains columns that can be used as attributes
    as-is with no normalization. In particular, if your data has columns that correspond
    to PII attributes that would typically be derived from other PII attributes, such as
    the initial of the first name.

    Examples:
        Create an identity attribute that uses the first initial directly
        as opposed to deriving from the first name.

        >>> attribute = IdentityAttribute("opprl.v1.first.initial")

        The transform method returns the input column unchanged.

        >>> attribute.transform(col("first_initial"), StringType())
        Column<'first_initial'>

        There are no derivatives of identity attributes aside from the attribute itself.

        >>> attribute.derivatives()
        {'opprl.v1.first.initial': IdentityAttribute(opprl.v1.first.initial)}

        Identity attributes can be passed to the tokenize function.

        >>> from spindle_token.opprl import OpprlV1 as v1
        >>> tokenize(
        >>>     df,
        >>>     {
        >>>         IdentityAttribute("opprl.v1.first.initial"): "first_initial",
        >>>         v1.last_name: "last_name",
        >>>         v1.gender: "gender",
        >>>         v1.birth_date: "birth_date",
        >>>     },
        >>>     [v1.token1],
        >>> )
        DataFrame[first_initial: string, last_name: string, ..., opprl_token_1v1: string]

    """

    def transform(self, column: Column, _: DataType) -> Column:
        return column


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


class EmailAttribute(PiiAttribute):

    @property
    def sha2(self) -> "HashedEmail":
        return HashedEmail(self.attr_id + ".sha2", _parent=self)

    def transform(self, column: Column, _: DataType) -> Column:
        return empty_to_null(regexp_replace(lower(column), "\\s", ""))

    def derivatives(self):
        attrs = super().derivatives()
        attrs.update(self.sha2.derivatives())
        return attrs


class HashedEmail(PiiAttribute):
    """An implementation of PiiAttribute for SHA2 hashed email addresses.

    This class can be used directly if the data has a hashed email column or it can be
    instantiated as a derivative of an [EmailAttribute][spindle_token.opprl._common.EmailAttribute].

    """

    def __init__(self, attr_id: str, *, _parent: EmailAttribute | None = None):
        super().__init__(attr_id)
        self._parent = _parent

    def transform(self, column: Column, dtype: DataType):
        if self._parent:
            column = sha2(self._parent.transform(column, dtype), 256)
        return lower(column)


@udf(returnType=StringType())
def _to_e164(phone: str, default_region: str) -> str | None:
    try:
        return phonenumbers.format_number(
            phonenumbers.parse(phone, default_region), phonenumbers.PhoneNumberFormat.E164
        )
    except phonenumbers.NumberParseException:
        return None


class PhoneNumberAttribute(PiiAttribute):
    """An implementation of PiiAttribute for US phone numbers.

    Normalizes phone numbers to E.164 format. See: https://www.itu.int/rec/T-REC-E.164/
    """

    def __init__(self, attr_id: str, default_region: str = "US"):
        super().__init__(attr_id)
        self.default_region = default_region

    def transform(self, column: Column, _: DataType):
        return _to_e164(column.cast(StringType()), lit(self.default_region))


def _is_valid_ssn(ssn: Column) -> Column:
    return ~(
        (length(ssn) != lit(9))
        | (first_char(ssn) == "9")
        | (substring(ssn, 1, 3).isin("000", "666"))
        | (substring(ssn, 4, 2) == "00")
        | (substring(ssn, 6, 4) == "0000")
    )


class SsnAttribute(PiiAttribute):
    """An implementation of PiiAttribute for US social security numbers."""

    def transform(self, column: Column, _: DataType):
        column = regexp_replace(column.cast(StringType()), "[^\\D]", "")
        return when(_is_valid_ssn(column), column)


class GroupNumberAttribute(PiiAttribute):
    """An implementation of PiiAttribute for health insurance "group number" associated with an employer or group plan."""

    def transform(self, column: Column, _: DataType):
        return empty_to_null(regexp_replace(upper(column), "\\s", ""))


class MemberIdAttribute(PiiAttribute):
    """An implementation of PiiAttribute for health insurance "member ID" (aka subscriber ID) that uniquely identifies a covered member of a specific health plan."""

    def transform(self, column: Column, _: DataType):
        return empty_to_null(regexp_replace(upper(column), "\\s", ""))
