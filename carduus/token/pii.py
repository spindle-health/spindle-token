from abc import ABC
from dataclasses import dataclass

import phonenumbers
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    array_join,
    coalesce,
    dayofmonth,
    length,
    lit,
    lower,
    month,
    regexp_replace,
    soundex,
    substring,
    to_date,
    upper,
    udf,
    when,
    year,
)
from pyspark.sql.types import (
    DataType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    TimestampNTZType,
    TimestampType,
)

from carduus.token._impl import (
    empty_to_null,
    first_char,
    metaphone,
    normalize_text,
    null_safe,
    remap,
)


class PiiTransform(ABC):
    """Abstract base class for normalization and enhancement of a specific PII attribute.

    Intended to be extended by users to add support for building tokens from a custom PII attribute.
    """

    def normalize(self, column: Column, dtype: DataType) -> Column:
        """A normalized representation of the PII column.

        A normalized value has eliminated all representation or encoding differences so all instances of the
        same logical values have identical physical values. For example, text attributes will often be normalized
        by filtering to alpha-numeric characters and whitespace, standardizing to whitespace to the space character,
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
            The normalized version of the PII attribute.
        """
        return column

    def enhancements(self, column: Column) -> dict[str, Column]:
        """A collection of PII attributes that can be automatically derived from a given normalized PII attribute

        If an implementation of [PiiTransform][carduus.token.PiiTransform] does not override this method, it is
        assumed that no enhancements can be derived

        Arguments:
            column:
                The normalized PII column to produce enhancements from.

        Return:
            A `dict` with keys that correspond to the PII attributes of the ___ and values that correspond to the `Column`
            expression that produced the new PII from a normalized input attribute.

        """
        return {}


@dataclass
class NameTransform(PiiTransform):
    enhancement_prefix: str

    def normalize(self, column: Column, _: DataType) -> Column:
        # @TODO Should spaces be stripped out?
        return normalize_text(regexp_replace(column, "[^a-zA-Z ]", ""))

    def enhancements(self, column: Column) -> dict[str, Column]:
        return {
            self.enhancement_prefix + "_initial": first_char(column),
            self.enhancement_prefix + "_soundex": soundex(column),
            self.enhancement_prefix + "_metaphone": metaphone(column),
        }


class GenderTransform(PiiTransform):

    _lookup = {
        "F": "F",
        "W": "F",
        "G": "F",
        "M": "M",
        "B": "M",
    }

    def normalize(self, column: Column, _: DataType) -> Column:
        return coalesce(
            remap(self._lookup, first_char(normalize_text(column))),
            when(column.isNotNull(), "O"),
        )


@dataclass
class DateTransform(PiiTransform):
    enhancement_prefix: str
    date_format: str = "yyyy-MM-dd"

    def normalize(self, column: Column, from_type: DataType) -> Column:
        if isinstance(from_type, (DateType, TimestampType, TimestampNTZType)):
            return column
        if isinstance(from_type, StringType):
            return to_date(column, self.date_format)
        raise Exception(
            f"Cannot normalize column of type {from_type} into a DateType column. Column: {column}."
        )

    def enhancements(self, column: Column) -> dict[str, Column]:
        return {
            self.enhancement_prefix + "_year": year(column),
            self.enhancement_prefix + "_month": month(column),
            self.enhancement_prefix + "_day": dayofmonth(column),
        }


@dataclass
class EmailTransform(PiiTransform):
    enhancement_prefix: str

    def normalize(self, column: Column, _: DataType) -> Column:
        return empty_to_null(regexp_replace(lower(column), "\\s", ""))


@udf(returnType=StringType())
def _to_e164(phone: str, default_region: str) -> str | None:
    try:
        return phonenumbers.format_number(
            phonenumbers.parse(phone, default_region), phonenumbers.PhoneNumberFormat.E164
        )
    except phonenumbers.NumberParseException:
        return None


@dataclass
class PhoneTransform(PiiTransform):
    enhancement_prefix: str
    default_region: str

    def normalize(self, column: Column, from_type: DataType) -> Column:
        if isinstance(from_type, (IntegerType, LongType)):
            column = column.cast(StringType())
        return _to_e164(column, lit(self.default_region))


def _is_valid_ssn(ssn: Column) -> Column:
    return ~(
        (length(ssn) != lit(9))
        | (first_char(ssn) == "9")
        | (substring(ssn, 1, 3).isin("000", "666"))
        | (substring(ssn, 4, 2) == "00")
        | (substring(ssn, 6, 4) == "0000")
    )


@dataclass
class SsnTransform(PiiTransform):

    def normalize(self, column: Column, from_type: DataType) -> Column:
        if isinstance(from_type, (IntegerType, LongType)):
            column = column.cast(StringType())
        column = regexp_replace(column, "[^\\D]", "")
        return when(_is_valid_ssn(column), column)


@dataclass
class GroupNumber(PiiTransform):

    def normalize(self, column: Column, from_type: DataType) -> Column:
        if not isinstance(from_type, StringType):
            column = column.cast(StringType())
        return empty_to_null(regexp_replace(upper(column), "\\s", ""))


@dataclass
class MemberId(PiiTransform):

    def normalize(self, column: Column, from_type: DataType) -> Column:
        if not isinstance(from_type, StringType):
            column = column.cast(StringType())
        return empty_to_null(regexp_replace(upper(column), "\\s", ""))


@null_safe
def join_pii(*args: Column):
    return array_join(array(*args), delimiter=":")
