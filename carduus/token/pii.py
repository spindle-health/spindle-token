from abc import ABC
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    array_join,
    coalesce,
    col,
    dayofmonth,
    month,
    regexp_replace,
    soundex,
    to_date,
    when,
    year,
)
from pyspark.sql.types import DataType, DateType, StringType, TimestampNTZType, TimestampType

from carduus.token._impl import first_char, metaphone, normalize_text, null_safe, remap


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
class Pii:
    data: DataFrame
    pii_columns: set[str]


def normalize_pii(
    df: DataFrame,
    pii_transforms: dict[str, PiiTransform],
) -> DataFrame:
    return df.select(
        *[
            (
                pii_transforms[column]
                .normalize(df[column], df.schema[column].dataType)
                .alias(column)
                if column in pii_transforms
                else col(column)
            )
            for column in df.columns
        ]
    )


def enhance_pii(
    df: DataFrame,
    pii_transforms: dict[str, PiiTransform],
) -> tuple[DataFrame, set[str]]:
    new_pii: set[str] = set()
    all_columns = []
    for column in df.columns:
        all_columns.append(col(column))
        if column in pii_transforms:
            enhancements = pii_transforms[column].enhancements(col(column))
            all_columns.extend(
                [
                    enhancement_col.alias(enhancement_name)
                    for enhancement_name, enhancement_col in enhancements.items()
                    if enhancement_name not in df.columns
                ]
            )
            new_pii = new_pii | set(enhancements.keys())
    return df.select(*all_columns), new_pii


@null_safe
def join_pii(*args: Column):
    return array_join(array(*args), delimiter=":")
