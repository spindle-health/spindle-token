from typing import Any, Callable

import jellyfish as j
from pyspark.sql import Column
from pyspark.sql.functions import (
    base64,
    length,
    lit,
    regexp_replace,
    replace,
    substring,
    trim,
    udf,
    upper,
    when,
)
from pyspark.sql.types import StringType


def remap(mapping: dict[Any, Any], col: Column, default: Any | None = None) -> Column:
    result = None
    for k, v in mapping.items():
        if result is None:
            result = when(col == lit(k), lit(v))
        else:
            result = result.when(col == lit(k), lit(v))
    if default is not None and result is not None:
        result = result.otherwise(default)
    return col if result is None else result


def null_if(pred: Callable[[Column], Column], col: Column) -> Column:
    return when(~pred(col), col).otherwise(lit(None))


def first_char(col: Column) -> Column:
    return substring(col, 1, 1)


def _metaphone_udf_impl(s: str | None) -> str | None:
    return j.metaphone(s) if s else None


metaphone = udf(_metaphone_udf_impl, returnType=StringType(), useArrow=False)


def normalize_text(raw: Column) -> Column:
    return null_if(lambda c: length(c) == 0, trim(regexp_replace(upper(raw), "\\s+", " ")))


def null_safe(func):
    """A decorator for functions that operate over pyspark Column objects.
    If any of the input columns are NULL, the result will be NULL. Otherwise the decorated function will be called.
    """

    def wrapper(*args: Column) -> Column:
        if len(args) == 0:
            return func()
        result: Column = when(args[0].isNull(), lit(None))
        for arg in args[1:]:
            result = result.when(arg.isNull(), lit(None))
        return result.otherwise(func(*args))

    return wrapper


def base64_no_newline(col: Column) -> Column:
    """The default behavior of Spark's base64 function adds newlines every 76 characters for parity with unix.
    Although this behavior doesn't make tokens invalid, it is likely to cause issues with users working with CSV files
    that contain tokens.

    See: https://superuser.com/questions/1225134/why-does-the-base64-of-a-string-contain-n
    """
    return replace(base64(col), lit("\r\n"), lit(""))
