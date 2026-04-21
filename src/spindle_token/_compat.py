from __future__ import annotations


def _is_missing_pyspark(exc: ModuleNotFoundError) -> bool:
    """Return True when the import failure is caused by missing PySpark."""
    return (
        exc.name == "pyspark"
        or (exc.name is not None and exc.name.startswith("pyspark."))
        or "No module named 'pyspark'" in str(exc)
    )
