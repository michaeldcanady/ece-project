"""`TableRow`: a dataclass instance whose fields are the columns of a Parquet table."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import DataclassInstance as TableRow

__all__ = ["TableRow"]
