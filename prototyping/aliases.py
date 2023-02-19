from __future__ import annotations

from typing import Any, TypeAlias

CollectionDocumentation: TypeAlias = str
CollectionName: TypeAlias = str
CollectionPattern: TypeAlias = Any
ColumnName: TypeAlias = str
DatasetTypeName: TypeAlias = str
DatasetTypePattern: TypeAlias = Any
DimensionElementName: TypeAlias = str
DimensionName: TypeAlias = str
GetParameter: TypeAlias = str
InMemoryDataset: TypeAlias = Any
OpaqueTableName: TypeAlias = str
StorageClassName: TypeAlias = str
FormatterName: TypeAlias = str


OpaqueTableValues = Any
"""Per-table, per-dataset opaque values.

This represents either:

- a single row in an opaque table for which the dataset UUID is the only
  primary key, which could just be a `dict` or even a (named) `tuple`.

- multiple rows in an opaque table for which the dataset UUID is part of a
  compound primary key (e.g. when component is another key), which could be a
  `list` or `dict` of `dict` or (named) `tuple` rows.

In both cases, the UUID does not need to be included in the rows, as it's
always held by some outer data structure.

These need to be built-ins or pydantic models so we can serialize them
directly.
"""
