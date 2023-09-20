"""A collection of aliases for `str` and `Any` that make type annotations
more readable.

While we could consider using these in the real codebase, the purpose of this
file isn't to propose that, and I am definitely not in favor of going all the
way to `typing.NewType`, as these pretty much all need to be appear in
public interfaces where the newtype would be a big hassle.
"""

from __future__ import annotations

from typing import Any, TypeAlias

CollectionName: TypeAlias = str
CollectionDocumentation: TypeAlias = str
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
ManifestName: TypeAlias = str
