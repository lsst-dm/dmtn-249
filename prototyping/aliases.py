"""A collection of aliases for `str`, `Any`, etc. that make type annotations
more readable.

While we could consider using these in the real codebase, the purpose of this
file isn't to propose that, and I am definitely not in favor of going all the
way to `typing.NewType`, as these pretty much all need to be appear in public
interfaces where the newtype would be a big hassle.
"""

from __future__ import annotations

__all__ = (
    "CollectionDocumentation",
    "CollectionName",
    "CollectionPattern",
    "ColumnName",
    "DatasetTypeName",
    "DatasetTypePattern",
    "DimensionElementName",
    "DimensionName",
    "GetParameter",
    "InMemoryDataset",
    "DatastoreTableName",
    "StorageClassName",
    "StorageURI",
    "TransferMode",
)

from typing import Any, TypeAlias
from lsst.resources import ResourcePath

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
DatastoreTableName: TypeAlias = str
StorageClassName: TypeAlias = str
PossiblyRelativePath: TypeAlias = str
DatastoreRoot: TypeAlias = ResourcePath
StorageURI: TypeAlias = tuple[
    DatastoreRoot | None, PossiblyRelativePath
]  # Datastore name, possibly-relative URI
TransferMode: TypeAlias = str
