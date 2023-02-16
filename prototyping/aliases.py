from __future__ import annotations

from collections.abc import Set
from typing import Any, TypeAlias

CollectionName: TypeAlias = str
DimensionName: TypeAlias = str
DimensionElementName: TypeAlias = str
DatasetTypeName: TypeAlias = str
ColumnName: TypeAlias = str
OpaqueTableName: TypeAlias = str
StorageClassName: TypeAlias = str
CollectionPattern: TypeAlias = Any
DatasetTypePattern: TypeAlias = Any
CollectionDocumentation: TypeAlias = str
GetParameter: TypeAlias = str
InMemoryDataset: TypeAlias = Any


class DimensionGroup(Set[DimensionName]):
    """Replacement for DimensionGraph approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """
