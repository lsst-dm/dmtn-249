from __future__ import annotations

import dataclasses
import enum
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any
from uuid import UUID

from lsst.daf.butler import DataCoordinate, StorageClass, ddl

if TYPE_CHECKING:
    from .aliases import (
        CollectionName,
        ColumnName,
        DatasetTypeName,
        DimensionName,
        OpaqueTableName,
        OpaqueTableValues,
        StorageClassName,
    )


class SequenceEditMode(enum.Enum):
    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    EXTEND = enum.auto()
    PREPEND = enum.auto()


class SetInsertMode(enum.Enum):
    INSERT_OR_FAIL = enum.auto()
    INSERT_OR_SKIP = enum.auto()
    INSERT_OR_REPLACE = enum.auto()


class SetEditMode(enum.Enum):
    INSERT_OR_FAIL = SetInsertMode.INSERT_OR_FAIL
    INSERT_OR_SKIP = SetInsertMode.INSERT_OR_SKIP
    INSERT_OR_REPLACE = SetInsertMode.INSERT_OR_REPLACE
    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    DISCARD = enum.auto()


class DimensionGroup(Set[DimensionName]):
    """Placeholder for DimensionGraph replacement approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """


@dataclasses.dataclass
class DatasetType:
    """Placeholder for the current DatasetType, but with:

    - a DimensionGroup instead of a DimensionGraph;
    - snake case for consistency with the rest of the prototyping.
    """

    name: DatasetTypeName
    dimensions: DimensionGroup
    storage_class_name: StorageClassName
    is_calibration: bool

    @property
    def storage_class(self) -> StorageClass:
        raise NotImplementedError()


@dataclasses.dataclass
class DatasetRef:
    """Like the current DatasetRef, but with

    - datastore records;
    - ``id`` renamed to ``uuid`` (more obviously distinct from data ID);
    - snake case for consistency with the rest of the prototyping.
    """

    uuid: UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    run: CollectionName

    datastore_records: Mapping[OpaqueTableName, OpaqueTableValues]
    """All opaque-table records with the same UUID as this dataset.

    It's worth considering whether this should be package private or public.
    It's important that both Registry and Datastore access it, so it can't be
    class-private, but we at least don't want users modifying it, and we may
    or may not want them looking at it.
    """


class OpaqueTableDefinition(ABC):
    """Object that represents the definition of an opaque table.

    Datastores construct these to describe what they need; Butler takes them
    from Datastore and gives them to Registry; Registry holds them and uses
    them to create and query tables without caring about the details.

    I'm still thinking vaguely about opaque tables being used by things other
    than Datastore (e.g. if we ever get around to storage-class-specific
    metadata tables).  But I think I'm willing to lock us into dataset UUIDs
    always being [part of] the primary key to try to reduce complexity.

    This class's methods interface with an  ``OpaqueTableValues`` alias that's
    really just `typing.Any` under the hood (see docstring in aliases.py).
    I'm using `Any` not just to avoid being overly specific here: this is a
    type erasure pattern, in which each OpaqueTableDefinition implementation
    probably has its own preferred type for OpaqueTableValues, but nothing else
    cares what it is.  Using `Any` avoids a lot of casts, and not using `Any`
    (but casting all over the place) isn't any type-safer.
    """

    @property
    @abstractmethod
    def name(self) -> OpaqueTableName:
        raise NotImplementedError()

    @property
    @abstractmethod
    def spec(self) -> ddl.TableSpec:
        raise NotImplementedError()

    @abstractmethod
    def values_to_raw(self, uuid: UUID, values: OpaqueTableValues) -> Iterable[dict[ColumnName, Any]]:
        """Convert from the OpaqueTableValues type used for this opaque table
        to dictionaries of built-ins that can be passed directly to SQLALchemy.
        """
        raise NotImplementedError()

    @abstractmethod
    def raw_to_values(self, rows: Iterable[Mapping[ColumnName, Any]]) -> dict[UUID, OpaqueTableValues]:
        """Convert from an iterable of SQLAlchemy-friendly mappings to a
        mapping of OpaqueTableValues keyed by UUID.
        """
        raise NotImplementedError()


class OpaqueTableKeyBatch:
    """A data structure that holds the UUID keys for rows in several
    opaque tables.

    This is used to express deletes from one or more opaque tables, since we
    only need the UUIDs for that.

    This is basically a convenience wrapper around defaultdict(set).
    """

    def __init__(self) -> None:
        self._data: defaultdict[OpaqueTableName, set[UUID]] = defaultdict(set)

    def __iter__(self) -> Iterator[tuple[OpaqueTableName, set[UUID]]]:
        return ((table_name, keys) for table_name, keys in self._data.items() if keys)

    def __getitem__(self, table_name: OpaqueTableName) -> set[UUID]:
        return self._data[table_name]

    def __delitem__(self, table_name: OpaqueTableName) -> None:
        del self._data[table_name]

    def insert(self, table_name: OpaqueTableName, keys: Iterable[UUID]) -> None:
        self._data[table_name].update(keys)

    def update(self, other: OpaqueTableKeyBatch) -> None:
        for table_name, keys in other:
            self.insert(table_name, keys)


class OpaqueTableBatch:
    """A data structure that holds rows for several opaque tables.

    This is used to express inserts into one or more opaque tables, usually
    coming from a Datastore.

    This is basically a convenience wrapper around defaultdict(dict).
    """

    def __init__(self) -> None:
        self._data: defaultdict[OpaqueTableName, dict[UUID, OpaqueTableValues]] = defaultdict(dict)

    def __iter__(self) -> Iterator[tuple[OpaqueTableName, dict[UUID, OpaqueTableValues]]]:
        return ((table_name, data) for table_name, data in self._data.items() if data)

    def __getitem__(self, table_name: OpaqueTableName) -> dict[UUID, OpaqueTableValues]:
        return self._data[table_name]

    def __delitem__(self, table_name: OpaqueTableName) -> None:
        del self._data[table_name]

    def insert(self, table_name: OpaqueTableName, data: Iterable[tuple[UUID, OpaqueTableValues]]) -> None:
        self._data[table_name].update(data)

    def update(self, other: OpaqueTableBatch) -> None:
        for table_name, data in other:
            self.insert(table_name, data.items())

    def attach_to(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        raise NotImplementedError()
