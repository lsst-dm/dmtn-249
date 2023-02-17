from __future__ import annotations

import dataclasses
import enum
import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import CollectionType

if TYPE_CHECKING:
    from .aliases import (
        CollectionDocumentation,
        CollectionName,
        ColumnName,
        DatasetRef,
        DatasetTypeName,
        DimensionElementName,
        DimensionName,
        OpaqueTableName,
        OpaqueTableRow,
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


@dataclasses.dataclass
class DimensionDataInsertion:
    element: DimensionElementName
    records: list[dict[ColumnName, Any]]
    mode: SetInsertMode


@dataclasses.dataclass
class DimensionDataSync:
    element: DimensionElementName
    record: list[dict[ColumnName, Any]]
    update: bool = False
    on_insert: list[DimensionDataInsertion | DimensionDataSync] = dataclasses.field(default_factory=list)
    on_update: list[DimensionDataInsertion | DimensionDataSync] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ChainedCollectionEdit:
    chain: CollectionName
    children: list[CollectionName | int]
    mode: SequenceEditMode
    flatten: bool = False


@dataclasses.dataclass
class TaggedCollectionEdit:
    collection: CollectionName
    datasets: set[uuid.UUID]
    mode: SetEditMode


@dataclasses.dataclass
class CalibrationCollectionEdit:
    collection: CollectionName
    datasets: set[uuid.UUID]
    mode: SetEditMode


class DatasetInsertionBatch:
    def include_refs(self, refs: Iterable[DatasetRef]) -> None:
        """Include DatasetRefs in the set to be inserted.

        Data IDs need not be fully expanded, and any attached datastore records
        are ignored.
        """
        raise NotImplementedError()

    # Internal state and accessors used by Registry are TBD.


class DatasetRemovalBatch:
    def include(self, refs: Iterable[DatasetRef]) -> None:
        """Include DatasetRefs in the set to be deleted.

        Data IDs need not be fully expanded, and any attached datastore records
        are ignored.
        """
        raise NotImplementedError()

    # Internal state and accessors used by Registry are TBD.


class OpaqueTableInsertionBatch:
    def include(self, table: OpaqueTableName, rows: Iterable[OpaqueTableRow]) -> None:
        raise NotImplementedError()

    def attach_to(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        raise NotImplementedError()

    # Internal state and accessors used by Registry are TBD.


class OpaqueTableRemovalBatch:
    def include(self, table: OpaqueTableName, uuids: Iterable[uuid.UUID]) -> None:
        raise NotImplementedError()

    # Internal state and accessors used by Registry are TBD.


@dataclasses.dataclass
class RawBatch:
    """A batch of butler operations to execute (mostly) within a Registry
    transaction.

    Attributes are defined in this class in the order in which Registry should
    apply them; this should ensure foreign key relationships are always
    satisfied (unless there's something wrong with what we've been asked to do,
    and hence *want* to raise and roll back).

    Dataset type registrations and removals can't go in transactions because
    they can involve table creation and deletion, so we make them go first and
    last and make them idempotent.

    This is very much an internal class.  I envision having a few higher-level
    classes that provide public interfaces for batching up some of the things
    it can do, and all of them converting their content into `BatchedEdit`
    instance for actual execution (often when a context manager is closed).
    See `RemovalHelper` as an example of this pattern.

    This class and all of the things it holds should be pydantic models or
    built-ins so it's serializable as is (no recursive transformation to some
    serializable form that requires a ton of new object instantiation).
    I don't really expect Registry to need to convert from these types back
    to our user-facing primitives (DatasetType, DataCoordinate, DatasetRef,
    DimensionRecord, etc), since SQLAlchemy ultimately wants builtins, too.
    """

    dataset_type_registrations: list[
        tuple[DatasetTypeName, set[DimensionName], StorageClassName, bool]
    ] = dataclasses.field(default_factory=list)

    collection_registrations: list[
        tuple[CollectionName, CollectionType, CollectionDocumentation]
    ] = dataclasses.field(default_factory=list)

    dimension_data: list[DimensionDataInsertion | DimensionDataSync] = dataclasses.field(default_factory=list)

    dataset_insertions: DatasetInsertionBatch = dataclasses.field(default_factory=DatasetInsertionBatch)

    collection_edits: list[
        ChainedCollectionEdit | TaggedCollectionEdit | CalibrationCollectionEdit
    ] = dataclasses.field(default_factory=list)

    opaque_table_insertions: OpaqueTableInsertionBatch = dataclasses.field(
        default_factory=OpaqueTableInsertionBatch
    )

    opaque_table_removals: OpaqueTableRemovalBatch = dataclasses.field(
        default_factory=OpaqueTableRemovalBatch
    )

    dataset_removals: DatasetRemovalBatch = dataclasses.field(default_factory=DatasetRemovalBatch)

    collection_removals: list[CollectionName] = dataclasses.field(default_factory=list)

    dataset_type_removals: list[DatasetTypeName] = dataclasses.field(default_factory=list)
