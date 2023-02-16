from __future__ import annotations

import dataclasses
import enum
import uuid
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import CollectionType, DatasetRef

if TYPE_CHECKING:
    from .aliases import (
        CollectionName,
        ColumnName,
        DatasetTypeName,
        DimensionElementName,
        DimensionName,
        OpaqueTableName,
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


class BatchedDatasetInsertions:
    @property
    def run_rows(
        self,
    ) -> Iterable[tuple[DatasetTypeName, Iterable[tuple[uuid.UUID, dict[DimensionName, Any]]]]]:
        raise NotImplementedError()

    @property
    def opaque_table_rows(self) -> Iterable[tuple[OpaqueTableName, dict[ColumnName, Any]]]:
        raise NotImplementedError()

    # TODO: private state, methods to add dataset info to this object


class BatchedDatasetRemovals:
    @property
    def ids_to_purge(self) -> Iterable[uuid.UUID]:
        """UUIDs that registry should purge.

        This does not include deleting Datastore records, which needs to come
        from a Datastore so a ChainedDatastore can tell us which tables to
        delete from.  Of course, if we don't delete a dataset from all opaque
        tables, we'll get a foreign key violation when we try to delete from
        the main dataset table - but that's a useful kind of failure, and its
        the kind we can fully rollback.

        Note that if there is no ON DELETE CASCADE from tags/calibs tables
        (there is right now, but DM-33635 says we should change that), we
        can join to the main dataset table to get the RUN collection ID from
        the UUID to delete the RUN entries, and only then delete from the
        main dataset table.
        """
        raise NotImplementedError()

    @property
    def refs_to_unstore(self) -> Iterable[DatasetRef]:
        """Fully-expanded DatasetRefs that Datastore should unstore.

        These will have Datastore records for all known Datastores attached
        (grouped by opaque table name), so a ChainedDatastore can filter this
        iterable according to its own rules when passing it along to its
        children.
        """
        raise NotImplementedError()

    def include_refs(self, refs: Iterable[DatasetRef], purge: bool) -> None:
        """Include fully-expanded DatasetRefs (including all datastore records)
        in the set to be deleted.
        """
        raise NotImplementedError()


@dataclasses.dataclass
class BatchedEdit:
    dataset_type_registrations: list[
        tuple[DatasetTypeName, set[DimensionName], StorageClassName, bool]
    ] = dataclasses.field(default_factory=list)
    collection_registrations: list[tuple[CollectionName, CollectionType]] = dataclasses.field(
        default_factory=list
    )
    dimension_data: list[DimensionDataInsertion | DimensionDataSync] = dataclasses.field(default_factory=list)
    dataset_insertions: BatchedDatasetInsertions = dataclasses.field(default_factory=BatchedDatasetInsertions)
    collection_edits: list[
        ChainedCollectionEdit | TaggedCollectionEdit | CalibrationCollectionEdit
    ] = dataclasses.field(default_factory=list)
    opaque_table_inserts: defaultdict[OpaqueTableName, list[dict[ColumnName, Any]]] = dataclasses.field(
        default_factory=defaultdict(list)
    )
    opaque_table_deletes: dict[OpaqueTableName, list[dict[ColumnName, Any]]] = dataclasses.field(
        default_factory=defaultdict(list)
    )
    dataset_removals: BatchedDatasetRemovals = dataclasses.field(default_factory=BatchedDatasetRemovals)
    collection_removals: list[CollectionName] = dataclasses.field(default_factory=list)
    dataset_type_removals: list[DatasetTypeName] = dataclasses.field(default_factory=list)
