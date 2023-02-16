from __future__ import annotations

import dataclasses
import enum
import uuid
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
    def to_purge(self) -> Iterable[uuid.UUID]:
        raise NotImplementedError()

    @property
    def to_unstore(self) -> Iterable[tuple[OpaqueTableName, Iterable[uuid.UUID]]]:
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
    dataset_removals: BatchedDatasetRemovals = dataclasses.field(default_factory=BatchedDatasetRemovals)
    collection_removals: list[CollectionName] = dataclasses.field(default_factory=list)
    dataset_type_removals: list[DatasetTypeName] = dataclasses.field(default_factory=list)
