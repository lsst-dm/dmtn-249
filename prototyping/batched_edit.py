from __future__ import annotations

import dataclasses
import enum
import uuid
from collections.abc import Iterable
from typing import Any, TYPE_CHECKING
from lsst.daf.butler import CollectionType, DatasetRef

if TYPE_CHECKING:
    from .butler import Butler
    from .aliases import (
        DimensionElementName,
        ColumnName,
        CollectionName,
        DatasetTypeName,
        OpaqueTableName,
        DimensionName,
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


@dataclasses.dataclass
class RemovalHelper:
    """A helper class for removing datasets and collections."""

    butler: Butler
    """Butler being operated upon.
    """

    datasets: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets to be purged.

    This is populated by ``include_collections`` when it processes a RUN
    collection.  Attempting to delete a RUN collection when this list does
    not contain all datasets within it is an error that will cause transaction
    rollback.
    """

    collections: set[CollectionName] = dataclasses.field(default_factory=set)
    """Collections to be removed.

    CHAINED collections in this set will always be removed first.
    """

    chain_links: dict[CollectionName, set[CollectionName]] = dataclasses.field(default_factory=dict)
    """CHAINED collection membership links that will be deleted in order to
    allow the target collections to be removed.

    Keys are CHAINED collection names and values are their to-be-deleted
    members.

    This is populated by ``include_collections``.  Attempting to delete a
    member of a CHAINED collection member without removing the CHAINED
    collection or having an entry here is an error.  Entries here for CHAINED
    collections that are being deleted will be silently ignored.
    """

    associations: dict[CollectionName, Iterable[DatasetRef]] = dataclasses.field(default_factory=dict)
    """TAGGED and CALIBRATION collection membership links that will be deleted
    in order to allow the target datasets to be removed.

    This is populated by ``included_collections`` and ``include_datasets`` if
    ``find_associations_in`` matches all TAGGED or CALIBRATION collections to
    consider (use ``find_associations_in=...`` to find them all).
    """

    orphaned: set[CollectionName] = dataclasses.field(default_factory=set)
    """Current members of a target CHAINED collection that will not belong to
    any CHAINED collection after this removal.

    This is populated by by `incldue_collections` if ``find_orphaned=True``.
    """

    def include_datasets(self, refs: Iterable[DatasetRef], find_associations_in: Any = ()) -> None:
        """Include datasets in the set to be deleted while optoonally looking
        for dangling associations.
        """
        raise NotImplementedError()

    def include_collections(
        self, wildcard: Any, find_orphaned: bool = False, find_associations_in: Any = ()
    ) -> dict[CollectionName, Iterable[DatasetRef]]:
        """Include collections in the set to be deleted while optoonally
        looking for dangling associations and CHAINED collection links.
        """
        raise NotImplementedError()

    def include_orphaned(
        self, find_orphaned: bool = True, recurse: bool = False, find_associations_in: Any = ()
    ) -> None:
        """Include all orphaned collections in the set to be deleted."""
        just_added = self.include(
            self.orphaned, find_orphaned=find_orphaned, find_associations_in=find_associations_in
        )
        self.orphaned.difference_update(just_added.keys())
        if recurse and self.orphaned:
            self.include_orphaned(find_orphaned=True, recurse=True, find_associations_in=find_associations_in)

    def cancel(self) -> None:
        self.datasets.clear()
        self.collections.clear()
        self.chain_links.clear()
        self.orphaned.clear()

    def __nonzero__(self) -> bool:
        return bool(self.targets) or bool(self.chain_links) or bool(self.associations)

    def _into_batched_edit(self) -> BatchedEdit:
        """Package-private method that converts this helper's content into
        a BatchedEdit object that Registry understands how to operate on.
        """
        raise NotImplementedError()
