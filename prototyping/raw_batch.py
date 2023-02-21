from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DataIdValue,
    DatastoreConfig,
    DimensionRecord,
    FileDataset,
)
from lsst.resources import ResourcePath

from .primitives import SequenceEditMode, SetEditMode, SetInsertMode

if TYPE_CHECKING:
    from .aliases import (
        CollectionDocumentation,
        CollectionName,
        ColumnName,
        DatasetTypeName,
        DimensionElementName,
        DimensionName,
        FormatterName,
        StorageClassName,
    )
    from .primitives import DatasetRef, DatasetType, OpaqueTableBatch, OpaqueTableKeyBatch


@dataclasses.dataclass
class DatasetTypeRegistration:
    """Serializable representation of a dataset type registration operation."""

    name: DatasetTypeName
    dimensions: set[DimensionName]
    storage_class_name: StorageClassName
    is_calibration: bool
    update: bool

    @classmethod
    def from_dataset_type(cls, dataset_type: DatasetType, update: bool) -> DatasetTypeRegistration:
        return cls(
            dataset_type.name,
            set(dataset_type.dimensions),
            dataset_type.storage_class_name,
            dataset_type.is_calibration,
        )

    def add_to(self, raw_batch: RawBatch) -> None:
        raw_batch.dataset_type_registrations[self.name] = self


@dataclasses.dataclass
class CollectionRegistration:
    """Serializable representation of a collection registration operation."""

    name: CollectionName
    type: CollectionType
    doc: CollectionDocumentation


@dataclasses.dataclass
class SetCollectionDocumentation:
    """Serializable representation of a collection documentation assignment."""

    name: CollectionName
    doc: CollectionDocumentation


@dataclasses.dataclass
class ChainedCollectionEdit:
    """Serializable representation of a CHAINED collection modification."""

    chain: CollectionName
    children: list[CollectionName | int]
    mode: SequenceEditMode
    flatten: bool = False


@dataclasses.dataclass
class TaggedCollectionEdit:
    """Serializable representation of a TAGGED collection modification."""

    collection: CollectionName
    datasets: set[uuid.UUID]
    mode: SetEditMode


@dataclasses.dataclass
class DimensionDataSync:
    """Serializable representation of a dimension-data sync operation."""

    element: DimensionElementName
    records: list[dict[ColumnName, Any]]
    update: bool = False
    on_insert: list[DimensionDataInsertion] = dataclasses.field(default_factory=list)
    on_update: list[DimensionDataInsertion] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class DimensionDataInsertion:
    """Serializable representation of a dimension-data insertion."""

    element: DimensionElementName
    records: dict[tuple[DataIdValue, ...], dict[ColumnName, Any]] = dataclasses.field(default_factory=dict)
    mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP

    def insert_records(self, records: Iterable[DimensionRecord]) -> None:
        raise NotImplementedError("Convert records to dictionaries and deduplicate.")

    def update_from_data_ids(self, data_ids: Iterable[DataCoordinate]) -> None:
        raise NotImplementedError("Extract records from expanded data IDs.")

    def update_from_dataset_refs(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError("Extract records from DatasetRefs with expanded data IDs.")


class DatasetInsertionBatch:
    """Serializable representation of a dataset insertion."""

    def include(self, refs: Iterable[DatasetRef]) -> None:
        """Include DatasetRefs in the set to be inserted.

        Data IDs need not be fully expanded, and any attached datastore records
        are ignored.
        """
        raise NotImplementedError()

    # Internal state and accessors are TBD.


class DatasetRemovalBatch:
    """Serializable representation of a dataset removal."""

    def include(self, refs: Iterable[DatasetRef]) -> None:
        """Include DatasetRefs in the set to be deleted.

        Data IDs need not be fully expanded, and any attached datastore records
        are ignored.
        """
        raise NotImplementedError()

    # Internal state and accessors are TBD.


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

    @classmethod
    def read_export_file(
        cls, file: ResourcePath, dimension_insert_mode: SetInsertMode | None = None
    ) -> Iterator[tuple[DatastoreConfig | None, RawBatch, list[FileDataset]]]:
        """Read an export file, yielding batches of inserts and FileDatsets.

        This will support reading our current YAML export files, yielding a
        single batch and FileDataset combination.

        I'm also envisioning a new export format that's a sequence of
        serialized `RawBatchExport` instances - a sequence to avoid our current
        trouble with exports that are too large to fit in memory.
        """
        raise NotImplementedError()

    def write_export_file(
        self,
        file: ResourcePath,
        datastore_config: DatastoreConfig | None,
        file_datasets: Iterable[FileDataset],
    ) -> None:
        """Append this batch to an export file."""
        raise NotImplementedError()

    def clear(self) -> None:
        raise NotImplementedError("TODO: clear all nested data structures.")

    dataset_type_registrations: dict[DatasetTypeName, DatasetTypeRegistration] = dataclasses.field(
        default_factory=dict
    )

    collection_registrations: dict[CollectionName, CollectionRegistration] = dataclasses.field(
        default_factory=dict
    )

    dimension_syncs: dict[DimensionElementName, DimensionDataSync] = dataclasses.field(default_factory=dict)

    dimension_insertions: dict[DimensionElementName, DimensionDataInsertion] = dataclasses.field(
        default_factory=dict
    )

    dataset_insertions: DatasetInsertionBatch = dataclasses.field(default_factory=DatasetInsertionBatch)

    collection_edits: list[
        # TODO: include CALIBRATION Collection edits, too.
        ChainedCollectionEdit
        | TaggedCollectionEdit
        | SetCollectionDocumentation
    ] = dataclasses.field(default_factory=list)

    opaque_table_insertions: OpaqueTableBatch = dataclasses.field(default_factory=OpaqueTableBatch)

    opaque_table_removals: OpaqueTableKeyBatch = dataclasses.field(default_factory=OpaqueTableKeyBatch)

    dataset_removals: DatasetRemovalBatch = dataclasses.field(default_factory=DatasetRemovalBatch)

    collection_removals: set[CollectionName] = dataclasses.field(default_factory=set)

    dataset_type_removals: set[DatasetTypeName] = dataclasses.field(default_factory=set)


@dataclasses.dataclass
class RawBatchExport:
    """Serializable form of a RawBatch-based export file chunk.

    This is not intended to be used in in-memory interfaces; it will be used
    only by `RawBatch`, but it provides useful exposition of the file format
    (which would be a sequence of these, possibly with compression, with each
    preceded by byte-offset pointers the end of that item on disk).
    """

    datastore_config: dict[str, Any] | None
    """Configuration for the Datastore that wrote
    ``batch.opaque_table_insertions``.

    If ``batch.opaque_table_insertions`` is empty, this should be `None`.
    Otherwise, the receiving Datastore can inspect this configuration to see if
    it can use some or all of those records as-is vs. create new records from
    the `files` attribute.
    """

    batch: RawBatch
    """Registry insertions for the export.

    Any ``opaque_table_insertions`` entries present may or may not be used at
    the discretion of the receiving Datastore (see
    `Datastore.transfer_transaction`).
    """

    files: dict[str, tuple[FormatterName | None, set[uuid.UUID]]]
    """Files for the Datastore to transfer and insert.

    `str` keys are absolute or relative URIs, with the latter relative to the
    export directory, never a datastore root (those should be transformed to
    absolute URIs, since we never want two non-nested datastores to think they
    own the same artifact).

    UUIDs here must correspond to datasets present in
    `batch.dataset_insertions` (or, if we can make that work, the receiving
    `Registry`).
    """
