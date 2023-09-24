from __future__ import annotations

import dataclasses
import uuid
from typing import TYPE_CHECKING, Any, final

import pydantic
from lsst.daf.butler import CollectionType, DataIdValue, DimensionRecord

from .primitives import SequenceEditMode, SetEditMode, SetInsertMode

if TYPE_CHECKING:
    from .aliases import (
        CollectionDocumentation,
        CollectionName,
        ColumnName,
        DatasetTypeName,
        DimensionElementName,
        DimensionName,
        OpaqueTableName,
        StorageClassName,
    )
    from .primitives import DatasetOpaqueRecordSet, DatasetRef, DatasetType


@final
class RawBatch(pydantic.BaseModel):
    """A batch of butler operations to execute (mostly) within a Registry
    transaction.

    Attributes are defined in this class in the order in which Registry should
    apply them; this should ensure foreign key relationships are always
    satisfied (unless there's something wrong with what we've been asked to do,
    and hence we *want* to raise and roll back).

    Dataset type registrations and removals can't go in transactions because
    they can involve table creation and deletion, so we make them go first and
    last and make them idempotent.
    """

    dataset_type_registrations: dict[DatasetTypeName, DatasetTypeRegistration] = pydantic.Field(
        default_factory=dict
    )

    collection_registrations: dict[CollectionName, CollectionRegistration] = pydantic.Field(
        default_factory=dict
    )

    dimension_syncs: dict[DimensionElementName, DimensionDataSync] = pydantic.Field(default_factory=dict)

    dimension_insertions: list[DimensionDataInsertion] = pydantic.Field(default_factory=list)

    dataset_insertions: dict[CollectionName, dict[DatasetTypeName, list[DatasetInsertion]]] = pydantic.Field(
        default_factory=dict
    )

    collection_edits: list[
        # TODO: include CALIBRATION collection edits, too.
        ChainedCollectionEdit
        | TaggedCollectionEdit
        | SetCollectionDocumentation
    ] = pydantic.Field(default_factory=list)

    opaque_table_insertions: dict[OpaqueTableName, dict[uuid.UUID, DatasetOpaqueRecordSet]] = pydantic.Field(
        default_factory=dict
    )

    opaque_table_removals: dict[OpaqueTableName, set[uuid.UUID]] = pydantic.Field(default_factory=dict)

    dataset_removals: set[uuid.UUID] = pydantic.Field(default_factory=set)

    collection_removals: set[CollectionName] = pydantic.Field(default_factory=set)

    dataset_type_removals: set[DatasetTypeName] = pydantic.Field(default_factory=set)


@dataclasses.dataclass
class DatasetTypeRegistration:
    """Serializable representation of a dataset type registration operation."""

    # No name, since that's the key in a dict and this is the value.
    dimensions: set[DimensionName]
    storage_class_name: StorageClassName
    is_calibration: bool
    update: bool

    @classmethod
    def from_dataset_type(cls, dataset_type: DatasetType, update: bool) -> DatasetTypeRegistration:
        return cls(
            set(dataset_type.dimensions),
            dataset_type.storage_class_name,
            dataset_type.is_calibration,
            update=update,
        )


@dataclasses.dataclass
class CollectionRegistration:
    """Serializable representation of a collection registration operation."""

    # No name, since that's the key in a dict and this is the value.
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

    # No element name because that's the key of the dict and this is the value.
    records: list[dict[ColumnName, Any]]
    update: bool = False
    on_insert: list[DimensionDataInsertion] = dataclasses.field(default_factory=list)
    on_update: list[DimensionDataInsertion] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class DimensionDataInsertion:
    """Serializable representation of a dimension-data insertion."""

    element: DimensionElementName
    """Element whose records this object holds.
    """

    records: list[DimensionRecord] = dataclasses.field(default_factory=list)
    """Records to insert.

    Unlike DatasetRef and DataCoordinate, DimensionRecords are simple structs
    with no possibly-shared references to other things we might want to
    normalize out.  We should just make sure Pydantic knows how to handle them
    directly.
    """

    mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP
    """Enum that controls how to handle conflicts with existing records that
    have the same data ID.
    """


@dataclasses.dataclass
class DatasetInsertion:
    """Serializable representation of a registry dataset insertion."""

    # No dataset type or RUN because those are keys in a nested dict and this
    # is part of the value.
    uuid: uuid.UUID
    """Universally unique ID for the dataset."""

    data_coordinate_values: tuple[DataIdValue, ...]
    """Required-only data ID values in the order set by the dataset type's
    dimensions.

    This is more compact and has *much* faster `DataCoordinate` conversions
    than the corresponnding required or full `dict`.
    """

    @classmethod
    def from_ref(cls, ref: DatasetRef) -> DatasetInsertion:
        return cls(uuid=ref.uuid, data_coordinate_values=ref.data_id.values_tuple())
