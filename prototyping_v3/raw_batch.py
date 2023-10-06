from __future__ import annotations

import uuid
from typing import Any, final

import pydantic
from lsst.daf.butler import (
    CollectionType,
    DataIdValue,
    DimensionRecord,
    StoredDatastoreItemInfo,
    DimensionUniverse,
    DimensionGraph,
)

from .primitives import SequenceEditMode, SetEditMode, SetInsertMode

from .aliases import (
    CollectionDocumentation,
    CollectionName,
    DatasetTypeName,
    DimensionElementName,
    StorageClassName,
    OpaqueTableName,
)
from .primitives import DatasetRef, DatasetType


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

    dimension_syncs: list[DimensionDataSync] = pydantic.Field(default_factory=list)

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

    opaque_table_insertions: dict[
        # TODO: implement pydantic hooks for polymorphism on read, too.
        OpaqueTableName,
        list[pydantic.SerializeAsAny[StoredDatastoreItemInfo]],
    ] = pydantic.Field(default_factory=dict)

    dataset_removals: set[uuid.UUID] = pydantic.Field(default_factory=set)

    collection_removals: set[CollectionName] = pydantic.Field(default_factory=set)

    dataset_type_removals: set[DatasetTypeName] = pydantic.Field(default_factory=set)

    def update(self, other: RawBatch) -> None:
        raise NotImplementedError("TODO")


@final
class DatasetTypeRegistration(pydantic.BaseModel):
    """Serializable representation of a dataset type registration operation."""

    # No name, since that's the key in a dict and this is the value.
    dimensions: DimensionGraph
    storage_class_name: StorageClassName
    is_calibration: bool
    update: bool

    @classmethod
    def from_dataset_type(cls, dataset_type: DatasetType, update: bool) -> DatasetTypeRegistration:
        return cls(
            dimensions=dataset_type.dimensions,
            storage_class_name=dataset_type.storageClass_name,
            is_calibration=dataset_type.isCalibration(),
            update=update,
        )


@final
class CollectionRegistration(pydantic.BaseModel):
    """Serializable representation of a collection registration operation."""

    # No name, since that's the key in a dict and this is the value.
    type: CollectionType
    doc: CollectionDocumentation


@final
class SetCollectionDocumentation(pydantic.BaseModel):
    """Serializable representation of a collection documentation assignment."""

    name: CollectionName
    doc: CollectionDocumentation


@final
class ChainedCollectionEdit(pydantic.BaseModel):
    """Serializable representation of a CHAINED collection modification."""

    chain: CollectionName
    children: list[CollectionName | int]
    mode: SequenceEditMode
    flatten: bool = False


@final
class TaggedCollectionEdit(pydantic.BaseModel):
    """Serializable representation of a TAGGED collection modification."""

    collection: CollectionName
    datasets: set[uuid.UUID]
    mode: SetEditMode


@final
class DimensionDataSync(pydantic.BaseModel):
    """Serializable representation of a dimension-data sync operation."""

    element: DimensionElementName

    # We use SerializeAsAny to avoid type-slicing when doing a pydantic dump.
    records: list[pydantic.SerializeAsAny[DimensionRecord]]
    update: bool = False
    on_insert: list[DimensionDataInsertion] = pydantic.Field(default_factory=list)
    on_update: list[DimensionDataInsertion] = pydantic.Field(default_factory=list)

    @pydantic.model_validator(mode="before")
    @classmethod
    def _convert_records(cls, data: Any, info: pydantic.ValidationInfo) -> DimensionDataSync:
        match data:
            case DimensionDataSync() as done:
                return done
            case {
                "element": str(element_name),
                "records": list(records),
                "update": bool(update),
                "on_insert": list(on_insert),
                "on_update": list(on_update),
            }:
                assert info.context is not None, "TODO: look for server global as well, raise gracefully"
                universe: DimensionUniverse = info.context["universe"]
                element = universe[element_name]
                type_adapter: pydantic.TypeAdapter[Any] = pydantic.TypeAdapter(element.RecordClass)
                return cls.model_construct(
                    element=element_name,
                    records=[type_adapter.validate_python(r) for r in records],
                    update=update,
                    on_insert=[
                        DimensionDataInsertion.model_validate(r, context=info.context) for r in on_insert
                    ],
                    on_update=[
                        DimensionDataInsertion.model_validate(r, context=info.context) for r in on_update
                    ],
                )
            case _:
                raise ValueError(f"Could not validate {data} as DimensionDataSync.")


@final
class DimensionDataInsertion(pydantic.BaseModel):
    """Serializable representation of a dimension-data insertion."""

    element: DimensionElementName
    """Element whose records this object holds.
    """

    records: list[pydantic.SerializeAsAny[DimensionRecord]] = pydantic.Field(default_factory=list)
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


@final
class DatasetInsertion(pydantic.BaseModel):
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
        return cls(uuid=ref.id, data_coordinate_values=ref.dataId.values_tuple())
