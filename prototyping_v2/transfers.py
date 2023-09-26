from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Protocol

import pydantic

from lsst.daf.butler import DataCoordinate
from lsst.resources import ResourcePath

from .aliases import TransferMode, DatasetTypeName, StorageClassName, CollectionName
from .primitives import DatasetRef, DatasetType, DatasetOpaqueRecordSet, EmptyOpaqueRecordSet, Checksum


class ArtifactTransferRequest(Protocol):
    transfer_mode: TransferMode

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        ...

    def extract_files(self) -> list[tuple[ResourcePath, Checksum | None]]:
        """Return URIs (signed for GET if necessary) and optional checksums."""
        ...


class DatasetFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single-complete-dataset file.

    This is the transfer request type normally emitted by `FileDatastore` for
    both the usual single-dataset-single-file and the
    disassembled-by-components case.

    Data IDs, dataset type dimensions, and the mapping from opaque table name
    to `DatasetOpaqueRecordSet` type are expected to be passed separately.
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    transfer_mode: TransferMode
    opaque_records: DatasetOpaqueRecordSet

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name, self.storage_class_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=self.opaque_records,
            )
        ]


class MultipleCoordinateFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single file that holds multiple datasets
    corresponding to different data IDs.

    This is the transfer request type needed for DECam raws.  We may want to
    limit `FileDatastore` acceptance of these requests to only
    ``transfer_mode="direct"``, since that absolves us of responsibility for
    being able to delete them.  Being able to delete these safely requires
    querying for all other datasets that have the same URI, which is a big
    complication we probably want to drop.
    """

    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    common_data_id: DataCoordinate
    child_records: dict[uuid.UUID, DatasetOpaqueRecordSet]
    transfer_mode: TransferMode

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset_uuid,
                get_dataset_type(self.dataset_type_name, self.storage_class_name),
                get_data_coordinate(dataset_uuid),
                run=run,
                _opaque_records=dataset_records,
            )
            for dataset_uuid, dataset_records in self.child_records.items()
        ]


class MultipleTypeFileTransferRequestDataset(pydantic.BaseModel):
    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    opaque_records: DatasetOpaqueRecordSet


class MultipleTypeFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single file that holds multiple datasets with
    the same data ID and different dataset types.

    We do not have datastore support for this type of storage yet, but it'd be
    nice to have for merging execution metadata (task metadata, logs,
    provenance files) after execution.
    """

    transfer_mode: TransferMode
    datasets: list[MultipleTypeFileTransferRequestDataset]

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset.dataset_uuid,
                get_dataset_type(dataset.dataset_type_name, dataset.storage_class_name),
                get_data_coordinate(dataset.dataset_uuid),
                run=run,
                _opaque_records=dataset.opaque_records,
            )
            for dataset in self.datasets
        ]


class StructuredDataTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends a JSON-compatible nested dict to
    be saved.

    A simple file datastore could accept this by just writing a JSON file.
    A more complicated file datastore could merge multiple such requests into
    a single file.  A Sasquatch datastore could check whether they have a
    metric storage class and upload them, etc...
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    data: dict[str, Any]

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name, self.storage_class_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=DatasetOpaqueRecordSet(EmptyOpaqueRecordSet()),
            )
        ]


class OpaqueRecordTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends opaque records that fully
    represent the dataset (i.e. there is no separate artifact).
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    opaque_records: DatasetOpaqueRecordSet

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName, StorageClassName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name, self.storage_class_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=self.opaque_records,
            )
        ]
