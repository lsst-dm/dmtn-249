from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from typing import Any, Protocol, Union, final

import pydantic
from lsst.daf.butler import DataCoordinate
from lsst.resources import ResourcePath

from .aliases import CollectionName, DatasetTypeName, StorageClassName, TransferMode
from .opaque import Checksum, DatasetOpaqueRecordSet, OpaqueRecordSet, EmptyOpaqueRecordSet
from .primitives import DatasetRef, DatasetType


class AbstractArtifactTransferRequest(Protocol):
    """An interface for objects that represent a transfer of an integral number
    of datasets as suggested by the origin datastore.

    Request instances should be as fine-grained as possible while still
    representing non-fractional datasets and artifacts.

    Implementations are expected to support serialization and deserialization
    by pydantic.
    """

    transfer_mode: TransferMode

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        ...

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        """Return URIs (signed for GET if necessary) and optional checksums."""
        ...


@final
class DatasetFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single-complete-dataset file.

    Notes
    -----
    This is the transfer request type normally emitted by `FileDatastore` for
    both the usual single-dataset-single-file and the
    disassembled-by-components case.

    The `file_records` here are declared to be the datastore's internal
    description of the file(s), and hence the receiving datastore may drop them
    and create its own while transferring the files without losing anything. If
    we ever want a Datastore capable of representing a single dataset via a
    combination of files and opaque records that are more than a description of
    the files, we'll need a new transfer-request type or an new optional
    attribute for those here.
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    transfer_mode: TransferMode
    file_records: DatasetOpaqueRecordSet

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=self.file_records,
            )
        ]

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        return self.file_records.extract_files()


@final
class MultipleCoordinateFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single file that holds multiple datasets
    corresponding to different data IDs.

    Notes
    -----
    This is the transfer request type needed for DECam raws.  We may want to
    limit `FileDatastore` acceptance of these requests to only
    ``transfer_mode="direct"``, since that absolves us of responsibility for
    being able to delete them.  Being able to delete these safely requires
    querying for all other datasets that have the same URI, which is a big
    complication we probably want to drop.

    The opaque records nested here are purely descriptions of the files, like
    `DatasetFileTransferRequest.file_records`.
    """

    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    common_data_id: DataCoordinate
    child_records: dict[uuid.UUID, DatasetOpaqueRecordSet]
    transfer_mode: TransferMode

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset_uuid,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(dataset_uuid),
                run=run,
                _opaque_records=dataset_records,
            )
            for dataset_uuid, dataset_records in self.child_records.items()
        ]

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        for records in self.child_records.values():
            # Records for children may differ, but the whole point of this
            # ArtifactTransferRequest type is that they refer to the same
            # file, so we just need one loop iteration.
            return records.extract_files()
        raise AssertionError("MultipleCoordinateFileTransferRequest must have children.")


@final
class MultipleTypeFileTransferRequestDataset(pydantic.BaseModel):
    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    file_records: DatasetOpaqueRecordSet


@final
class MultipleTypeFileTransferRequest(pydantic.BaseModel):
    """Transfer request for a single file that holds multiple datasets with
    the same data ID and different dataset types.

    Notes
    -----
    We do not have datastore support for this type of storage yet, but it'd be
    nice to have for merging execution metadata (task metadata, logs,
    provenance files) after execution.

    The opaque records nested here are purely descriptions of the files, like
    `DatasetFileTransferRequest.file_records`.
    """

    transfer_mode: TransferMode
    datasets: list[MultipleTypeFileTransferRequestDataset]

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset.dataset_uuid,
                get_dataset_type(dataset.dataset_type_name),
                get_data_coordinate(dataset.dataset_uuid),
                run=run,
                _opaque_records=dataset.file_records,
            )
            for dataset in self.datasets
        ]

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        for dataset in self.datasets:
            # Records for children may differ, but the whole point of this
            # ArtifactTransferRequest type is that they refer to the same
            # file, so we just need one loop iteration.
            return dataset.file_records.extract_files()
        raise AssertionError("MultipleTypeFileTransferRequest must have datasets.")


@final
class StructuredDataTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends a JSON-compatible nested dict to
    be saved.

    Notes
    -----
    A simple file datastore could accept this by just writing a JSON file.
    A more complicated file datastore could merge multiple such requests into
    a single file.  A Sasquatch datastore could check whether they have a
    metric storage class and upload them, etc.
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    data: dict[str, Any]

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=DatasetOpaqueRecordSet(EmptyOpaqueRecordSet()),
            )
        ]

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        return {}


@final
class OpaqueRecordTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends opaque records that fully
    represent the dataset (i.e. there is no separate artifact).

    Notes
    -----
    The opaque records here do *not* describe files; this request imagines a
    future Datastore (or future functionality in new Datastore) that could
    serialize tiny datasets (e.g. metric values) to registry database tables if
    they're the sort of thing we'd like to be able to use in registry queries
    someday.
    """

    dataset_uuid: uuid.UUID
    dataset_type_name: DatasetTypeName
    storage_class_name: StorageClassName
    opaque_records: DatasetOpaqueRecordSet

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[uuid.UUID], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_uuid,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_uuid),
                run=run,
                _opaque_records=self.opaque_records,
            )
        ]

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        return {}


# Union of all recognized concrete ArtifactTransferRequest types.  Note that
# these are enumerated up front, and are not intended to be extensible outside
# daf_butler.
ArtifactTransferRequest = Union[
    DatasetFileTransferRequest,
    MultipleCoordinateFileTransferRequest,
    MultipleTypeFileTransferRequest,
    StructuredDataTransferRequest,
    OpaqueRecordTransferRequest,
]


class ArtifactTransferResponse(pydantic.BaseModel):
    origin: ArtifactTransferRequest
    destination_records: OpaqueRecordSet


class ArtifactTransferManifest(pydantic.BaseModel):
    # TODO: need to factor the first tow attributes with RawBatch better;
    # job for a dataset container type.
    dataset_types: dict[DatasetTypeName, DatasetType]
    data_coordinates: dict[uuid.UUID, DataCoordinate]
    responses: dict[CollectionName, ArtifactTransferResponse]

    def extract_refs(self) -> Iterable[DatasetRef]:
        for run_name, response in self.responses.items():
            yield from response.origin.extract_refs(
                run_name,
                lambda n: self.dataset_types[n],
                lambda u: self.data_coordinates[u],
            )
