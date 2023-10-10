from __future__ import annotations

__all__ = (
    "ArtifactTransferRequest",
    "AbstractArtifactTransferRequest",
    "SingleDatasetTransferRequest",
    "MultipleCoordinateTransferRequest",
    "MultipleTypeTransferRequestDataset",
    "StructuredDataTransferRequest",
    "RecordDataTransferRequest",
)

from collections.abc import Callable
from typing import Any, Protocol, Union, final

import pydantic
from lsst.daf.butler import DataCoordinate, StoredDatastoreItemInfo, DatasetId

from .aliases import CollectionName, DatasetTypeName, StorageClassName, TransferMode, DatastoreTableName
from .primitives import DatasetRef, DatasetType


class AbstractArtifactTransferRequest(Protocol):
    """An interface for objects that represent a transfer of an integral number
    of datasets as suggested by the origin datastore.

    Request instances should be as fine-grained as possible while still
    representing non-fractional datasets and artifacts.  All datasets in a
    request must belong to the same `~lsst.daf.butler.CollectionType.RUN`
    collection.

    Implementations are expected to support serialization and deserialization
    by pydantic, and will be enumerated in the
    `lsst.daf.butler.ArtifactTransferRequest` type alias.

    Request objects are expected to be used in contexts in which a complete
    list of `DatasetRef` objects being transferred is held externally, and
    hence they do not need to record collection or data ID information unless
    it is needed to associate storage artifacts with datasets.  The dataset
    type name and storage class *should* generally be included for convenience
    (since datastore accept/reject rules often use these).
    """

    transfer_mode: TransferMode
    """Transfer mode for these artifacts.

    Note that ``transfer_mode=None`` is no longer supported, as direct writes
    to a datastore root by external code violate repository consistency rules.
    """

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        """Extract references to the datasets represented by this request.

        Parameters
        ----------
        run : `str`
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection all
            datasets belong to.
        get_dataset_type
            Callable that returns the registered `DatasetType` given a (parent)
            dataset type name.  It is unspecified whether this uses the origin
            repository storage class, the destination storage class, or
            something else, but this should be used in the returned
            `DatasetRef` objects for this type regardless.
        get_data_coordinate
            Callable that returns a `DataCoordinate` given a dataset UUID.

        Returns
        -------
        refs : `list` [ `DatasetRef` ]
            Datasets represented by this request.  Must not contain duplicates.
        """
        ...


@final
class SingleDatasetTransferRequest(pydantic.BaseModel):
    """Transfer request for an artifact or set of artifacts that represent a
    single dataset.

    Notes
    -----
    This is the transfer request type normally emitted by `FileDatastore` for
    both the usual single-dataset-single-file case and the
    disassembled-by-components case.
    """

    dataset_id: DatasetId
    """Unique ID for the dataset."""

    dataset_type_name: DatasetTypeName
    """Parent dataset type name."""

    storage_class_name: StorageClassName
    """Storage class in the origin datastore."""

    artifact_records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]]
    """The origin datastore's internal description of the artifacts.

    Because these are records that *just* represent the artifacts, the
    receiving datastore may drop them and create its own records while
    transferring the artifacts without losing anything. If we ever want a
    Datastore capable of representing a single dataset via a combination of
    artifacts and datastore records that are more than a description of the
    artifacts, we'll need a new transfer-request type or an new optional
    attribute for those here.
    """

    transfer_mode: TransferMode
    """Transfer mode for these artifacts."""

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_id,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_id),
                run=run,
                _datastore_records=self.artifact_records,
            )
        ]


@final
class MultipleCoordinateTransferRequest(pydantic.BaseModel):
    """Transfer request for a single artifact that holds multiple datasets
    corresponding to different data IDs.

    Notes
    -----
    This is the transfer request type needed for DECam raws.  We may want to
    limit `FileDatastore` acceptance of these requests to only
    ``transfer_mode="direct"``, since that absolves us of responsibility for
    being able to delete them.  Being able to delete these safely requires
    querying for all other datasets that have the same URI, which is a big
    complication we might not want to continue to support without a stronger
    use case.

    The datastore records nested here are purely descriptions of the files,
    like `SingleDatasetTransferRequest.artifact_records`.
    """

    dataset_type_name: DatasetTypeName
    """Dataset type name."""

    storage_class_name: StorageClassName
    """Storage class in the origin datastore."""

    common_data_id: DataCoordinate
    """Data ID that is common to all datasets in the request.

    For DECam raws this would be an ``{instrument, exposure}`` data ID.
    """

    child_artifact_records: dict[DatasetId, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]
    """The origin datastore's internal description of the artifacts, keyed by
    dataset UUID.

    These are purely descriptions of the artifacts, like
    `SingleDatasetTransferRequest.artifact_records`.
    """

    transfer_mode: TransferMode
    """Transfer mode for these artifacts."""

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset_id,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(dataset_id),
                run=run,
                _datastore_records=dataset_records,
            )
            for dataset_id, dataset_records in self.child_artifact_records.items()
        ]


@final
class MultipleTypeTransferRequestDataset(pydantic.BaseModel):
    """Struct representing a dataset in a `MultipleTypeTransferRequest`."""

    dataset_id: DatasetId
    """Unique ID for the dataset."""

    dataset_type_name: DatasetTypeName
    """Dataset type name."""

    storage_class_name: StorageClassName
    """Storage class in the origin datastore."""

    artifact_records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]]
    """The origin datastore's internal description of the artifact.

    These are purely descriptions of the artifacts, like
    `SingleDatasetTransferRequest.artifact_records`.  All records for all
    datasets in the same request must refer to the same single artifact.
    """


@final
class MultipleTypeTransferRequest(pydantic.BaseModel):
    """Transfer request for a single file that holds multiple datasets with
    the same data ID and different dataset types.

    Notes
    -----
    We do not have a datastore that would emit this request right now; this is
    an illustration of a future possibility.

    It would be nice to have something like this for merging execution
    metadata (task metadata, logs, provenance files) after execution.

    The datastore records nested here are purely descriptions of the files,
    like `DatasetFileTransferRequest.file_records`.
    """

    datasets: list[MultipleTypeTransferRequestDataset]
    """Structs representing the datasets stored in this artifact."""

    transfer_mode: TransferMode
    """Transfer mode for these artifacts."""

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                dataset.dataset_id,
                get_dataset_type(dataset.dataset_type_name),
                get_data_coordinate(dataset.dataset_id),
                run=run,
                _datastore_records=dataset.artifact_records,
            )
            for dataset in self.datasets
        ]


@final
class StructuredDataTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends a JSON-compatible nested dict to
    be saved.

    Notes
    -----
    We do not have a datastore that would emit this request right now; this
    is an illustration of a future possibility.

    A simple file datastore could accept this by just writing a JSON file.
    A more complicated file datastore could merge multiple such requests into
    a single file.  The Sasquatch datastore could check whether they have a
    metric storage class and upload them, etc.  A Sasquatch datastore that is
    read-write rather than write-only might emit this request type.
    """

    dataset_id: DatasetId
    """Unique ID for the dataset."""

    dataset_type_name: DatasetTypeName
    """Dataset type name."""

    storage_class_name: StorageClassName
    """Storage class in the origin datastore."""

    data: dict[str, Any]
    """JSON-compatible data."""

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_id,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_id),
                run=run,
                _datastore_records={},
            )
        ]


@final
class RecordDataTransferRequest(pydantic.BaseModel):
    """A transfer request that directly sends datastore records that fully
    represent a single dataset (i.e. there is no separate artifact).

    Notes
    -----
    We do not have a datastore that would emit this request right now; this is
    an illustration of a future possibility.

    The data records here do *not* describe artifacts; this request imagines
    a future Datastore (or future functionality in new Datastore) that could
    serialize tiny datasets (e.g. metric values) to butler database tables
    specifically to allow those values to be referenced in queries.
    """

    dataset_id: DatasetId
    """Unique ID for the dataset."""

    dataset_type_name: DatasetTypeName
    """Dataset type name."""

    storage_class_name: StorageClassName
    """Storage class in the origin datastore."""

    data_records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]]
    """Records that represent a complete serialization of the dataset.
    """

    def extract_refs(
        self,
        run: CollectionName,
        get_dataset_type: Callable[[DatasetTypeName], DatasetType],
        get_data_coordinate: Callable[[DatasetId], DataCoordinate],
    ) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_id,
                get_dataset_type(self.dataset_type_name),
                get_data_coordinate(self.dataset_id),
                run=run,
                _datastore_records=self.data_records,
            )
        ]


ArtifactTransferRequest = Union[
    SingleDatasetTransferRequest,
    MultipleCoordinateTransferRequest,
    MultipleTypeTransferRequest,
    StructuredDataTransferRequest,
    RecordDataTransferRequest,
]
"""The union of all recognized concrete `ArtifactTransferRequest` types.

Note that these are enumerated up front, and are not intended to be extensible
without modifying core butler code, since all datastore implementations need
to be aware of the complete list.
"""
