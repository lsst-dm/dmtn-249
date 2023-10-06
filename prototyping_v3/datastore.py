from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, TYPE_CHECKING

from lsst.resources import ResourcePath

from lsst.daf.butler import StoredDatastoreItemInfo, ddl
from .aliases import GetParameter, InMemoryDataset, OpaqueTableName, StorageURI
from .artifact_transfer import ArtifactTransferManifest, ArtifactTransferRequest
from .extension_config import ExtensionConfig
from .primitives import DatasetRef

if TYPE_CHECKING:
    from .persistent_limited_butler import PersistentLimitedButler


class DatastoreConfig(ExtensionConfig):
    @classmethod
    @abstractmethod
    def make_datastore(cls, root: ResourcePath) -> Datastore:
        raise NotImplementedError()


@dataclasses.dataclass
class DatastoreTableDefinition:
    spec: ddl.TableSpec
    record_type: type[StoredDatastoreItemInfo]


class Datastore(ABC):
    """Interface for butler component that stores dataset contents."""

    config: DatastoreConfig
    """Configuration that can be used to reconstruct this datastore.
    """

    @property
    @abstractmethod
    def tables(self) -> Mapping[OpaqueTableName, DatastoreTableDefinition]:
        raise NotImplementedError()

    @abstractmethod
    def extract_existing_uris(self, refs: Iterable[DatasetRef]) -> list[StorageURI]:
        """Extract URIs from the records attached to datasets.

        These are possibly-relative URIs that need to be made absolute and
        possibly signed in order to be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def predict_new_uris(self, refs: Iterable[DatasetRef]) -> list[StorageURI]:
        """Predict URIs for new datasets to be written.

        These are possibly-relative URIs that need to be made absolute and
        possibly signed in order to be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Load datasets into memory.

        Incoming `DatasetRef` objects will have already been fully expanded to
        include both expanded data IDs and all possibly-relevant opaque table
        records.

        Notes
        -----
        The datasets are not necessarily returned in the order they are passed
        in, to better permit async implementations with lazy first-received
        iterator returns.  Implementations that can guarantee consistent
        ordering might want to explicitly avoid it, to avoid allowing callers
        to grow dependent on that behavior instead of checking the returned
        `DatasetRef` objects themselves.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_transfer_requests(
        self, refs: Iterable[DatasetRef], transfer_mode: str
    ) -> Iterable[ArtifactTransferRequest]:
        """Map the given datasets into artifact transfer requests that could
        be used to transfer those datasets to another datastore.

        Each transfer request object should represent an integral number of
        datasets and correspond to how the artifacts would ideally be
        transferred to another datastore of the same type.  There is no
        guarantee that the receiving datastore will keep the same artifacts.
        """
        raise NotImplementedError()

    @abstractmethod
    def receive_transfer_requests(
        self,
        refs: Iterable[DatasetRef],
        requests: Iterable[ArtifactTransferRequest],
        origin: PersistentLimitedButler,
    ) -> ArtifactTransferManifest:
        """Create a manifest that records how this datastore would receive the
        given artifacts from another datastore.

        This does not actually perform any artifact writes.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be received.
        requests : `~collections.abc.Iterable` [ `ArtifactTransferRequest` ]
            Artifacts according to the origin datastore.  Minimal-effort
            transfers - like file copies - preserve artifacts, but in the
            general case transfers only need to preserve datasets.
        origin : `PersistentLimitedButler`
            Butler that owns or at least knows how to read the datasets
            being transferred.

        Returns
        -------
        manifest
            Description of the artifacts to be transferred as interpreted by
            this datastore.  Embedded destination records may include signed
            URIs.
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_transfer_manifest(
        self,
        manifest: ArtifactTransferManifest,
        destination_paths: Mapping[StorageURI, ResourcePath],
        origin: PersistentLimitedButler,
    ) -> dict[OpaqueTableName, list[StoredDatastoreItemInfo]]:
        """Actually execute transfers to this datastore."""
        raise NotImplementedError()

    @abstractmethod
    def put(
        self, obj: InMemoryDataset, ref: DatasetRef, uris: Mapping[str, ResourcePath]
    ) -> dict[OpaqueTableName, list[StoredDatastoreItemInfo]]:
        """Write an in-memory object to this datastore.

        Parameters
        ----------
        obj
            Object to write.
        ref : `DatasetRef`
            Metadata used to identify the persisted object.  This should not
            have opaque records attached when passed, and [if we make
            DatasetRef mutable] it should have them attached on return.

        Returns
        -------
        records
            Records needed by the datastore in order to read the dataset.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        """Remove all stored artifacts associated with the given datasets.

        Notes
        -----
        Artifacts that do not exist should be silently ignored - that allows
        this method to be called to clean up after an interrupted operation has
        left artifacts in an unexpected state.

        The given `DatasetRef` object may or may not have opaque records
        attached.  If no records are attached implementations may assume that
        they would be the same as would be returned by calling `put` on these
        refs.
        """
        raise NotImplementedError()

    @abstractmethod
    def verify(self, ref: DatasetRef, paths: Mapping[StorageURI, ResourcePath]) -> bool:
        """Test whether all artifacts are present for a dataset.

        If all artifacts are present , return `True`.  If no artifacts are
        present, return `False`.  If only some artifacts are present or any
        artifact is corrupted, raise.
        """
        raise NotImplementedError()
