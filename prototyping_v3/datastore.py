from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .artifact_transfer import ArtifactTransferManifest, ArtifactTransferRequest
from .extension_config import ExtensionConfig
from .opaque import DatasetOpaqueRecordSet, OpaqueRecordSet
from .primitives import DatasetRef


class DatastoreConfig(ExtensionConfig):
    @classmethod
    @abstractmethod
    def make_datastore(cls, root: ResourcePath) -> Datastore:
        raise NotImplementedError()


class Datastore(ABC):
    """Interface for butler component that stores dataset contents."""

    config: DatastoreConfig
    """Configuration that can be used to reconstruct this datastore.
    """

    @property
    @abstractmethod
    def opaque_record_set_type(self) -> type[OpaqueRecordSet]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def dataset_opaque_record_set_type(self) -> type[DatasetOpaqueRecordSet]:
        raise NotImplementedError()

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
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
        self, refs: Iterable[DatasetRef], requests: Iterable[ArtifactTransferRequest], origin: Datastore
    ) -> ArtifactTransferManifest:
        """Create a manifest that records how this datastore would receive the
        given artifacts from another datastore.

        This does not actually perform any artifact writes.

        Parameters
        ----------
        TODO: make refs a convenience collection
        requests : `~collections.abc.Iterable` [ `ArtifactTransferRequest` ]
            Artifacts according to the origin datastore.  Minimal-effort
            transfers - like file copies - preserve artifacts, but in the
            general case transfers only need to preserve datasets.
        origin : `Datastore`
            Datastore that owns or at least knows how to read the datasets
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
        self, manifest: ArtifactTransferManifest, origin: Datastore
    ) -> OpaqueRecordSet:
        """Actually execute transfers to this datastore.

        Notes
        -----
        The manifest may include signed URIs, but this is not guaranteed and
        the datastore should be prepared to refresh them if they are absent
        or expired.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon_transfer_manifest(self, manifest: ArtifactTransferManifest) -> None:
        """Delete artifacts from a manifest that has been partially
        transferred.

        Notes
        -----
        The manifest may include signed URIs, but this is not guaranteed and
        the datastore should be prepared to refresh them if they are absent
        or expired.
        """
        raise NotImplementedError()

    @abstractmethod
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetOpaqueRecordSet:
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
        records : `DatasetOpaqueRecordSet`
            Records needed by the datastore in order to read the dataset.

        Notes
        -----
        The fact that no signed URIs are passed here enshrines the idea that a
        signed-URI datastore will always need to be able to go get them itself,
        at least for writes.
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
        attached, and if records are attached, they may or may not have signed
        URIs (though if signed URIs are attached, they are guaranteed to be
        signed for delete and existence-check operations), and signed URIs may
        need to be refreshed.  If no records are attached implementations may
        assume that they would be the same as would be returned by calling
        `put` on these refs.
        """
        raise NotImplementedError()

    @abstractmethod
    def verify(self, ref: DatasetRef) -> bool:
        """Test whether all artifacts are present for a dataset.

        If all artifacts are present and all checksums embedded in records are
        are correct, return `True`.  If no artifacts are present, return
        `False`.  If only some artifacts are present or checksums fail, raise.
        """
        raise NotImplementedError()
