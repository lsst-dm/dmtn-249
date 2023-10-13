from __future__ import annotations

__all__ = ("Datastore", "DatastoreTableDefinition", "ArtifactTransferResponse", "DatastoreConfig")

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, TypeAlias, TYPE_CHECKING, final

from lsst.resources import ResourcePath

from lsst.daf.butler import StoredDatastoreItemInfo, ddl
from .extension_config import ExtensionConfig
from .aliases import GetParameter, InMemoryDataset, DatastoreTableName, StorageURI
from .artifact_transfer_request import ArtifactTransferRequest
from .primitives import DatasetRef

if TYPE_CHECKING:
    from .persistent_limited_butler import PersistentLimitedButler


class Datastore(ABC):
    """Interface for the polymorphic butler component that stores dataset
    contents.
    """

    @property
    @abstractmethod
    def tables(self) -> Mapping[DatastoreTableName, DatastoreTableDefinition]:
        """Database tables needed to store this Datastore's records."""
        raise NotImplementedError()

    @abstractmethod
    def get_root(self, datastore_name: str) -> ResourcePath | None:
        """Return the root URL for datasets stored in datastore with the given
        name.

        If the datastore name is not recognized by this datastore or any nested
        datastore, `None` should be returned.  If `None` is ultimately returned
        to the calling code that datastore may have only absolute URLs.
        """
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
        include both expanded data IDs and all possibly-relevant datastore
        table records.

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
    def initiate_transfer_from(
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
    def interpret_transfer_to(
        self,
        refs: Iterable[DatasetRef],
        requests: Iterable[ArtifactTransferRequest],
        origin: PersistentLimitedButler,
    ) -> list[ArtifactTransferResponse]:
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
        response
            Description of the artifacts to be transferred as interpreted by
            this datastore.  Embedded destination records may include signed
            URIs.
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_transfer_to(
        self,
        responses: list[ArtifactTransferResponse],
        destination_paths: Mapping[StorageURI, ResourcePath],
        origin: PersistentLimitedButler,
    ) -> None:
        """Actually execute transfers to this datastore.

        Parameters
        ----------
        responses : `list` [ `ArtifactTransferResponses` ]
            Responses returned by `interpret_transfer_to`.
        destination_paths
            Mapping from possibly-relative URI to definitely-absolute,
            signed-if-needed URL.
        origin
            Butler being transferred from.  May be used to `~LimitedButler.get`
            datasets to `~LimitedButler.put` them again, or to obtained signed
            origin `ResourcePath` objects in order to perform artifact
            transfers.
        """
        raise NotImplementedError()

    def serialize_transfer_to(self, responses: list[ArtifactTransferResponse]) -> Any:
        """Serialize a list of transfer responses created by this datastore
        to JSON-friendly data.

        If all response instances are pydantic models or are otherwise
        recognized by pydantic, the ``responses`` list may returned as-is. This
        is assumed to be the case by the base class implementation.
        """
        return responses

    @abstractmethod
    def deserialize_transfer_to(self, data: Any) -> list[ArtifactTransferRequest]:
        """Deserialize a list of transfer responses created and then serialized
        by this datastore from JSON-friendly data.

        If all expected response types are pydantic models or are otherwise
        recognized by pydantic, this just needs to invoke
        `pydantic.TypeAdapter.validate_python` or similar using the right type.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_many(
        self,
        arg: Iterable[tuple[InMemoryDataset, DatasetRef]],
        /,
    ) -> None:
        """Write an in-memory object to this datastore.

        Parameters
        ----------
        arg
            Objects to write and the `DatasetRef` objects that should identify
            them.
        """
        raise NotImplementedError()

    @abstractmethod
    def verify(
        self, ref: DatasetRef, paths: Mapping[StorageURI, ResourcePath]
    ) -> tuple[bool, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        """Test whether all artifacts are present for a dataset and return
        records that represent them.

        Parameters
        ----------
        ref : ~collections.abc.Iterable` [ `DatasetRef` ]
            Dataset to verify.  May or may not have datastore records attached;
            if records are attached, they must be consistent with the artifact
            content (e.g. if checksums are present, they should be checked).
        paths : `~collections.abc.Mapping` [ `StorageURI`, `ResourcePath` ]
            Mapping from possibly-relative URI to definitely-absolute,
            signed-if-needed URL.

        Returns
        -------
        present : `bool`
            If all artifacts are present , `True`.  If no artifacts are
            present, `False`.  If only some artifacts are present or any
            artifact is corrupted an exception is raised.
        records
            Datastore records that should be attached to this `DatasetRef`.
            These must be created from scratch if none were passed in (the
            datastore may assume its configuration has not changed since the
            artifact(s) were written) and maybe augmented if incomplete (e.g.
            if sizes or checksums were absent and have now been calculated).
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

        The given `DatasetRef` object may or may not have datastore records
        attached.  If no records are attached the datastore may assume its
        configuration has not changed since the artifact(s) were written.
        """
        raise NotImplementedError()


@final
@dataclasses.dataclass
class DatastoreTableDefinition:
    """Definition of a database table used to store datastore records."""

    spec: ddl.TableSpec
    record_type: type[StoredDatastoreItemInfo]


ArtifactTransferResponse: TypeAlias = Any
"""A type alias for the datastore-internal type used to represent the response
to a sequence of `ArtifactTransferRequest`.

An `ArtifactTransferResponse` generally combines one or more
`ArtifactTransferResponse` instances with a datastore-specific representation
of how they will appear in the datastore.
"""


class DatastoreConfig(ExtensionConfig):
    """Configuration and factory for a `Datastore`."""

    @abstractmethod
    def make_datastore(self, root: ResourcePath | None) -> Datastore:
        """Construct a datastore from this configuration and the given root.

        The root is not stored with the configuration to encourage
        relocatability, and hence must be provided on construction in addition
        to the config.  The root is only optional if all nested datastores
        know their own absolute root or do not require any paths.
        """
        raise NotImplementedError()
