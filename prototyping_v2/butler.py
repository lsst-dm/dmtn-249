from __future__ import annotations

import dataclasses
import getpass
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Set
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, TypeVar, cast, overload

from lsst.daf.butler import DataCoordinate, DataId, DataIdValue, DimensionUniverse, StorageClass
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import (
    ColumnName,
    CollectionName,
    CollectionPattern,
    DatasetTypeName,
    GetParameter,
    InMemoryDataset,
    OpaqueTableName,
    ArtifactTransactionID,
    ArtifactTransactionName,
)
from .artifact_transfer import ArtifactTransferManifest, ArtifactTransferRequest
from .config import ButlerConfig, DatastoreConfig
from .import_transaction import ImportTransaction
from .minimal_butler import MinimalButler
from .primitives import (
    DatasetOpaqueRecordSet,
    DatasetRef,
    DatasetType,
    DeferredDatasetHandle,
    OpaqueRecordSet,
    SignedPermissions,
)
from .raw_batch import RawBatch
from .removal_transaction import RemovalTransaction
from .transactions import ArtifactTransactionManager


class Registry(ABC):
    """Interface for butler component that stores dimensions, dataset metadata,
    and relationships.
    """

    @abstractmethod
    def expand_data_coordinates(self, data_coordinates: Iterable[DataCoordinate]) -> Iterable[DataCoordinate]:
        raise NotImplementedError()

    @abstractmethod
    def expand_existing_dataset_refs(
        self, refs: Iterable[DatasetRef], sign: SignedPermissions
    ) -> Iterable[DatasetRef]:
        raise NotImplementedError()

    @abstractmethod
    def execute_batch(self, batch: RawBatch, artifact_transaction_id: ArtifactTransactionID | None) -> None:
        """Perform registry write operations within a single database
        transaction.

        Parameters
        ----------
        batch : `RawBatch`
            Serializable description of the operations to be performed.
        artifact_transaction_id : `int` or `None`, optional
            ID of an active artifact transaction that is being committed.  This
            must be provided if `batch.opaque_table_insertions` is not `None`.
            This artifact transaction is deleted as part of the transaction.

        Returns
        -------
        The registry http API should either not accept
        ``artifact_transaction_id`` at all or require special user permissions;
        it should only be passed by an implementation of
        `ArtifactTransactionManager.commit`, and science users should only be
        interacting with a client/server implementation of that that calls this
        registry method on the server.

        The registry is not responsible for checking that the batch's contents
        correspond to the runs locked by the artifact transaction; this is the
        transaction manager's job.
        """
        raise NotImplementedError()

    @abstractmethod
    def open_artifact_ransaction(
        self,
        name: ArtifactTransactionName,
        runs: Iterable[CollectionName],
        opaque_record_deletions: Mapping[OpaqueTableName, tuple[ColumnName, Set[Any]]],
    ) -> ArtifactTransactionID:
        """Insert rows representing an artifact transaction and optionally
        delete opaque records that transaction will take ownership of.

        Notes
        -----
        The registry http API should either not support this method at all or
        require special user permissions; it should only be passed by an
        implementation of `ArtifactTransactionManager.open`, and science
        users should only be interacting with a client/server implementation of
        that that calls this registry method on the server.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon_artifact_transaction(self, artifact_transaction_id: ArtifactTransactionID) -> None:
        """Delete rows that represent an artifact transaction.

        Notes
        -----
        The registry http API should either not support this method at all or
        require special user permissions; it should only be passed by an
        implementation of `ArtifactTransactionManager.abandon`, and science
        users should only be interacting with a client/server implementation of
        that that calls this registry method on the server.
        """
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


class Butler(MinimalButler):
    """Client for butler data repositories."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    _root: ResourcePath
    _config: ButlerConfig
    _registry: Registry
    _datastore: Datastore
    _transaction_manager: ArtifactTransactionManager

    @property
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError("TODO")

    @property
    def is_writeable(self) -> bool:
        raise NotImplementedError("TODO")

    ###########################################################################
    #
    # Implementation of the minimal interface with overloads for dataset
    # type + data ID arguments.
    #
    ###########################################################################

    @overload
    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def get(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> InMemoryDataset:
        # This overload is not inherited from MinimalButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get(self, *args: Any, **kwargs: Any) -> InMemoryDataset:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get(ref)``, which will delegate to `get_many`.
            """
        )

    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign=SignedPermissions.GET)
        return self._datastore.get_many(zip(list(expanded_refs), parameters))

    @overload
    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def get_deferred(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> DeferredDatasetHandle:
        # This overload is not inherited from MinimalButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get_deferred(self, *args: Any, **kwargs: Any) -> DeferredDatasetHandle:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get_deferred(ref)``, which will delegate to
            `get_many_deferred`.
            """
        )

    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign=SignedPermissions.GET)
        return [
            (ref, parameters_for_ref, DeferredDatasetHandle(ref, self._datastore))
            for ref, parameters_for_ref in zip(expanded_refs, parameters)
        ]

    ###########################################################################
    #
    # Writes operations that demonstrate transactions. These are pedagogical
    # pale shadows of the real thing.
    #
    ###########################################################################

    def transfer_from(self, origin: Butler, refs: Iterable[DatasetRef], mode: str) -> None:
        assert mode != "move", "Moves are not supported for datasets managed by another repo."
        refs_by_uuid = {}
        runs = set()
        for ref in origin._registry.expand_existing_dataset_refs(refs, sign=SignedPermissions.GET):
            refs_by_uuid[ref.uuid] = ref
            runs.add(ref.run)
        requests = origin._datastore.make_transfer_requests(refs_by_uuid.values(), mode)
        manifest = self._datastore.receive_transfer_requests(
            refs_by_uuid.values(), requests, origin._datastore
        )
        transaction_id = self._transaction_manager.open(
            ImportTransaction,
            data=ImportTransaction.make_header_data(
                RawBatch(), manifest, origin._root, origin._config.datastore
            ),
            runs=runs,
        )
        self._transaction_manager.commit(transaction_id)

    def unstore_datasets(
        self,
        refs: Iterable[DatasetRef],
    ) -> None:
        refs_by_uuid = {}
        runs = set()
        for ref in self._registry.expand_existing_dataset_refs(refs, sign=SignedPermissions.NONE):
            refs_by_uuid[ref.uuid] = ref
            runs.add(ref.run)
        transaction_id = self._transaction_manager.open(
            RemovalTransaction,
            data=RemovalTransaction(refs=refs_by_uuid).model_dump(),
            runs=runs,
        )
        self._transaction_manager.abandon(transaction_id)
