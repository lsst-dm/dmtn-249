from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any, overload
import enum
import uuid

from lsst.daf.butler import DataId, StorageClass, DataIdValue

from .aliases import (
    GetParameter,
    InMemoryDataset,
    DatasetTypeName,
    CollectionPattern,
    OpaqueTableName,
    ManifestName,
)
from .primitives import (
    DatasetRef,
    DeferredDatasetHandle,
    DatasetType,
    DatasetOpaqueRecordSet,
)
from .minimal_butler import MinimalButler


class ManifestOperation(enum.Enum):
    """Enumeration of the types of manifest used to maintain registry/datastore
    consistency.
    """

    INSERT = enum.auto()
    REMOVE = enum.auto()


class Registry(ABC):
    """Interface for butler component that stores dimensions, dataset metadata,
    and relationships.
    """

    @abstractmethod
    def register_manifest(self, name: ManifestName, operation: ManifestOperation) -> tuple[int, bool]:
        """Register a manifest for datasets about to be transferred to this
        data repository.

        Parameters
        ----------
        name : `str`
            Manifest name.  Should be prefixed with ``u/$USER`` for regular
            non-privileged users.
        operation : `ManifestOperation`
            The kind of operation the manifest should track.  This must match
            the operation of the existing manifest with this name.

        Returns
        -------
        manifest_id : `int`
            Integer ID for the manifest.
        inserted : `bool`
            `True` if the manifest was just added, `False` if it was already
            present.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_manifest(self, manifest_id: int) -> None:
        """Remove the given manifest.

        This will fail if the manifest still has datasets associated with it.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_manifests(self) -> list[tuple[ManifestName, int]]:
        """Return all active manifests."""
        raise NotImplementedError()

    @abstractmethod
    def storage_insertion_manifest(
        self, manifest_id: int, new_refs: Iterable[DatasetRef]
    ) -> AbstractContextManager[dict[uuid.UUID, DatasetRef]]:
        """Return a context manager for storing dataset artifacts in an
        associated datastore.

        Notes
        -----
        On `~AbstractContextManager.__enter__`, this inserts the given datasets
        into the registry and the ``manifest_dataset`` table, with the latter
        indicating that the artifacts for those datasets are either currently
        being written or a problem was encountered with those writes.  This
        happens within one database transaction, guaranteeing consistency and
        preventing any other process from attempting to write any datasets with
        the same UUIDs concurrently.  Errors with invalid or conflicting data
        IDs will occur at this stage, before any datastore writes are
        attempted.  If a UUID conflict occurs, the new dataset is ignored, on
        the *assumption* (difficult for us to check, but guaranteed outside of
        pathological cases by the properties of UUIDs) that it has the same
        dataset type and data ID, and we are attempting to now attempting file
        artifacts for that existing dataset.  The data IDs of given
        `DatasetRef` objects *and* all datasets present in this manifest
        returned with fully expanded data IDs and any records they already
        have (allowing storage in new datastores to be added).

        The caller is expected to actually perform datastore artifact writes
        inside the context manager's scope, and only exit that scope without
        raising if all writes were successful.  The caller should be prepared
        for artifacts for datasets that were not just passed in to already be
        present (and possibly corrupted/incomplete).

        On `~AbstractContextManager.__exit__`, the `dict` returned by
        `~AbstractContextManager.__enter__` is compared to the contents of the
        ``manifest_table``:

        - Datasets that now have opaque records attached will have those
          records inserted into the database, marking them as stored in the
          datastore.  These datasets are removed from the manifest.

        - Datasets that do not have opaque records attached will be ignored,
          leaving them in the registry but marked as unstored by any datastore.
          These datasets are also removed from the manifest.

        - Datasets that have been fully removed from the `dict` are ignored and
          left in the manifest.

        Callers must not remove opaque records from datasets that already have
        them attached; use `storage_removal_manifest` instead.

        Note that the exit behavior does not depend on whether an exception was
        raised, to allow code inside the context block to mark exceptions as
        being gracefully handled, by leaving them in the returned `dict`
        without opaque records, while still allowing the exception to propagate
        up. This does mean that gracefully-handled errors still result in
        registry entries (but no datastore entries).  This makes it much easier
        for this method to be idempotent, which is more important than giving
        it strong exception safety, since we still maintain the data repository
        consistency guarantees.

        A useful pattern for code inside this context is to `~dict.pop` each
        dataset off of the returned `dict` just before its artifacts are
        written and to add it back in with opaque records attached immediately
        after the write succeeds.  This will cause lower-level I/O errors to
        have the desired effect.

        If a hard failure occurs during the context block that prevents the
        exit code from running, the ``manifest_dataset`` table continues to
        record all datasets that *might* be present, allowing subsequent calls
        to complete the writes or clean up the mess.
        """
        raise NotImplementedError()


class Datastore(ABC):
    """Interface for butler component that stores dataset contents."""

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
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> dict[OpaqueTableName, DatasetOpaqueRecordSet]:
        """Write an in-memory object as a new dataset.

        Parameters
        ----------
        obj
            Object to save.
        ref : `DatasetRef`
            Dataset reference with a fully expanded data ID but no datastore
            records.

        Returns
        -------
        opaque_records
            Opaque datastore records to be saved in the registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def cleanup(self, refs: Iterable[DatasetRef]) -> None:
        """Remove the given datasets, without assuming that they were
        successfully written.

        Missing datasets should be silently ignored.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to remove.  These may or may not have opaque records
            attached.  If they do not, implementations may assume they would
            match those returned by `put` for the same `DatasetRef`.
        """
        raise NotImplementedError()


class Butler(MinimalButler):
    """Client for butler data repositories."""

    _registry: Registry
    _datastore: Datastore

    ###########################################################################
    #
    # Implementation of the Minimal interface with overloads for dataset
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
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign_for_get=True)
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
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign_for_get=True)
        return [
            (ref, parameters_for_ref, DeferredDatasetHandle(ref, self._datastore))
            for ref, parameters_for_ref in zip(expanded_refs, parameters)
        ]
