from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any, overload
import uuid
import dataclasses

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


@dataclasses.dataclass
class StorageSyncContext:
    refs: Mapping[uuid.UUID, DatasetRef]
    clean: set[uuid.UUID] = dataclasses.field(default_factory=dict)
    stored: dict[uuid.UUID, dict[OpaqueTableName, DatasetOpaqueRecordSet]] = dataclasses.field(
        default_factory=dict
    )
    unstored: dict[uuid.UUID, set[OpaqueTableName]] = dataclasses.field(default_factory=dict)


class Registry(ABC):
    """Interface for butler component that stores dimensions, dataset metadata,
    and relationships.
    """

    @abstractmethod
    def register_manifest(self, name: ManifestName) -> tuple[int, bool]:
        """Register a manifest for datasets about to be transferred to this
        data repository.

        Parameters
        ----------
        name : `str`
            Manifest name.  Should be prefixed with ``u/$USER`` for regular
            non-privileged users.

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
    def get_all_manifests(self) -> list[tuple[ManifestName, int]]:
        """Return all active manifests."""
        raise NotImplementedError()

    @abstractmethod
    def synchronize_storage(
        self,
        manifest_id: int,
        insertions: Iterable[DatasetRef] = (),
        modifications: Iterable[uuid.UUID] = (),
    ) -> AbstractContextManager[StorageSyncContext]:
        """Return a context manager for storing or unstoring dataset artifacts
        in an associated datastore.

        Parameters
        ----------
        manifest_id : `int`
            Unique identifier for an active manifest.
        insertions : `~collections.abc.Iterable` [ `DatasetRef` ], optional
            New datasets to insert into the registry and store in a datastore.
        modifications : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            modifications datasets whose datastore artifact storage will be
            modified.

        Returns
        -------
        context
            Context whose scope should be used to make datastore artifact
            storage changes.

        Notes
        -----
        On `~AbstractContextManager.__enter__`, within another database
        transaction:

        - All datasets in ``insertions`` are inserted into the registry's own
          ``dataset`` and ``dataset_tag_*`` tables.  UUID conflicts are
          ignoring, which amounts to treating anything here that already exists
          as if its UUID had been passed in ``modifications`` instead.  Other
          conflicts and foreign key violations cause failures (with full
          rollback) at this point.

        - All datasets in ``insertions`` or ``modifications`` are inserted into
          ``manifest_dataset``, with conflict resolution such that duplicate
          inserts of a dataset into the same manifest is ignored, but presence
          of the same dataset in different manifests is an error (with full
          rollback).

        - Query for datasets in ``manifest_dataset`` for this ``manifest_id``
          and return them in `StorageSyncContext`, including
          expanded data IDs and any datastore records currently present. This
          may return datasets that were previously present in the manifest but
          were not passed in via eithe ``insertions`` or ``modifications``.

        The caller is expected to actually perform datastore artifact writes
        inside the context manager's scope, and only exit that scope without
        raising if all writes were successful.  The caller should be prepared
        for artifacts for datasets that were not just passed in to already be
        present (and possibly corrupted/incomplete).

        On `~AbstractContextManager.__exit__`, within another database
        transaction:

        - Dataset opaque records in `StorageSyncContext.unstored`
          are deleted from the database.

        - Dataset opaque records in `StorageSyncContext.stored` are
          inserted into the database.  Repeated/conflicting inserts are an
          error, since this should only be possible due to logic bugs in the
          caller.  A dataset appear here while also appearing in
          `StorageSyncContext.unstored` to replace all of its
          records.

        - All datasets in any of `~StorageSyncContext.unstored`,
          `~StorageSyncContext.stored`, or
          `~StorageSyncContext.unchanged` are removed from
          ``manifest_dataset``.

        TODO

        Note that the exit behavior does not depend on whether an exception was
        raised. This allows code inside the context block to mark exceptions as
        being gracefully handled, by leaving them out of the two, while still
        allowing the exception to propagate up. This does mean that
        gracefully-handled errors still result in registry entries (but no
        datastore entries).  This makes it much easier for this method to be
        idempotent, which is more important than giving it strong exception
        safety, since we still maintain the data repository consistency
        guarantees.

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
