from __future__ import annotations

import dataclasses
from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, overload

from lsst.daf.butler import (
    DataCoordinate,
    DataId,
    DataIdValue,
    DimensionUniverse,
    StorageClass,
    Timespan,
    DimensionRecord,
)
from lsst.daf.butler.registry import CollectionSummary
from lsst.daf.butler.registry import RegistryDefaults as ButlerClientDefaults
from lsst.daf.butler.registry.interfaces import CollectionRecord
from lsst.resources import ResourcePath

from .aliases import (
    ArtifactTransactionName,
    CollectionPattern,
    CollectionName,
    DatasetTypeName,
    DimensionElementName,
    GetParameter,
    InMemoryDataset,
    StorageClassName,
)
from .transactions import ArtifactTransaction
from .limited_butler import LimitedButler
from .primitives import DatasetRef, DatasetType, DeferredDatasetHandle
from .datastore import Datastore


@dataclasses.dataclass
class ButlerClientCache:
    """Objects expected to be aggressively fetched and cached on the client
    side by all butlers.

    "Aggressively" here means "just download everything up front", and
    refreshing it all when we see a cache miss.

    Or at least that's the simple version we do now - we probably want to at
    least downgrade from "everything" to "everything the user is allowed to
    access" for collections, and refine dataset types to just those used in
    those collections.
    """

    dataset_types: Mapping[DatasetTypeName, DatasetType]
    collections: Mapping[CollectionName, tuple[CollectionRecord, CollectionSummary]]
    dimension_records: Mapping[DimensionElementName, Mapping[tuple[DataIdValue, ...], DimensionRecord]]


class Butler(LimitedButler):
    """Base class for full clients of butler data repositories."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Butler:
        raise NotImplementedError()

    _root: ResourcePath
    _cache: ButlerClientCache | None
    _defaults: ButlerClientDefaults
    _datastore: Datastore

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError("TODO")

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError("TODO")

    ###########################################################################
    # Full-butler-only attributes and miscellaneous methods, from both the
    # current Butler interface and the Registry public interface.
    ###########################################################################

    @property
    def collections(self) -> Sequence[CollectionName]:
        return self._defaults.collections

    @property
    def run(self) -> CollectionName | None:
        return self._defaults.run

    @property
    def data_id_defaults(self) -> DataCoordinate:
        return self._defaults.dataId

    def clear_caches(self) -> None:
        """Clear all local caches.

        This may be necessary to pick up new dataset types, collections, and
        governor dimension values added by other clients.
        """
        self._cache = None

    ###########################################################################
    #
    # Implementation of the LimitedButler interface with overloads for dataset
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
        storage_class: StorageClass | StorageClassName | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> InMemoryDataset:
        # This overload is not inherited from LimitedButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get(self, *args: Any, **kwargs: Any) -> InMemoryDataset:
        raise NotImplementedError("TODO: implement here by delegating to resolve_dataset and get_many.")

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
        expanded_refs = self.expand_existing_dataset_refs(refs)
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
        storage_class: StorageClass | StorageClassName | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> DeferredDatasetHandle:
        # This overload is not inherited from LimitedButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get_deferred(self, *args: Any, **kwargs: Any) -> DeferredDatasetHandle:
        raise NotImplementedError(
            "TODO: implement here by delegating to resolve_dataset and get_many_deferred."
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
        expanded_refs = self.expand_existing_dataset_refs(refs)
        return [
            (ref, parameters_for_ref, DeferredDatasetHandle(ref, self._datastore))
            for ref, parameters_for_ref in zip(expanded_refs, parameters)
        ]

    @overload
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetRef:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def put(
        self,
        obj: InMemoryDataset,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        storage_class: StorageClass | StorageClassName | None = None,
        run: CollectionName | None = None,
        **kwargs: DataIdValue,
    ) -> DatasetRef:
        # This overload is not inherited from LimitedButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def put(self, obj: InMemoryDataset, *args: Any, **kwargs: Any) -> DatasetRef:
        raise NotImplementedError(
            "TODO: implement here by delegating to expand_data_coordinate and put_many."
        )

    ###########################################################################
    #
    # Write operations that demonstrate artifact transactions.
    #
    # These are pedagogical pale shadows of the more complete interfaces we'll
    # need (which will differ only in being able to also modify a lot of
    # database-only content).  For example, there's no import or ingest because
    # those are not fundamentally different from transfer_from in terms of what
    # the receiving butler needs to do.
    #
    # These need to have different implementations in RemoteButler and
    # DirectButler, because the responsibility for ensuring consistency between
    # Datastore artifacts and opaque records has to move to the server in the
    # former, while it can only live in the client in the latter.
    #
    ###########################################################################

    @abstractmethod
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        # Signature is inherited, but here it accepts not-expanded refs.
        raise NotImplementedError()

    @abstractmethod
    def transfer_from(self, origin: Butler, refs: Iterable[DatasetRef], mode: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def unstore_datasets(
        self,
        refs: Iterable[DatasetRef],
    ) -> None:
        raise NotImplementedError()

    ###########################################################################
    #
    # Convenience interfaces that will delegate to query().
    #
    # These methods are all convenience wrappers that can delegate to the
    # public query() method (and the interfaces on the thing that returns) in
    # the future, instead of having their own separate implementations in
    # RemoteButler and DirectButler.
    #
    ###########################################################################

    def expand_data_coordinates(self, data_coordinates: Iterable[DataCoordinate]) -> Iterable[DataCoordinate]:
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    def expand_existing_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    def resolve_dataset(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        expand: bool = True,
        timespan: Timespan | None,
        exists: bool = True,
        **kwargs: DataIdValue,
    ) -> DatasetRef:
        """Resolve data IDs and dataset types into DatasetRefs.

        The `resolve_dataset` method is the new `Registry.findDataset`, as
        well as the entry point for all of the Butler logic that interprets
        non-standard data IDs (e.g. day_obs + seq_num).
        """
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    def refresh(self) -> None:
        """Refresh all client-side caches."""
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    ###########################################################################
    #
    # Full-butler-only query methods, very abbreviated here since being
    # read-only they're relevant here only as something other methods delegate
    # to.
    #
    ###########################################################################

    @abstractmethod
    def query(self, defer: bool = True) -> Any:
        """Power-user interface and implementation point for queries."""
        raise NotImplementedError()

    ###########################################################################
    #
    # Artifact transaction interface for ensuring consistency between database
    # and datastores.
    #
    ###########################################################################

    @abstractmethod
    def begin_transaction(
        self,
        transaction: ArtifactTransaction,
        name: ArtifactTransactionName | None = None,
    ) -> tuple[ArtifactTransaction, bool]:
        """Open a persistent transaction in this repository.

        Parameters
        ----------
        cls
            Transaction type object.
        transaction
            Object that knows how to commit or abandon the transaction, can
            [de]serialize itself from JSON data, and provide information about
            what it will modify.  Note that this instance cannot know the
            transaction name or ID.
        name
            Name of the transaction.  Should be prefixed with ``u/$USER`` for
            regular users.  If not provided, defaults to something unique with
            that prefix and the type of transaction.

        Returns
        -------
        transaction
            New transaction object.
        just_opened
            Whether this process actually opened the transaction (`True`) vs.
            finding an already-open identical one (`False`).  This is important
            for at least prompt processing.

        Notes
        -----
        This method begins by calling `ArtifactTransaction.get_header_data` and
        saving a `ArtifactTransactionHeader` instance with it to a
        config-defined location.  It then performs several database operations
        in a single transaction:

        - It inserts a row into a ``artifact_transaction`` table, generating
          the transaction ID either as an autoincrement primary key (unless we
          generate it as a UUID earlier).

        - It inserts rows into the ``artifact_transaction_run`` table,
          effectively locking those runs against writes from other artifact
          transactions (but not database transactions involving registry-only
          state).  This fails (due to unique constraints in
          ``artifact_transaction_run``) if those runs are already locked.

        - It removes all opaque records associated with the datasets in
          `transaction.unstored`.

        - It executes `transaction.initial_batch`.

        If the database commit fails or is never attempted, the butler should
        attempt to remove the header it just saved, but we cannot count on this
        always occurring. Butler implementations must hence ignore headers with
        no associated database entry until given an opportunity to clean them
        up via `vacuum_transactions`.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(self, name: ArtifactTransactionName, transaction: ArtifactTransaction | None = None) -> None:
        """Commit an existing transaction.

        Most users should only need to call this method when a previous
        (often implicit) commit failed and they want to retry.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon(self, name: ArtifactTransactionName, transaction: ArtifactTransaction | None = None) -> None:
        """Commit an existing transaction.

        Most users should only need to call this method when a previous
        (often implicit) commit failed and they want to retry.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_transactions(self) -> list[ArtifactTransactionName]:
        """Return the names of all active transactions that the current user
        has write access to.

        Notes
        -----
        Administrators are expected to use this to check that there are no
        active artifact transactions before any migration or change to central
        datastore configuration.  Active transactions are permitted to write to
        datastore locations without having the associated opaque records saved
        anywhere in advance, so we need to ensure the datastore configuration
        used to predict artifact locations is not changed while they are
        active.
        """
        raise NotImplementedError()

    @abstractmethod
    def vacuum_transactions(self) -> None:
        """Clean up any empty directories and persisted transaction headers not
        associated with an active transaction.
        """
        raise NotImplementedError()
