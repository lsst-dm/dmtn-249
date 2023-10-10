from __future__ import annotations

import dataclasses
from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, overload, final

from lsst.daf.butler import (
    DataCoordinate,
    DataId,
    DataIdValue,
    DimensionUniverse,
    StorageClass,
    DatasetType,
    Timespan,
    DimensionRecord,
    DeferredDatasetHandle,
)
from lsst.daf.butler.registry import CollectionSummary
from lsst.daf.butler.registry import RegistryDefaults as ButlerClientDefaults
from lsst.daf.butler.registry.interfaces import CollectionRecord
from lsst.resources import ResourcePath

from .aliases import (
    CollectionPattern,
    CollectionName,
    DatasetTypeName,
    DimensionElementName,
    GetParameter,
    InMemoryDataset,
    StorageClassName,
    StorageURI,
)
from .artifact_transaction import ArtifactTransaction, ArtifactTransactionName
from .persistent_limited_butler import PersistentLimitedButler, PersistentLimitedButlerConfig
from .primitives import DatasetRef


class Butler(PersistentLimitedButler):
    """Base class for full clients of butler data repositories."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Butler:
        # This is a placeholder for the real signature, which will need to
        # be able to return the appropriate concrete Butler subclass while
        # accepting arguments all implementations must accept.
        raise NotImplementedError()

    _cache: ButlerClientCache | None
    _defaults: ButlerClientDefaults
    _universe: DimensionUniverse

    @final
    @property
    def dimensions(self) -> DimensionUniverse:
        return self._universe

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError("TODO")

    ###########################################################################
    # Full-butler-only attributes and miscellaneous methods, from both the
    # current Butler interface and the Registry public interface.
    ###########################################################################

    @final
    @property
    def collections(self) -> Sequence[CollectionName]:
        return self._defaults.collections

    @final
    @property
    def run(self) -> CollectionName | None:
        return self._defaults.run

    @final
    @property
    def data_id_defaults(self) -> DataCoordinate:
        return self._defaults.dataId

    @final
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

    @final
    def get(self, *args: Any, **kwargs: Any) -> InMemoryDataset:
        raise NotImplementedError("TODO: implement here by delegating to resolve_dataset and get_many.")

    @final
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
        paths = self._get_resource_paths(self._datastore.extract_existing_uris(expanded_refs))
        return self._datastore.get_many(zip(list(expanded_refs), parameters), paths)

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

    @final
    def get_deferred(self, *args: Any, **kwargs: Any) -> DeferredDatasetHandle:
        raise NotImplementedError(
            "TODO: implement here by delegating to resolve_dataset and get_many_deferred."
        )

    @final
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
            (ref, parameters_for_ref, DeferredDatasetHandle(self, ref, parameters_for_ref))  # type: ignore
            for ref, parameters_for_ref in zip(expanded_refs, parameters)
        ]

    @overload
    def get_uri(self, ref: DatasetRef) -> StorageURI:
        ...

    @overload
    def get_uri(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> StorageURI:
        ...

    @final
    def get_uri(self, *args: Any, **kwargs: Any) -> StorageURI:
        raise NotImplementedError("TODO: implement here by delegating to resolve_dataset and get_many_uris.")

    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, StorageURI]]:
        result = []
        for ref in self.expand_existing_dataset_refs(refs):
            for uri in self._datastore.extract_existing_uris([ref]):
                result.append((ref, uri))
        return result

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

    @final
    def put(self, obj: InMemoryDataset, *args: Any, **kwargs: Any) -> DatasetRef:
        raise NotImplementedError("TODO: implement here by delegating to resolve_dataset and put_many.")

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
    ###########################################################################

    @final
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        # Signature is inherited, but here it accepts not-expanded refs.
        raise NotImplementedError("TODO")

    @final
    def transfer_artifacts_from(
        self, origin: PersistentLimitedButler, refs: Iterable[DatasetRef], mode: str
    ) -> None:
        """Transfer dataset artifacts from another butler to this one.

        Parameters
        ----------
        origin : `PersistentLimitedButler`
            Butler to transfer from.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets whose artifacts should be transferred.
        mode : `str`
            Transfer mode.  Note that `None` is not supported, because directly
            writing to a datastore-managed location without an open transaction
            is now illegal.

        Notes
        -----
        For simplicity this method assumes the given datasets already exist in
        the destination database but are not stored there, so only their
        artifacts and datastore records need to be transferred.  The extension
        to more realistic cases just involves constructing a `RawBatch`
        instance and passing it through the transaction object.
        """
        from .transfer_transaction import TransferTransaction

        refs_dict = {ref.id: ref for ref in refs}
        requests = origin._datastore.initiate_transfer_from(refs_dict.values(), mode)
        responses = self._datastore.interpret_transfer_to(refs_dict.values(), requests, origin)
        transaction = TransferTransaction(
            responses=responses,
            origin_root=origin._root,
            origin_config=origin._config,
            refs=refs_dict,
        )
        transaction_name, _ = self.begin_transaction(transaction)
        self.commit(transaction_name)

    @final
    def unstore_datasets(self, refs: Iterable[DatasetRef]) -> None:
        """Remove datasets from storage.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets whose artifacts should be deleted.

        Notes
        -----
        For simplicity this method should be deleted from datastore storage but
        not the database.  The extension to more realistic cases just involves
        constructing a `RawBatch` instance and passing it through the
        transaction object.
        """
        from .removal_transaction import RemovalTransaction

        transaction = RemovalTransaction(refs={ref.id: ref for ref in refs})
        transaction_name, _ = self.begin_transaction(transaction)
        self.commit(transaction_name)

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

    @final
    def expand_data_coordinates(self, data_coordinates: Iterable[DataCoordinate]) -> Iterable[DataCoordinate]:
        """Expand data IDs to include all relevant dimension records."""
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    @final
    def expand_existing_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Expand DatasetRefs to include all relevant dimension records and
        datastore records.
        """
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    @final
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

    @final
    def refresh(self) -> None:
        """Refresh all client-side caches."""
        raise NotImplementedError("TODO: implement here by delegating to query() and _cache.")

    ###########################################################################
    #
    # Full-butler-only query methods, very abbreviated here, since being
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
    ) -> tuple[ArtifactTransactionName, bool]:
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
        name
            Name fo the transaction.
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

        - It removes all datastore records associated with the datasets in
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
        datastore locations without having the associated datastore records
        saved anywhere in advance, so we need to ensure the datastore
        configuration used to predict artifact locations is not changed while
        they are active.
        """
        raise NotImplementedError()

    @abstractmethod
    def vacuum_transactions(self) -> None:
        """Clean up any empty directories and persisted transaction headers not
        associated with an active transaction.
        """
        raise NotImplementedError()


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


class ButlerConfig(PersistentLimitedButlerConfig):
    """Configuration and factory for a `Butler`."""

    @abstractmethod
    def make_butler(self, root: ResourcePath | None) -> Butler:
        """Construct a butler from this configuration and the given root.

        The root is not stored with the configuration to encourage
        relocatability, and hence must be provided on construction in addition
        to the config.  The root is only optional if all nested datastores
        know their own absolute root or do not require any paths.
        """
        raise NotImplementedError()
