from __future__ import annotations

import dataclasses
from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence, Set
from datetime import datetime, timedelta
from typing import Any, ClassVar, final, overload

from lsst.daf.butler import (
    ButlerConfig,
    DataCoordinate,
    DataId,
    DataIdValue,
    DatasetId,
    DatasetType,
    DeferredDatasetHandle,
    DimensionRecord,
    DimensionUniverse,
    StorageClass,
    Timespan,
)
from lsst.daf.butler.registry import CollectionSummary
from lsst.daf.butler.registry import RegistryDefaults as ButlerClientDefaults
from lsst.daf.butler.registry.interfaces import CollectionRecord
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import (
    CollectionName,
    CollectionPattern,
    DatasetTypeName,
    DimensionElementName,
    GetParameter,
    InMemoryDataset,
    StorageClassName,
    StorageURI,
)
from .artifact_transaction import ArtifactTransaction, ArtifactTransactionName
from .datastore import Datastore
from .limited_butler import LimitedButler
from .primitives import DatasetRef


class UnfinishedTransactionError(RuntimeError):
    """Exception raised when an operation failed while leaving an
    artifact transaction open.
    """


class PathMapping(Mapping[StorageURI, ResourcePath]):
    def __init__(
        self,
        uris: Set[StorageURI],
    ):
        self._uris = uris

    def __iter__(self) -> Iterator[StorageURI]:
        return iter(self._uris)

    def __contains__(self, __key: object) -> bool:
        return __key in self._uris

    def __len__(self) -> int:
        return len(self._uris)

    def __getitem__(self, __key: StorageURI) -> ResourcePath:
        root, relative = __key
        if root is not None:
            return root.join(relative)
        else:
            # TODO: I'm not sure forceAbsolute=True is appropriate - we require
            # the result to be absolute, but we want it to raise if it isn't
            # already, rather than assume the current directory is relevant.
            return ResourcePath(relative, forceAbsolute=True)


class SignedPathMapping(PathMapping):
    def __init__(
        self,
        uris: Set[StorageURI],
        sign: Callable[[Iterable[StorageURI]], tuple[dict[StorageURI, ResourcePath], datetime]],
    ):
        super().__init__(uris)
        self._sign = sign
        self._cache, self._expiration = sign(self._uris)

    _PADDING: ClassVar[timedelta] = timedelta(seconds=5)

    def __getitem__(self, __key: StorageURI) -> ResourcePath:
        if self._expiration + self._PADDING < datetime.now():
            self._cache, self._expiration = self._sign(self.keys())
        return self._cache[__key]


class Butler(LimitedButler):
    """Base class for full clients of butler data repositories."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Butler:
        # This is a placeholder for the real signature, which will need to
        # be able to return the appropriate concrete Butler subclass while
        # accepting arguments all implementations must accept.
        raise NotImplementedError()

    _cache: ButlerClientCache | None
    _datastore: Datastore
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
    def put(
        self,
        obj: InMemoryDataset,
        ref: DatasetRef,
        transaction_name: ArtifactTransactionName | None = None,
    ) -> None:
        # Signature is mostly inherited, but here it accepts not-expanded refs.
        # If transaction_name is provided, concurrent calls with the same name
        # pick a single winner, with the losers doing nothing.
        ...

    @overload
    def put(
        self,
        obj: InMemoryDataset,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        transaction_name: ArtifactTransactionName | None = None,
        storage_class: StorageClass | StorageClassName | None = None,
        run: CollectionName | None = None,
        **kwargs: DataIdValue,
    ) -> None:
        # This overload is not inherited from LimitedButler, but it's unchanged
        # from what we have now except for snake_case. If transaction_name is
        # provided, concurrent calls with the same name pick a single winner,
        # with the losers doing nothing.
        ...

    @final
    def put(
        self,
        obj: InMemoryDataset,
        *args: Any,
        transaction_name: ArtifactTransactionName | None = None,
        **kwargs: Any,
    ) -> None:
        # If transaction_name is provided, concurrent calls with the same name
        # pick a single winner, with the losers doing nothing.
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
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]]) -> None:
        # Signature is inherited, but here it accepts not-expanded refs. If
        # transaction_name is provided, concurrent calls with the same name
        # pick a single winner, with the losers doing nothing.
        from .put_transaction import PutTransaction

        refs: dict[DatasetId, DatasetRef] = {}
        objs: list[InMemoryDataset] = []
        data_ids: dict[tuple[DataIdValue, ...], DataCoordinate] = {}
        for obj, ref in arg:
            refs[ref.id] = ref
            data_ids[ref.dataId.values_tuple()] = ref.dataId
            objs.append(obj)
        # Expand the data IDs associated with all refs.
        data_ids = {
            data_id.values_tuple(): data_id for data_id in self.expand_data_coordinates(data_ids.values())
        }
        for ref in refs.values():
            refs[ref.id] = dataclasses.replace(ref, dataId=data_ids[ref.dataId.values_tuple()])
        # Open a transaction.
        transaction = PutTransaction(refs=refs)
        transaction_name, _ = self.begin_transaction(transaction)
        try:
            uris = self._datastore.predict_new_uris(refs.values())
            self._datastore.put_many(zip(objs, refs.values()), paths=self._get_resource_paths(uris))
            self.commit_transaction(transaction_name)
        except BaseException as main_err:
            try:
                self.revert_transaction(transaction_name)
            except BaseException:
                raise UnfinishedTransactionError(
                    "Dataset write failed (see chained exception for details) and could not be reverted. "
                    f"Artifact transaction {transaction_name!r} must be manually committed, reverted, "
                    "or abandoned after any low-level problems with the repository have been addressed."
                ) from main_err
            raise

    @final
    def remove_datasets(
        self,
        refs: Iterable[DatasetRef],
        purge: bool = False,
    ) -> None:
        """Remove datasets.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets whose artifacts should be deleted.
        purge : `bool`, optional
            Whether to un-register datasets in addition to removing their
            stored artifacts.
        transaction_name : `str`, optional
            Name of the transaction.  If provided, concurrent calls with the
            same transaction will pick a single winner with the result doing
            nothing.  The caller is responsible for ensuring all such calls
            would actually do the same thing..
        """
        from .removal_transaction import RemovalTransaction

        transaction = RemovalTransaction(
            refs={ref.id: ref for ref in self.expand_existing_dataset_refs(refs)}, purge=purge
        )
        transaction_name, _ = self.begin_transaction(transaction)
        try:
            self.commit_transaction(transaction_name)
        except BaseException as main_err:
            try:
                self.revert_transaction(transaction_name)
            except BaseException:
                raise UnfinishedTransactionError(
                    "Dataset removal failed (see chained exception for details) and could not be reverted. "
                    f"Artifact transaction {transaction_name!r} must be manually committed, reverted, "
                    "or abandoned after any low-level problems with the repository have been addressed."
                ) from main_err
            raise

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
            Object that knows how to commit, revert, or abandon the
            transaction can [de]serialize itself from JSON data, and provide
            information about what it will modify.
        name
            Name of the transaction.  Should be prefixed with ``u/$USER`` for
            regular users.  If not provided, defaults to something that is
            guaranteed to be universally unique with that prefix.

        Returns
        -------
        name
            Name of the transaction.
        just_opened
            Whether this process actually opened the transaction (`True`) vs.
            finding an already-open identical one (`False`).
        """
        raise NotImplementedError()

    @abstractmethod
    def commit_transaction(self, name: ArtifactTransactionName) -> None:
        """Commit an existing transaction.

        Most users should only need to call this method when a previous (often
        implicit) commit failed and they want to retry.

        This method will raise an exception and leave the transaction open if
        it cannot fully perform all of the operations originally included in
        the transaction.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert_transaction(self, name: ArtifactTransactionName) -> bool:
        """Revert an existing transaction.

        Most users should only need to call this method when a previous (often
        implicit) commit failed and they want to retry.

        This method will raise an exception and leave the transaction open if
        it cannot fully undo any modifications made by the transation since it
        was opened.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon_transaction(self, name: ArtifactTransactionName) -> None:
        """Abandon an existing transaction.

        Most users should only need to call this method when a previous (often
        implicit) commit failed and they want to retry.

        This method will only raise (and leave the transaction) open if it
        encounters a low-level error, but it may leave the transaction's
        operations incomplete (with warnings logged).  Repository invariants
        will still be satisfied.
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

    @classmethod
    @abstractmethod
    def make_workspace_client(
        cls, config: ButlerConfig | ResourcePathExpression, name: ArtifactTransactionName
    ) -> Any:
        """Make a workspace client for the given transaction.

        Notes
        -----
        This is a `classmethod` to allow it to avoid the need to connect to a
        database or REST server (as `Butler` construction typically does):
        workspace transactions are serialized to a JSON file whose path should
        be deterministic given butler config and the transaction name, and the
        `Datastore` should also be constructable from butler config alone.

        If the serialized transaction does not exist at the expected location,
        this method will have to connect to a server to see if the transaction
        exists there; if it does, the serialized transaction will be written to
        the file location for future calls.  If it does not, the transaction
        has been closed an an exception is raised.  This behavior guards
        against unexpected failures in either opening or closing a workspace
        transaction.
        """
        raise NotImplementedError()

    @abstractmethod
    def vacuum_workspaces(self) -> None:
        """Clean up any workspace directories not associated with an active
        transaction.

        This method is only needed because:

         - we have to write transaction headers to workspace roots before
           inserting the transaction into the database when it is opened;
         - we have to delete workspace roots after deleting the transaction
           from the database on commit or abandon;

        and in both cases we can't guarantee we won't be interrupted.  But
        vacuums should only be needed extremely rarely.
        """
        raise NotImplementedError()

    def _get_resource_paths(
        self, uris: Iterable[StorageURI], write: bool = False
    ) -> Mapping[StorageURI, ResourcePath]:
        """Return `ResourcePath` objects usable for direct Datastore operations
        for the given possibly-relative URIs.

        This turns URIs into absolute URLs and will sign them if needed for
        this repository.
        """
        return PathMapping(frozenset(uris))


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
