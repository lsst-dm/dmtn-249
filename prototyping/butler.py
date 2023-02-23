from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence, Set
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import Any, overload

from lsst.daf.butler import (
    CollectionType,
    Config,
    DataCoordinate,
    DataId,
    DataIdValue,
    DatasetIdFactory,
    DeferredDatasetHandle,
    DimensionRecord,
    DimensionUniverse,
    FileDataset,
    StorageClass,
    StorageClassFactory,
    Timespan,
)
from lsst.daf.butler.registry import CollectionTypeError, MissingCollectionError, MissingDatasetTypeError
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import (
    CollectionDocumentation,
    CollectionName,
    CollectionPattern,
    DatasetTypeName,
    DatasetTypePattern,
    DimensionElementName,
    DimensionName,
    GetParameter,
    InMemoryDataset,
    StorageClassName,
)
from .batch_helper import BatchHelper
from .butler_extractor import ButlerExtractor
from .datastore import DatastoreConfig
from .datastore_butler import DatastoreButler
from .limited_butler import LimitedButler, LimitedButlerExtractor
from .primitives import DatasetRef, DatasetType, SequenceEditMode, SetEditMode, SetInsertMode
from .queries import (
    CollectionQuery,
    DataCoordinateQueryAdapter,
    DatasetQueryChain,
    DatasetTypeQuery,
    DimensionRecordQueryAdapter,
    Query,
)
from .raw_batch import RawBatch
from .registry import Registry
from .removal_helper import RemovalHelper


class Butler(DatastoreButler):
    """A fully-featured concrete Butler that is backed by a (private) Registry
    and Datastore, constructable from just a repository URI.

    Butler is a concrete class that directly holds a private `Registry`
    instance and indirectly (via its base class) holds a private `Datastore`
    instance.  The interface is different from the current Butler's for several
    different reasons:

    - Some changes first appear in `LimitedButler` (see that class for
      details), especially those from RFC-888.

    - I've used `typing.overload` to expand out the multiple-argument-type
      support in get/put/etc, so these look like they've changed more than
      they actually have.

    - Butler has absorbed much of the Registry public interface, since its
      registry attribute is going private.

    - Since we cannot support transactions in the same way any more, I've
      reoriented write operations around the new `BatchHelper` (public) and
      `RawBatch` classes, which allow many operations to be bundled up (and
      sent to a butler server, if there is one) to be executed in a single
      transaction.  The `BatchHelper` interface also uses context managers, but
      now the methods are on the context object returned, and the context
      lifetime doesn't affect the parent `Butler` object at all.

    - I've attempted to unify the interfaces and logic of `ingest` and
      especially `import`, `export`, and `transfer_from`.  In particular,
      `transfer_from` now uses a context manager (similar to the one `export`
      currently uses) to control what's transferred.

    - The query interface has been reworked to take advantage of some of the
      things I think ``daf_relation`` will (finally) let us do, and that
      includes a new more powerful `query` method (see the `Query` class it
      returns for more details) as well as forwarders that mostly mimic what
      the old query system could do before.

    See the directory README for more notes on conventions and caveats.
    """

    _registry: Registry
    """Registry that backs this Butler.
    """

    @staticmethod
    def make_repo(
        root: ResourcePathExpression,
        config: Config | ResourcePathExpression | None = None,
        dimensions: Config | ResourcePathExpression | None = None,
        standalone: bool = False,
        search_paths: list[ResourcePathExpression] | None = None,
        force_config_root: bool = True,
        outfile: ResourcePathExpression | None = None,
        overwrite: bool = False,
    ) -> Config:
        """Create a new repo.

        This is unchanged from what we have now, aside from more consistent
        ResourcePathExpression usage and snake_case.

        But do we actually need all of these arguments?  Or are some of them
        now the "old way" to solve certain problems, and we could consider
        removing them?
        """
        raise NotImplementedError("Implementation should be unchanged, or changed very little.")

    @staticmethod
    def get_repo_uri(label: str) -> ResourcePath:
        raise NotImplementedError("Implementation should be unchanged, or changed very little.")

    @staticmethod
    def get_known_repos() -> Set[str]:
        raise NotImplementedError("Implementation should be unchanged, or changed very little.")

    def __init__(
        self,
        config: Config | ResourcePathExpression | None = None,
        *,
        butler: Butler = None,
        collections: CollectionPattern = None,
        run: CollectionName = None,
        search_paths: list[ResourcePathExpression] = None,
        writeable: bool | None = None,
        infer_defaults: bool = True,
        **kwargs: DataIdValue,
    ):
        """Create a new repo.

        This is unchanged from what we have now, aside from more consistent
        ResourcePathExpression usage and snake_case.

        But do we actually need all of these arguments?  Or are some of them
        now the "old way" to solve certain problems, and we could consider
        removing them?
        """
        raise NotImplementedError("Implementation should be unchanged, or changed very little.")

    ###########################################################################
    #
    # Implementation of the LimitedButler interface with overloads for dataset
    # type + data ID arguments.
    #
    ###########################################################################

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
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().put(obj, ref)``, which will delegate to `put_many`.
            """
        )

    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        # Signature is inherited, but here it accepts not-expanded refs.
        # Unzip arguments to expand DatasetRefs and then re-zip.
        objs_by_uuid = {}
        refs = []
        for obj, ref in arg:
            objs_by_uuid[ref.uuid] = obj
            refs.append(ref)
        expanded_refs = self._registry.expand_new_dataset_refs(refs, sign=True)
        pairs = [(objs_by_uuid[ref.uuid], ref) for ref in expanded_refs]
        raw_batch = RawBatch()
        raw_batch.dataset_insertions.include(expanded_refs)
        # We can't delegate to super because we need to use the transactional
        # version of the Datastore API to ensure consistency via journal files.
        with self._datastore.put_many_transaction(pairs) as opaque_table_rows:
            raw_batch.opaque_table_insertions.update(opaque_table_rows)
            self._registry.apply_batch(raw_batch)
        return raw_batch.opaque_table_insertions.attach_to(refs)

    def predict_put_many(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Return an iterable of `DatasetRef` objects that have been augmented
        with datastore records as if they had been passed to `put_many`.

        This is intended to be used by QuantumGraph generation to pre-populate
        datastore records for intermediates as well as inputs.
        """
        # Not sure we care one way or another about signing here.
        return self._registry.expand_new_dataset_refs(refs, sign=True)

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
        # This overload is not inherited from LimitedButler, but it's unchanged
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
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        refs = list(self._registry.expand_existing_dataset_refs(refs, sign_for_get=True))
        return super().get_many(zip(refs, parameters))

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
        # This overload is not inherited from LimitedButler, but it's unchanged
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
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        refs = list(self._registry.expand_existing_dataset_refs(refs, sign_for_get=True))
        return super().get_many_deferred(zip(refs, parameters))

    @overload
    def get_uri(self, ref: DatasetRef) -> ResourcePath:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def get_uri(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> ResourcePath:
        # This overload is not inherited from LimitedButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get_uri(self, *args: Any, **kwargs: Any) -> ResourcePath:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get_uri(ref)``, which will delegate to
            `get_many_uri`.
            """
        )

    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        # Do we want signed URIs here?  Should that be an option for the user?
        return super().get_many_uris(self._registry.expand_existing_dataset_refs(refs))

    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        # Signature is inherited, but here it accepts not-expanded refs.  Note
        # that this method still does Datastore-only removals, but here that
        # includes removing Datastore records from Registry.  See
        # BatchHelper.removal for dataset and collection removals.
        refs = self._registry.expand_existing_dataset_refs(refs, sign_for_unstore=True)
        raw_batch = RawBatch()
        with self._datastore.unstore_transaction(refs) as opaque_table_keys:
            raw_batch.opaque_table_removals.update(opaque_table_keys)
            self._registry.apply_batch(raw_batch)

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._registry.dimensions

    @property
    def is_writeable(self) -> bool:
        return self._registry.is_writeable  # constructor should ensure Datastore agrees

    ###########################################################################
    # Full-butler-only attributes and miscellaneous methods, from both the
    # current Butler interface and the Registry public interface.
    ###########################################################################

    @property
    def collections(self) -> Sequence[CollectionName]:
        return self._registry.defaults.collections

    @property
    def run(self) -> CollectionName | None:
        return self._registry.defaults.run

    @property
    def data_id_defaults(self) -> DataCoordinate:
        return self._registry.defaults.dataId

    @property
    def storage_class_factory(self) -> StorageClassFactory:
        return self._registry.storage_class_factory

    @property
    def dataset_id_factory(self) -> DatasetIdFactory:
        return self._registry.dataset_id_factory

    def clear_caches(self) -> None:
        """Clear all local caches.

        This may be necessary to pick up new dataset types, collections, and
        governor dimension values added by other clients.
        """
        raise NotImplementedError()

    ###########################################################################
    #
    # Full-butler-only query methods, adapted from the current public Registry
    # interface and prototyping to take better advantage of daf_relation.
    #
    ###########################################################################

    def query(self, defer: bool = True) -> Query:
        """Power-user interface and implementation point for queries.

        Every other method in this section and a few methods in other sections
        delegate to `query` and the `Query` object it returns so they can be
        implemented purely in the concrete `Butler` class with no
        specializations for the SQL vs. http.  Instead there would be two
        implementations of various classes below `Query` (not shown in this
        prototype).

        This also replaces `Registry.queryDatasetAssociations`, as that does
        not have a direct-counterpart replacement.

        The ``defer`` argument here controls whether to immediately execute the
        query and fetch all results or defer this, allowing a query to be built
        up over the course of several chained method calls.  The default here
        is to defer, but the simpler ``query_*`` methods later default to
        running immediately.  Queries that have been executed can still be
        chained upon, but this is much less efficient if the first execution
        isn't useful in its own right.  The ``defer`` behavior is "sticky": the
        default for all methods on `Query` is to defer or not based on how that
        `Query` was created.
        """
        return self._registry.query()

    def resolve_dataset(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        expand: bool = True,
        timespan: Timespan | None,
        **kwargs: DataIdValue,
    ) -> DatasetRef:
        """The `resolve_dataset` method is the new `Registry.findDataset`, as
        well as the entry point for all of the Butler logic that interprets
        non-standard data IDs (e.g. day_obs + seq_num).

        This will ultimately call `Query`, and I hope to move as much of the
        logic as possible into `Query` itself so it can be used more generally.
        But the most important thing is that we don't execute too many queries
        from client-side logic here, as that'll lead to a lot of latency.  We
        may need to add a new Registry method with essentially this signature
        at first just so we can move all of that logic to the server ASAP.
        """
        raise NotImplementedError("Will delegate to self.query()")

    def get_dataset(self, uuid: uuid.UUID) -> DatasetRef:
        """Like the current Registry.getDataset."""
        raise NotImplementedError("Will delegate to self.query()")

    def expand_data_id(self, data_id: DataId, dimensions: Iterable[str], **kwargs: Any) -> DataCoordinate:
        """Like the current Registry.expandDataId.

        That means it's still a really bad idea to call this on lots of data
        IDs in a loop rather than using something vectorized, but I think the
        single-data ID expansion use case is important enough that we need to
        keep it."""
        raise NotImplementedError("Will delegate to self.query()")

    def query_collections(
        self,
        pattern: CollectionPattern = ...,
        *,
        types: Iterable[CollectionType] = CollectionType.all(),
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        having_datasets: Iterable[DatasetTypeName] | DatasetTypeName = (),
        exact: bool = True,
    ) -> CollectionQuery:
        """Like the current Registry.queryCollections, with a few additions:

        - The returned `CollectionQuery` object is a Sequence, but it's also
          more than that.

        - ``having_datasets`` and ``exact`` provide a way to get the
          collections that have at least one dataset from each of a list of
          dataset types (one can call query_collections multiple times to get
          "collection with datatsets of any of the given  types" behavior
          instead).
        """
        raise NotImplementedError(
            """Will delegate to registry.query_collections and filtering of
            client-side caches.  If ``having_datasets`` is not empty, will
            delegate to `query` as well for further filtering.
            """
        )

    def query_dataset_types(
        self,
        pattern: DatasetTypePattern = ...,
        *,
        # TODO: constrain on storage class or override storage class?
        dimensions: Iterable[str] = (),
        collections: CollectionPattern | None = None,
        exact: bool = True,
    ) -> DatasetTypeQuery:
        """Like the current Registry.queryDatasetTypes, with a few additition:

        - The returned `DatasetTypeQuery` object is a Mapping (keyed by name),
          but it's also more than that.

        - Let's make the ``collections`` argument actually work, and return
          only dataset types with datasets in those collections.  We should
          probably also make it default to `Butler.collections` when that is
          set (for consistency with other methods, if nothing else), though
          that might be a surprising behavior change.

        """
        raise NotImplementedError(
            """Will delegate to registry.query_dataset_types and filtering of
            client-side caches.  If ``collections`` is active, will delegate to
            `query` as well for further filtering.
            """
        )

    def query_datasets(
        self,
        dataset_type: DatasetTypePattern,
        collections: CollectionPattern = ...,
        *,
        find_first: bool = True,
        where: str = "",
        data_id: DataId | None = None,
        uuid: uuid.UUID | None = None,
        bind: Mapping[str, Any] | None = None,
        defer: bool = False,
        **kwargs: DataIdValue,
    ) -> DatasetQueryChain:
        """Like the current Registry.queryDatasets.

        Main difference is that find_first=True by default, as it it probably
        always should have been.

        In addition, this now returns results immediately by default, as
        controlled by ``defer`` (see `Butler.query`).
        """
        raise NotImplementedError("Will delegate to self.query()")

    def query_data_ids(
        self,
        dimensions: Iterable[DimensionName],
        *,
        where: str = "",
        data_id: DataId | None = None,
        bind: Mapping[str, Any] | None = None,
        defer: bool = False,
        **kwargs: DataIdValue,
    ) -> DataCoordinateQueryAdapter:
        """Like the current Registry.queryDataIds, but with the ``datasets``
        and ``collections`` arguments removed to encourage people to use
        `query_datasets` instead.

        The ``datasets`` and ``collections`` behavior can still be obtained by
        using `query` directly, which is appropriate since it's more of a
        power-user thing.

        In addition, this now returns results immediately by default, as
        controlled by ``defer`` (see `Butler.query`).
        """
        raise NotImplementedError("Will delegate to self.query()")

    def query_dimension_records(
        self,
        element: DimensionElementName,
        *,
        where: str = "",
        data_id: DataId | None = None,
        bind: Mapping[str, Any] | None = None,
        defer: bool = False,
        **kwargs: DataIdValue,
    ) -> DimensionRecordQueryAdapter:
        """Like the current Registry.queryDimensionRecords, but with the
        ``datasets`` and ``collections`` arguments removed to encourage people
        to use `query_datasets` instead.

        The ``datasets`` and ``collections`` behavior can still be obtained by
        using `query` directly, which is appropriate since it's more of a
        power-user thing.

        In addition, this now returns results immediately by default, as
        controlled by ``defer`` (see `Butler.query`).
        """
        raise NotImplementedError("Will delegate to self.query()")

    def get_collection_chain(self, chain_name: CollectionName) -> Sequence[CollectionName]:
        """Like the current Registry.getCollectionChain."""
        for _, collection_type, _, children in self.query().collections(chain_name).details():
            if children is None:
                raise CollectionTypeError(
                    f"Collection {chain_name!r} has type {collection_type}, not CHAINED."
                )
            return children
        raise MissingCollectionError(f"Collection {chain_name!r} not found.")

    def get_collection_documentation(self, name: CollectionName) -> CollectionDocumentation:
        """Like the current Registry.getCollectionDocumentation."""
        for _, _, docs, _ in self.query().collections(name).details():
            return docs
        raise MissingCollectionError(f"Collection {name!r} not found.")

    def get_dataset_type(self, name: str) -> DatasetType:
        """Like the current Registry.getDatasetType."""
        for dataset_type in self.query().dataset_types(name):
            return dataset_type
        raise MissingDatasetTypeError(f"Dataset type {name!r} does not exist.")

    ###########################################################################
    #
    # Transfers between data repositories.
    #
    ###########################################################################

    def _make_extractor(
        self,
        raw_batch: RawBatch,
        file_datasets: dict[ResourcePath, list[FileDataset]],
        on_commit: Callable[[DatastoreConfig | None], None],
        *,
        directory: ResourcePath | None,
        transfer: str | None = None,
        include_datastore_records: bool = True,
    ) -> ButlerExtractor:
        # Inherited from DatastoreButler; reimplemented just to update types.
        return ButlerExtractor(
            self,
            raw_batch,
            file_datasets,
            on_commit,
            directory=directory,
            transfer=transfer,
            include_datastore_records=include_datastore_records,
        )

    def export(
        self,
        filename: ResourcePathExpression | None = None,
        *,
        transfer: str | None = None,
        directory: ResourcePathExpression | None = None,
        include_datastore_records: bool = True,
    ) -> AbstractContextManager[ButlerExtractor]:
        # Override exists just to declare more-derived return type;
        # DatastoreButler.export does all the real work, in part by delegating
        # back to `_make_extractor`.
        return super().export(
            directory=directory,
            filename=filename,
            transfer=transfer,
            include_datastore_records=include_datastore_records,
        )

    def import_(
        self,
        filename: ResourcePathExpression | None = None,
        *,
        transfer: str | None = None,
        directory: ResourcePathExpression | None = None,
        dimension_insert_mode: SetInsertMode | None = None,
        record_validation_info: bool = True,
    ) -> None:
        """Import repository content described by an export file.

        Parameters
        ----------
        directory, optional
            Directory used as the root for all relative URIs in the export
            file.  If not provided all URIs must be absolute.
        filename, optional
            Name of the file that describes the repository context.  If
            relative, will be assumed to be relative to ``directory``.  If not
            provided, standard filenames within ``directory`` will be tried.
        transfer, optional
            Transfer mode recognized by `ResourcePath`.
        dimension_insert_mode, optional
            Enum value that controls how to resolve conflicts between dimension
            data in the export file and dimension data already in the
            repository.  Overrides the insert mode set in the file, if any.
        record_validation_info, optional
            Whether Datastore should record checksums and sizes (etc) for the
            transferred datasets.

        Notes
        -----
        The previous signature took TextIO in filename as well; I think that
        was just for execution butler creation, and going forward I think
        that's a job for `transfer_from` instead of export/import.  If we want
        take I/O objects in addition URIs maybe we could integrate that into a
        ResourcePath constructor.

        The previous signature also took a 'format' argument that had to be
        'yaml', and it also tried the filename with that appended.  Going
        forward I think we just want to infer the format from the file
        extent/header, and when no filename is given we try the default
        filenames for all supported formats in 'directory'.
        """

        if directory is not None:
            directory = ResourcePath(directory)

        # TODO: process 'filename' with directory (try abs/relative, different
        # extensions, etc) until we find something that exists.

        # Iterate over batches and tell Datastore and Registry to handle them.
        # See RawBatch.read_export_file and RawBatchExport for details.
        for datastore_config, raw_batch, file_datasets in RawBatch.read_export_file(
            filename, dimension_insert_mode
        ):
            if datastore_config is not None:
                opaque_data = (datastore_config, raw_batch.opaque_table_insertions)
            with self._datastore.receive(
                {directory: file_datasets},
                transfer=transfer,
                record_validation_info=record_validation_info,
                opaque_data=opaque_data,
            ) as opaque_table_rows:
                raw_batch.opaque_table_insertions = opaque_table_rows
                self._registry.apply_batch(raw_batch)

    @overload
    def transfer_from(
        self,
        source_butler: Butler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        record_validation_info: bool = True,
    ) -> AbstractContextManager[ButlerExtractor]:
        # Overload that takes a full Butler and returns a full ButlerExtractor.
        ...

    @overload
    def transfer_from(
        self,
        source_butler: LimitedButler,
        transfer: str = "auto",
        record_validation_info: bool = True,
    ) -> AbstractContextManager[LimitedButlerExtractor]:
        # Overload that takes a LimitedButler and returns a
        # LimitedButlerExtractor.
        ...

    @contextmanager
    def transfer_from(
        self,
        source_butler: LimitedButler,
        transfer: str = "auto",
        record_validation_info: bool = True,
    ) -> Iterator[LimitedButlerExtractor]:
        """Transfer content from one data repository to another, or from a
        staging area (e.g. QuantumGraph execution results) to the repository
        proper.

        Parameters
        ----------
        source_butler
            Butler representing the source repository.'
        transfer
            Transfer mode recognized by `ResourcePath`.
        record_validation_info
            Whether to record file sizes, checksums, etc.

        Returns
        -------
        extractor_context
            A context manager that when entered returns a helper object with
            methods for indicating what should be transferred.  When the
            context manager exits without error, the updates to the repository
            are committed.

        Notes
        -----
        For QuantumGraph execution transfer jobs (or anything else), we might
        need a way to cycle though many source QuantumBackedButlers within one
        batch, which we could do with another method on
        ``LimitedButlerExtractor`` and its subclasses.  Another option might
        be to make QuantumBackedButler capable of handling multiple quanta at
        once (extending up to the full graph, which might be more useful for
        other things).
        """
        raw_batch = RawBatch()
        file_datasets: list[FileDataset] = []

        def on_commit(datastore_config: DatastoreConfig | None) -> None:
            if datastore_config is not None:
                opaque_data = (datastore_config, raw_batch.opaque_table_insertions)
            with self._datastore.receive(
                file_datasets,
                datastore_config,
                opaque_data=opaque_data,
                transfer=transfer,
                record_validation_info=record_validation_info,
            ) as opaque_table_rows:
                raw_batch.opaque_table_insertions = opaque_table_rows
                self._registry.apply_batch(raw_batch)
            raw_batch.clear()
            file_datasets.clear()

        extractor = source_butler._make_extractor(
            raw_batch,
            file_datasets,
            on_commit,
            transfer=None,  # receiving butler will transfer.
        )
        del on_commit
        yield extractor
        extractor.commit()

    ###########################################################################
    #
    # Batch-capable mutators.
    #
    # Everything after the `batched` method here is just a convenience forward
    # to an identical method on `BatchHelper`; see those for comments and docs.
    #
    ###########################################################################

    @contextmanager
    def batched(self) -> Iterator[BatchHelper]:
        """Make multiple modifications to the data repository atomically.

        Returns
        -------
        batch_context
            A context manager that when entered returns a `BatchHelper`, which
            provides various methods that modify the data repository.  These
            methods do not act immediately and are not always applied in the
            same order they are invoked.  When the context manager exits
            without error all changes are applied.
        """
        raw_batch = RawBatch()
        with ExitStack() as exit_stack:
            yield BatchHelper(self, raw_batch, exit_stack)
            self._registry.apply_batch(raw_batch)

    def register_collection(
        self,
        name: CollectionName,
        type: CollectionType = CollectionType.RUN,
        doc: CollectionDocumentation = "",
    ) -> None:
        with self.batched() as batch:
            batch.register_collection(name, type, doc)

    def edit_collection_chain(
        self,
        chain_name: CollectionName,
        children: Sequence[CollectionName | int],
        mode: SequenceEditMode,
        *,
        flatten: bool = False,
    ) -> None:
        with self.batched() as batch:
            batch.edit_collection_chain(chain_name, children, mode, flatten=flatten)

    def set_collection_documentation(self, name: CollectionName, doc: CollectionDocumentation) -> None:
        with self.batched() as batch:
            batch.set_collection_documentation(name, doc)

    def edit_associations(
        self, collection: CollectionName, refs: Iterable[DatasetRef], mode: SetEditMode
    ) -> None:
        with self.batched() as batch:
            batch.edit_associations(collection, refs, mode)

    def certify(self, collection: CollectionName, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        with self.batched() as batch:
            batch.certify(collection, refs, timespan)

    def decertify(
        self,
        collection: CollectionName,
        dataset_type: DatasetTypeName | DatasetType,
        timespan: Timespan | None = None,
        *,
        data_ids: Iterable[DataId] | None = None,
    ) -> None:
        with self.batched() as batch:
            batch.decertify(collection, dataset_type, timespan, data_ids=data_ids)

    def merge_certifications(self, output: CollectionName, inputs: Sequence[CollectionName]) -> None:
        with self.batched() as batch:
            batch.merge_certifications(output, inputs)

    @contextmanager
    def removal(self) -> Iterator[RemovalHelper]:
        with self.batched() as batch:
            yield batch.removal()

    def register_dataset_type(
        self,
        dataset_type_or_name: DatasetTypeName | DatasetType,
        /,
        dimensions: Iterable[DimensionName] | None = None,
        storage_class: StorageClassName | StorageClass | None = None,
        is_calibration: bool | None = None,
        update: bool = False,
    ) -> None:
        with self.batched() as batch:
            batch.register_dataset_type(
                dataset_type_or_name, dimensions, storage_class, is_calibration, update=update
            )

    def remove_dataset_type(self, name: str) -> None:
        with self.batched() as batch:
            batch.register_dataset_type(name)

    def insert_dimension_data(
        self,
        element: DimensionElementName,
        data: Iterable[DimensionRecord],
        mode: SetEditMode = SetEditMode.INSERT_OR_FAIL,
    ) -> None:
        with self.batched() as batch:
            batch.insert_dimension_data(element, data, mode)

    def sync_dimension_data(self, record: DimensionRecord, update: bool = False) -> None:
        with self.batched() as batch:
            batch.sync_dimension_data(record, update)

    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        record_validation_info: bool = True,
    ) -> None:
        with self.batched() as batch:
            batch.ingest(*datasets, transfer, record_validation_info)
