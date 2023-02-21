from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set, Callable
from contextlib import ExitStack, contextmanager, AbstractContextManager
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
from .datastore_butler import DatastoreButler
from .datastore import DatastoreConfig
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
from .butler_extractor import ButlerExtractor
from .removal_helper import RemovalHelper


class Butler(DatastoreButler):
    """A fully-featured concrete Butler that is backed by a (private) Registry
    and Datastore, constructable from just a repository URI.
    """

    ###########################################################################
    # Instance state
    ###########################################################################

    _registry: Registry

    ###########################################################################
    # Repository and instance creation methods.
    #
    # Open questions / notable changes:
    #
    # - Do we actually need all of the myriad arguments to make_repo?  This is
    #   a good opportunity to remove dead weight.
    #
    # - I've replaced a lot of `str` with `ResourcePathExpression` here.
    #
    ###########################################################################

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
        collections: Any = None,
        run: str = None,
        search_paths: list[str] = None,
        writeable: bool = None,
        infer_defaults: bool = True,
        **kwargs: DataIdValue,
    ):
        raise NotImplementedError("Implementation should be unchanged, or changed very little.")

    ###########################################################################
    #
    # Implementation of the LimitedButler interface with overloads for dataset
    # type + data ID arguments.
    #
    # In addition, all signatures that accept DatasetRef objects no longer
    # require them to be expanded, as full Butler can expand them as needed.
    #
    ###########################################################################

    @overload
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetRef:
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
        ...

    def put(self, obj: InMemoryDataset, *args: Any, **kwargs: Any) -> DatasetRef:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().put(obj, ref)``, which will delegate to `put_many`.
            """
        )

    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        # Unzip arguments to expand DatasetRefs and then re-zip.
        objs_by_uuid = {}
        refs = []
        for obj, ref in arg:
            objs_by_uuid[ref.uuid] = obj
            refs.append(ref)
        expanded_refs = self._expand_new_dataset_refs(refs)
        pairs = [(objs_by_uuid[ref.uuid], ref) for ref in expanded_refs]
        raw_batch = RawBatch()
        raw_batch.dataset_insertions.include(expanded_refs)
        with self._datastore.put_many_transaction(pairs) as opaque_table_rows:
            raw_batch.opaque_table_insertions.update(opaque_table_rows)
            self._registry.apply_batch(raw_batch)
        return raw_batch.opaque_table_insertions.attach_to(refs)

    def predict_put_many(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        return super().predict_put_many(self._expand_new_dataset_refs(refs))

    @overload
    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
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
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        refs = list(self._expand_existing_dataset_refs(refs))
        return super().get_many(zip(refs, parameters))

    @overload
    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
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
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        refs = list(self._expand_existing_dataset_refs(refs))
        return super().get_many_deferred(zip(refs, parameters))

    @overload
    def get_uri(self, ref: DatasetRef) -> ResourcePath:
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
        ...

    def get_uri(self, *args: Any, **kwargs: Any) -> ResourcePath:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get_uri(ref)``, which will delegate to
            `get_many_uri`.
            """
        )

    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        return super().get_many_uris(self._expand_existing_dataset_refs(refs))

    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        refs = self._expand_existing_dataset_refs(refs)
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
        self._registry.clear_caches()

    ###########################################################################
    #
    # Full-butler-only query methods, adapted from the current public Registry
    # interface and prototyping to take better advantage of daf_relation.
    #
    # Open questions / notable changes:
    #
    # - The query() method and the methods of the Query object it returns are
    #   the power-user interface; it can do everything current methods can do
    #   and more.
    #
    # - Everything else in this section (and a few methods in other sections
    #   delegate to query() and Query(), so they can be implemented purely in
    #   the concrete Butler class with no specialization for the SQL vs.  http.
    #   Instead there are two implementations of various classes below Query.
    #
    # - The `resolve_dataset` method is the new Registry.findDataset, as well
    #   as the entry point for all of the Butler logic that interprets
    #   non-standard data IDs that Butler.get_many will call.  Most of that
    #   will be delegated to Query, but some of it my need to live here in
    #   order to have access to the full context in which non-standard data ID
    #   keys should be interpreted.
    #
    # - The new `expand_datasets` method both expands DatasetRef data IDs to
    #   include dimension records and expands the DatasetRefs themselves to
    #   include all related DatastoreRecords.
    #
    # - query_data_ids and query_dimension_records no longer accept dataset and
    #   collection constraints, as these were more often misused than used
    #   correctly, and power-users like QG generation can use query().
    #
    # - queryDatasetAssociations has no direct replacement.  This was mostly
    #   used to allow CALIBRATION collection timespans to be queried, and it
    #   turns out there's really no good way to pack that functionality into
    #   queryDatasets (now query_datasets), since it would necessitate
    #   returning something other than DatasetRefs and I dislike changing
    #   return types based on arguments (and so does MyPy).
    #
    # - query_collections and query_dataset_types now have options to constrain
    #   results based on dataset type presence in collections, as has long been
    #   requested.
    #
    ###########################################################################

    def query(self) -> Query:
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
        raise NotImplementedError("Will delegate to self.query()")

    def get_dataset(self, uuid: uuid.UUID) -> DatasetRef:
        raise NotImplementedError("Will delegate to self.query()")

    def _expand_new_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Expand data IDs in datasets that are assumed not to exist in the
        Registry.

        An expanded version of every given ref must be returned.  If one or
        more dataset refs already exist in the registry, the implementation may
        fail or ignore the fact that they exist.
        """
        raise NotImplementedError("Will delegate to self.query()")

    def _expand_existing_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Expand data IDs in datasets that are assumed to exist in the
        Registry.

        Datasets that do not actually exist in the Registry need not be
        returned, but implementations should trust already-expanded content in
        the given refs to avoid unnecessary queries.
        """
        raise NotImplementedError("Will delegate to self.query()")

    def expand_data_id(self, data_id: DataId, dimensions: Iterable[str], **kwargs: Any) -> DataCoordinate:
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
        raise NotImplementedError("Will delegate to self.query()")

    def query_dataset_types(
        self,
        pattern: DatasetTypePattern = ...,
        *,
        # TODO: constrain on storage class or override storage class
        dimensions: Iterable[str] = (),
        collections: CollectionPattern = ...,
        exact: bool = True,
    ) -> DatasetTypeQuery:
        raise NotImplementedError("Will delegate to self.query()")

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
        **kwargs: DataIdValue,
    ) -> DatasetQueryChain:
        raise NotImplementedError("Will delegate to self.query()")

    def query_data_ids(
        self,
        dimensions: Iterable[DimensionName],
        *,
        where: str = "",
        data_id: DataId | None = None,
        bind: Mapping[str, Any] | None = None,
        **kwargs: DataIdValue,
    ) -> DataCoordinateQueryAdapter:
        raise NotImplementedError("Will delegate to self.query()")

    def query_dimension_records(
        self,
        element: DimensionElementName,
        *,
        where: str = "",
        data_id: DataId | None = None,
        bind: Mapping[str, Any] | None = None,
        **kwargs: DataIdValue,
    ) -> DimensionRecordQueryAdapter:
        raise NotImplementedError("Will delegate to self.query()")

    def get_collection_chain(self, chain_name: CollectionName) -> Sequence[CollectionName]:
        for _, collection_type, _, children in self.query().collections(chain_name).details():
            if children is None:
                raise CollectionTypeError(
                    f"Collection {chain_name!r} has type {collection_type}, not CHAINED."
                )
            return children
        raise MissingCollectionError(f"Collection {chain_name!r} not found.")

    def get_collection_documentation(self, name: CollectionName) -> CollectionDocumentation:
        for _, _, docs, _ in self.query().collections(name).details():
            return docs
        raise MissingCollectionError(f"Collection {name!r} not found.")

    def get_dataset_type(self, name: str) -> DatasetType:
        for dataset_type in self.query().dataset_types(name):
            return dataset_type
        raise MissingDatasetTypeError(f"Dataset type {name!r} does not exist.")

    ###########################################################################
    #
    # Bulk transfers
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
        that's a job for transfer_from instead of export/import.  If we want
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
