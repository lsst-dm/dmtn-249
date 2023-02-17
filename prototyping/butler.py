from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from contextlib import contextmanager
from typing import Any, TextIO, overload

from lsst.daf.butler import (
    CollectionType,
    Config,
    DataCoordinate,
    DataId,
    DataIdValue,
    DatasetIdFactory,
    DatasetIdGenEnum,
    DatasetType,
    DeferredDatasetHandle,
    DimensionUniverse,
    FileDataset,
    StorageClass,
    StorageClassFactory,
    Timespan,
)
from lsst.daf.butler.registry import CollectionTypeError, MissingCollectionError, MissingDatasetTypeError
from lsst.daf.butler.transfers import RepoExportContext
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
from .raw_batch import (
    ChainedCollectionEdit,
    DimensionDataInsertion,
    DimensionDataSync,
    RawBatch,
    SequenceEditMode,
)
from .datastore import Datastore
from .limited_butler import LimitedButler
from .primitives import DatasetRef
from .queries import (
    CollectionQuery,
    DataCoordinateQueryAdapter,
    DatasetQueryChain,
    DatasetTypeQuery,
    DimensionRecordQueryAdapter,
    Query,
)
from .registry import Registry
from .removal_helper import RemovalHelper


class Butler(LimitedButler):
    """A fully-featured concrete Butler that is backed by a (private) Registry
    and Datastore, constructable from just a repository URI.
    """

    ###########################################################################
    # Instance state
    ###########################################################################

    _registry: Registry
    _datastore: Datastore

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
    # Implementation of the LimitedButler interface with overloads for dataset
    # type + data ID arguments.
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
        expanded_refs = self.expand_new_dataset_refs(refs)
        pairs = [(objs_by_uuid[ref.uuid], ref) for ref in expanded_refs]
        raw_batch = RawBatch()
        raw_batch.dataset_insertions.include_refs(expanded_refs)
        with self._datastore.put_many_transaction(pairs) as opaque_table_rows:
            for table_name, rows_for_table in opaque_table_rows:
                raw_batch.opaque_table_insertions.include(table_name, rows_for_table)
            self._registry.apply_batch(raw_batch)
        return raw_batch.opaque_table_insertions.attach_to(refs)

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
        refs = list(self.expand_existing_dataset_refs(refs))
        return self._datastore.get_many(zip(refs, parameters))

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
        refs = list(self.expand_existing_dataset_refs(refs))
        return [
            # It's not shown in the prototyping here, but since these are
            # expanded DatasetRefs that hold all datastore records,
            # DeferredDatasetHandle only needs to hold a Datastore instead of a
            # Butler.
            (ref, parameters_for_ref, DeferredDatasetHandle(self._datastore, ref, parameters))
            for ref, parameters_for_ref in zip(refs, parameters)
        ]

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
        return self._datastore.get_many_uri(self.expand_existing_dataset_refs(refs))

    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        refs = self.expand_existing_dataset_refs(refs)
        raw_batch = RawBatch()
        with self._datastore.unstore_transaction(refs) as opaque_table_uuids:
            for table_name, uuids in opaque_table_uuids:
                raw_batch.opaque_table_removals.include(table_name, uuids)
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

    def expand_new_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Expand data IDs in datasets that are assumed not to exist in the
        Registry.

        An expanded version of every given ref must be returned.  If one or
        more dataset refs already exist in the registry, the implementation may
        fail or ignore the fact that they exist.
        """
        raise NotImplementedError("Will delegate to self.query()")

    def expand_existing_dataset_refs(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
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

    ###########################################################################
    #
    # Collection manipulation, adapted from the current Registry public
    # interface.
    #
    # Open questions / notable changes:
    #
    # - registerRun is gone, but RUN is now the default type for
    #   register_collections.
    #
    # - merge_calibrations comes from DM-36590, which appears to be what the
    #   CPP team really want instead of a decertify CLI.  I still think
    #   decertify is worth keeping in its current form, but with
    #   merge_collections it becomes more of a rare-use admin tool instead of
    #   ever becoming part of the normal calibration management workflow.
    #
    # - removal and the RemovalHelper class are an attempt to provide both
    #   atomic collection deletion and some of the conveniences of the
    #   remove-collections CLI in Python.
    #
    ###########################################################################

    def register_collection(
        self,
        name: CollectionName,
        type: CollectionType = CollectionType.RUN,
        doc: CollectionDocumentation = "",
    ) -> None:
        raw_batch = RawBatch()
        raw_batch.collection_registrations.append((name, type, doc))
        self._registry.apply_batch(raw_batch)

    def get_collection_chain(self, chain_name: CollectionName) -> Sequence[CollectionName]:
        for _, collection_type, _, children in self.query().collections(chain_name).details():
            if children is None:
                raise CollectionTypeError(
                    f"Collection {chain_name!r} has type {collection_type}, not CHAINED."
                )
            return children
        raise MissingCollectionError(f"Collection {chain_name!r} not found.")

    def edit_collection_chain(
        self,
        chain_name: CollectionName,
        children: Sequence[CollectionName | int],
        mode: SequenceEditMode,
        *,
        flatten: bool = False,
    ) -> None:
        raw_batch = RawBatch()
        raw_batch.collection_edits.append(ChainedCollectionEdit(chain_name, list(children), mode, flatten))
        self._registry.apply_batch(raw_batch)

    def get_collection_documentation(self, name: CollectionName) -> CollectionDocumentation:
        for _, _, docs, _ in self.query().collections(name).details():
            return docs
        raise MissingCollectionError(f"Collection {name!r} not found.")

    def set_collection_documentation(self, name: CollectionName, doc: CollectionDocumentation) -> None:
        raise NotImplementedError("Make BatchedEdit and apply it.")

    def associate(self, collection: CollectionName, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError("Make BatchedEdit and apply it.")

    def disassociate(self, collection: CollectionName, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError("Make BatchedEdit and apply it.")

    def certify(self, collection: CollectionName, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        raise NotImplementedError("Make BatchedEdit and apply it.")

    def decertify(
        self,
        collection: CollectionName,
        dataset_type: DatasetTypeName | DatasetType,
        timespan: Timespan | None = None,
        *,
        data_ids: Iterable[DataId] | None = None,
    ) -> None:
        raise NotImplementedError("Will delegate to Registry.")

    def merge_calibrations(
        self, new_collection: CollectionName, existing_collections: Sequence[CollectionName]
    ) -> None:
        raise NotImplementedError("will delegate to Registry.")

    @contextmanager
    def removal(self) -> Iterator[RemovalHelper]:
        helper = RemovalHelper(self)
        yield helper
        if helper:
            raw_batch = helper._into_raw_batch()
            with self._datastore.unstore_transaction(
                raw_batch.dataset_removals.refs_to_unstore
            ) as opaque_rows:
                for table_name, rows in opaque_rows.items():
                    raw_batch.opaque_table_deletes[table_name].extend(rows)
                self._registry.apply_batch(raw_batch)

    ###########################################################################
    #
    # Dataset type manipulation, adapted from the current Registry public
    # interface.
    #
    # Open questions / notable changes:
    #
    # - register_dataset_type doesn't need to be passed a full DatasetType
    #   instance anymore, so it's less verbose to call.
    #
    # - register_dataset_type now has an `update` option, which could be used
    #   to change the storage class.  If there are no datasets that need to be
    #   updated it could also modify is_calibration and the dimensions.  If
    #   we retire NameKeyCollectionManager we could also pretty easily add a
    #   ``rename: str | None = None`` that could do renames when
    #   ``update=True``.
    #
    ###########################################################################

    def register_dataset_type(
        self,
        dataset_type_or_name: str | DatasetType,
        /,
        dimensions: Iterable[str],
        storage_class: str | StorageClass,
        is_calibration: bool = False,
        update: bool = False,
    ) -> DatasetType:
        raise NotImplementedError("Will delegate to Registry.")

    def get_dataset_type(self, name: str) -> DatasetType:
        for dataset_type in self.query().dataset_types(name):
            return dataset_type
        raise MissingDatasetTypeError(f"Dataset type {name!r} does not exist.")

    def remove_dataset_type(self, name: str) -> None:
        raise NotImplementedError("Will delegate to Registry.")

    ###########################################################################
    #
    # Dimension data manipulation.
    #
    # This wraps the Registry insertDimensionData and syncDimensionData methods
    # into a pair of helper classes that are passed as an iterable, allowing
    # heterogeneous insertions and even a bit of conditional logic (see
    # DimensionDataSync.on_insert and on_update) to be performed atomically
    # even when that has to happen on a butler server.  I've inspected
    # RawIngestTask, DefineVisitsTask, BaseSkyMap.register, and
    # Instrument.register to verify that we could reimplement all of those with
    # this interface without changing their behavior.
    #
    ###########################################################################

    def insert_dimension_data(self, data: Iterable[DimensionDataInsertion | DimensionDataSync]) -> None:
        raw_batch = RawBatch()
        raw_batch.dimension_data.extend(data)
        self._registry.apply_batch(raw_batch)

    ###########################################################################
    #
    # Bulk transfers
    #
    ###########################################################################

    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        run: str | None = None,
        id_generation_mode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        record_validation_info: bool = True,
    ) -> None:
        raise NotImplementedError("TODO")

    @contextmanager
    def export(
        self,
        *,
        directory: str | None = None,
        filename: str | None = None,
        format: str | None = None,
        transfer: str | None = None,
    ) -> Iterator[RepoExportContext]:
        raise NotImplementedError("TODO")

    def import_(
        self,
        *,
        directory: str | None = None,
        filename: str | TextIO | None = None,
        format: str | None = None,
        transfer: str | None = None,
        skip_dimensions: Set | None = None,
        id_generation_mode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> None:
        raise NotImplementedError("TODO")

    def transfer_from(
        self,
        source_butler: LimitedButler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        id_gen_map: Mapping[str, DatasetIdGenEnum] | None = None,
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
    ) -> list[DatasetRef]:
        raise NotImplementedError("TODO")
