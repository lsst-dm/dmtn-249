from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from lsst.daf.butler import DeferredDatasetHandle, DimensionUniverse, FileDataset
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import GetParameter, InMemoryDataset
from .datastore import Datastore, DatastoreConfig
from .limited_butler import LimitedButler, LimitedButlerExtractor
from .primitives import DatasetRef
from .raw_batch import RawBatch


class DatastoreButler(LimitedButler):
    """An intermediate base class that implements much of the `LimitedButler`
    interface by delegating to `Datastore` and a few new abstract methods.

    This is intended to be the immediate base class for both the full `Butler`
    and `QuantumBackedButler`.  I don't have any other subclasses of
    `LimitedButler` itself in mind, except perhaps a fully in-memory butler
    that's intended primarily for unit testing.  But even if there aren't any I
    think it's still useful to distinguish between the `LimitedButler`
    interface and this partial implementation.  `DatastoreButler` does not
    add any new interfaces of its own - it just implements those defined by
    `LimitedButler`.

    A major difference between this and the current `QuantumBackedButler` is
    that with `DatasetRef` capable of carrying around records for `Datastore`,
    we can include those the `DatasetRef` objects in the quanta for all inputs
    (including intermediates) when generate the graph, and thus do the
    "prediction" at that stage (this is not a practical advantage, but I think
    it keeps the division of responsibilities cleaner).
    """

    def __init__(self, datastore: Datastore):
        self._datastore = datastore

    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        # This needs to be reimplemented by full Butler so it can call
        # put_many_transaction to guarantee consistency with Registry.
        pairs = list(arg)
        opaque_table_data = self._datastore.put_many(pairs)
        return opaque_table_data.attach_to([ref for _, ref in pairs])

    def predict_put_many(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        refs = list(refs)
        opaque_table_data = self._datastore.predict_put_many(refs)
        return opaque_table_data.attach_to(refs)

    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        return self._datastore.get_many(arg)

    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        return [
            # It's not shown in the prototyping here, but since these are
            # expanded DatasetRefs that hold all datastore records,
            # DeferredDatasetHandle only needs to hold a Datastore instead of a
            # Butler.
            (ref, parameters_for_ref, DeferredDatasetHandle(self._datastore, ref, parameters_for_ref))
            for ref, parameters_for_ref in arg
        ]

    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        return self._datastore.get_many_uris(refs)

    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        # This needs to be reimplemented by full Butler so it can call
        # put_many_transaction to guarantee consistency with Registry.
        self._datastore.unstore(refs)

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError()

    def _make_extractor(
        self,
        raw_batch: RawBatch,
        file_datasets: dict[ResourcePath, list[FileDataset]],
        on_commit: Callable[[DatastoreConfig | None], None],
        *,
        directory: ResourcePath | None = None,
        transfer: str | None = None,
        include_datastore_records: bool = True,
    ) -> DatastoreButlerExtractor:
        return DatastoreButlerExtractor(
            self,
            raw_batch,
            file_datasets,
            on_commit,
            directory=directory,
            transfer=transfer,
            include_datastore_records=include_datastore_records,
        )

    @contextmanager
    def export(
        self,
        directory: ResourcePathExpression | None = None,
        filename: ResourcePathExpression | None = None,
        *,
        transfer: str | None = None,
        include_datastore_records: bool = True,
    ) -> Iterator[DatastoreButlerExtractor]:
        if directory is not None:
            directory = ResourcePath(directory)

        # TODO: process 'filename' with directory to handle relative paths,
        # extensions.  Reject .yaml in favor of whatever our new extension is;
        # if necessary we could add a ``legacy: bool = False`` kwarg to keep
        # support writing YAML for a bit, but I'd like to drop writing (but
        # keep reading) as soon as an alternative exists.

        raw_batch = RawBatch()
        file_datasets: dict[ResourcePath, FileDataset] = {}

        def on_commit(datastore_config: DatastoreConfig | None) -> None:
            raw_batch.write_export_file(filename, datastore_config, file_datasets)
            raw_batch.clear()
            file_datasets.clear()

        extractor = self._make_extractor(
            raw_batch,
            file_datasets,
            on_commit,
            directory=directory,
            transfer=transfer,
            include_datastore_records=include_datastore_records,
        )
        yield extractor
        del on_commit
        extractor.commit()


class DatastoreButlerExtractor(LimitedButlerExtractor):
    """Implementation of `LimitedButlerExtractor` that holds
    a `DatastoreButler`.
    """

    def __init__(
        self,
        butler: DatastoreButler,
        raw_batch: RawBatch,
        file_datasets: dict[ResourcePath, list[FileDataset]],
        on_commit: Callable[[DatastoreConfig | None], None],
        directory: ResourcePath | None,
        transfer: str | None,
        include_datastore_records: bool,
    ):
        self.butler = butler
        self._directory = directory
        self._include_datastore_records = include_datastore_records
        self._file_datasets = file_datasets
        self._on_commit = on_commit
        self._transfer = transfer
        self._raw_batch = raw_batch

    butler: DatastoreButler

    def include_datasets(
        self,
        refs: Iterable[DatasetRef],
        include_types: bool = True,
        include_dimensions: bool = True,
        include_run_collections: bool = True,
    ) -> None:
        self._raw_batch.dataset_insertions.include(refs)
        opaque_table_insertions, file_datasets = self.butler._datastore.export(
            refs,
            transfer=self._transfer,
            directory=self._directory,
            return_records=self._include_datastore_records,
        )
        if opaque_table_insertions is not None:
            self._raw_batch.opaque_table_insertions.update(opaque_table_insertions)

        for root_uri, files_in_root in file_datasets.items():
            self._file_datasets.setdefault(root_uri, []).extend(files_in_root)

        # TODO: handle optional associated inclusions, all via updates to
        # self._raw_batch.

    def commit(self) -> None:
        self._on_commit(self.butler._datastore.config)
