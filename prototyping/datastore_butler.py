from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from lsst.daf.butler import DeferredDatasetHandle, DimensionUniverse, FileDataset
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .datastore import Datastore
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
    interface and this partial implementation.

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
        """Return an iterable of `DatasetRef` objects that have been augmented
        with datastore records as if they had been passed to `put_many`.

        This is intended to be used by QuantumGraph generation to pre-populate
        datastore records for intermediates as well as inputs.  It doesn't
        actually need to be present on QuantumBackedButler (which consumes
        these predictions rather than creating them), but I think
        DatastoreButler is the right place to put this method even though full
        Butler reimplements it (it does delegate to super).
        """
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
        directory: ResourcePath | None,
        raw_batch: RawBatch,
        file_datasets: list[FileDataset],
        *,
        transfer: str | None = None,
        include_datastore_records: bool = True,
    ) -> DatastoreButlerExtractor:
        return DatastoreButlerExtractor(
            self._datastore,
            directory=directory,
            transfer=transfer,
            raw_batch=raw_batch,
            file_datasets=file_datasets,
            include_datastore_records=include_datastore_records,
        )

    @contextmanager
    def export(
        self,
        filename: ResourcePath,
        directory: ResourcePath | None,
        *,
        transfer: str | None = None,
        include_datastore_records: bool = True,
    ) -> Iterator[DatastoreButlerExtractor]:
        raw_batch = RawBatch()
        file_datasets: list[FileDataset] = []
        yield self._make_extractor(
            directory, raw_batch, transfer=transfer, include_datastore_records=include_datastore_records
        )
        raw_batch.write_export_file(filename, self._datastore.config, file_datasets)


class DatastoreButlerExtractor(LimitedButlerExtractor):
    def __init__(
        self,
        butler: DatastoreButler,
        directory: ResourcePath | None,
        transfer: str | None,
        raw_batch: RawBatch,
        file_datasets: list[FileDataset],
        include_datastore_records: bool,
    ):
        self.butler = butler
        self._directory = directory
        self._transfer = transfer
        self._include_datastore_records = include_datastore_records
        self._file_datasets = file_datasets
        self._raw_batch = raw_batch

    def include_datasets(self, refs: Iterable[DatasetRef]) -> None:
        self._raw_batch.dataset_insertions.include(refs)
        opaque_table_insertions, file_datasets = self.butler._datastore.export(
            refs,
            mode=self._transfer,
            directory=self._directory,
            return_records=self._include_datastore_records,
        )
        if opaque_table_insertions is not None:
            self._raw_batch.opaque_table_insertions.update(opaque_table_insertions)
        self._file_datasets.extend(file_datasets)

        # TODO: export dimension records attached to ref data IDs.  But we need
        # a policy on which SetInsertMode to use for each dimension, and as
        # well as a different data structure for dimension data in RawBatch
        # that would permit deduplication.  DM-34834 is also relevant here.

        # TODO: export RUN collection registrations for all datasets.
