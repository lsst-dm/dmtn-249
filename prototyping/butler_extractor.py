from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from lsst.daf.butler import DatasetRef, FileDataset, DimensionRecord, DataCoordinate
from lsst.resources import ResourcePath

from .datastore_butler import DatastoreButlerExtractor
from .raw_batch import RawBatch
from .aliases import CollectionName, DimensionElementName
from .primitives import SetInsertMode

if TYPE_CHECKING:
    from .butler import Butler


class ButlerExtractor(DatastoreButlerExtractor):
    def __init__(
        self,
        butler: Butler,
        directory: ResourcePath | None,
        transfer: str | None,
        raw_batch: RawBatch,
        file_datasets: list[FileDataset],
        include_datastore_records: bool,
    ):
        self.butler: Butler
        super().__init__(
            self, butler, directory, transfer, raw_batch, file_datasets, include_datastore_records
        )

    def include_datasets(
        self,
        refs: Iterable[DatasetRef],
        include_types: bool = True,
        include_dimensions: bool = True,
        include_run_collections: bool = True,
    ) -> None:
        super().include_datasets(
            self.butler._expand_existing_dataset_refs(refs),
            include_types=include_types,
            include_dimensions=include_dimensions,
            include_run_collections=include_run_collections,
        )

    def include_collections(self, names: Iterable[CollectionName]) -> None:
        raise NotImplementedError(
            """Use butler.query() to get types and docs, then update
            self._raw_batch.
            """
        )

    def include_dimension_records(
        self,
        element: DimensionElementName,
        records: Iterable[DimensionRecord],
        mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP,
    ) -> None:
        raise NotImplementedError(
            """Extract dimension records and update self._raw_batch, while
            dealing with mode conflicts.
            """
        )

    def include_data_ids(
        self, data_ids: Iterable[DataCoordinate], mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP
    ) -> None:
        raise NotImplementedError(
            """Extract dimension records and update self._raw_batch, while
            dealing with mode conflicts.
            """
        )
